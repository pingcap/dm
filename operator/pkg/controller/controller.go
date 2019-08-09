/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	pv1alpha1 "github.com/pingcap/dm/operator/pkg/apis/pingcap.com/v1alpha1"
	clientset "github.com/pingcap/dm/operator/pkg/client/clientset/versioned"
	dscheme "github.com/pingcap/dm/operator/pkg/client/clientset/versioned/scheme"
	informers "github.com/pingcap/dm/operator/pkg/client/informers/externalversions"
	listers "github.com/pingcap/dm/operator/pkg/client/listers/pingcap.com/v1alpha1"
	"golang.org/x/time/rate"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"
)

var (
	metaDB        pv1alpha1.DMDB
	deployLock    sync.RWMutex
	masterDeploy  = make(map[string]string)
	masterSvcLock sync.Mutex
	masterSvc     = make(map[string]string)
)

const (
	portName         = "service"
	dataDir          = "/data"
	configDir        = "/etc/dm"
	masterConfigFile = "master.toml"
	workerConfigFile = "worker.toml"

	masterPort                  = 8261
	workerPort                  = 8262
	lastAppliedConfigAnnotation = "pingcap.com/last-applied-configuration"
	componentMaster             = "master"
	componentWorker             = "worker"
	componentTask               = "task"
	componentMeta               = "meta"
	componentKey                = "app.kubernetes.io/component"
	instanceKey                 = "app.kubernetes.io/instance"
	sourceKey                   = "app.kubernetes.io/source"
	managedByKey                = "app.kubernetes.io/managed-by"
	controllerAgentName         = "dm-operator"
	// SuccessSynced is used as part of the Event 'reason' when a resource is synced
	SuccessSynced = "Synced"
	// ErrResourceExists is used as part of the Event 'reason' when a resource fails
	// to sync due to a resource of the same name already existing.
	ErrResourceExists = "ErrResourceExists"
	// MessageResourceExists is the message used for Events when a resource
	// fails to sync due to a resource already existing
	MessageResourceExists = "Resource %q already exists and is not managed by dm-operator"
	// MessageResourceSynced is the message used for an Event fired when a resource
	// is synced successfully
	MessageResourceSynced = "Resource synced successfully"
)

// Controller is the controller implementation for resources
type Controller struct {
	httpClient *http.Client
	// kubeclientset is a standard kubernetes clientset
	kubeclientset kubernetes.Interface
	// dmClient is a clientset for our own API group
	dmClient     clientset.Interface
	masterPVC    bool
	storageClass string

	statefulsetsLister appslisters.StatefulSetLister
	statefulsetsSynced cache.InformerSynced
	configmapsLister   corelisters.ConfigMapLister
	configmapsSynced   cache.InformerSynced
	servicesLister     corelisters.ServiceLister
	servicesSynced     cache.InformerSynced
	pvcLister          corelisters.PersistentVolumeClaimLister
	pvcSynced          cache.InformerSynced
	pvLister           corelisters.PersistentVolumeLister
	pvSynced           cache.InformerSynced
	podLister          corelisters.PodLister
	podSynced          cache.InformerSynced

	nodesLister listers.DMNodeLister
	nodesSynced cache.InformerSynced
	tasksLister listers.DMTaskLister
	tasksSynced cache.InformerSynced
	metasLister listers.DMMetaLister
	metasSynced cache.InformerSynced

	// workqueue is a rate limited work queue. This is used to queue work to be
	// processed instead of performing it as soon as a change happens. This
	// means we can ensure we only process a fixed amount of resources at a
	// time, and makes it easy to ensure we are never processing the same item
	// simultaneously in two different workers.
	nodequeue workqueue.RateLimitingInterface
	taskqueue workqueue.RateLimitingInterface
	// recorder is an event recorder for recording Event resources to the
	// Kubernetes API.
	recorder record.EventRecorder
}

// NewController returns a new sample controller
func NewController(
	kubeclientset kubernetes.Interface,
	dmClient clientset.Interface,
	kubeInformerFactory kubeinformers.SharedInformerFactory,
	dmInformerFactory informers.SharedInformerFactory,
	timeout int, pvc bool, sc string) *Controller {

	// Create event broadcaster
	// Add pingcap types to the default Kubernetes Scheme so Events can be
	// logged for pingcap types.
	utilruntime.Must(dscheme.AddToScheme(scheme.Scheme))
	klog.V(4).Info("Creating event broadcaster")
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeclientset.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerAgentName})

	stsInformer := kubeInformerFactory.Apps().V1().StatefulSets()
	cmInformer := kubeInformerFactory.Core().V1().ConfigMaps()
	svcInformer := kubeInformerFactory.Core().V1().Services()
	pvcInformer := kubeInformerFactory.Core().V1().PersistentVolumeClaims()
	pvInformer := kubeInformerFactory.Core().V1().PersistentVolumes()
	podInformer := kubeInformerFactory.Core().V1().Pods()
	nodeInformer := dmInformerFactory.Pingcap().V1alpha1().DMNodes()
	taskInformer := dmInformerFactory.Pingcap().V1alpha1().DMTasks()
	metaInformer := dmInformerFactory.Pingcap().V1alpha1().DMMetas()

	controller := &Controller{
		httpClient:    &http.Client{Timeout: time.Duration(timeout) * time.Second},
		kubeclientset: kubeclientset,
		dmClient:      dmClient,
		masterPVC:     pvc,
		storageClass:  sc,

		statefulsetsLister: stsInformer.Lister(),
		statefulsetsSynced: stsInformer.Informer().HasSynced,
		configmapsLister:   cmInformer.Lister(),
		configmapsSynced:   cmInformer.Informer().HasSynced,
		servicesLister:     svcInformer.Lister(),
		servicesSynced:     svcInformer.Informer().HasSynced,
		pvLister:           pvInformer.Lister(),
		pvSynced:           pvInformer.Informer().HasSynced,
		pvcLister:          pvcInformer.Lister(),
		pvcSynced:          pvcInformer.Informer().HasSynced,
		podLister:          podInformer.Lister(),
		podSynced:          podInformer.Informer().HasSynced,

		nodesLister: nodeInformer.Lister(),
		nodesSynced: nodeInformer.Informer().HasSynced,
		tasksLister: taskInformer.Lister(),
		tasksSynced: taskInformer.Informer().HasSynced,
		metasLister: metaInformer.Lister(),
		metasSynced: metaInformer.Informer().HasSynced,

		nodequeue: workqueue.NewNamedRateLimitingQueue(workqueue.NewMaxOfRateLimiter(
			workqueue.NewItemExponentialFailureRateLimiter(500*time.Millisecond, 10*time.Second),
			// 10 qps, 100 bucket size.  This is only for retry speed and its only the overall factor (not per item)
			&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
		), "DMNodes"),
		taskqueue: workqueue.NewNamedRateLimitingQueue(workqueue.NewMaxOfRateLimiter(
			workqueue.NewItemExponentialFailureRateLimiter(500*time.Millisecond, 10*time.Second),
			// 10 qps, 100 bucket size.  This is only for retry speed and its only the overall factor (not per item)
			&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
		), "DMTasks"),
		recorder: recorder,
	}

	klog.Info("Setting up event handlers")
	// Set up an event handler for when DMNode resources change
	nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueueNode,
		UpdateFunc: func(old, new interface{}) {
			controller.enqueueNode(new)
		},
		DeleteFunc: controller.enqueueNode,
	})
	// Set up an event handler for when DMTask resources change
	taskInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueueTask,
		UpdateFunc: func(old, new interface{}) {
			controller.enqueueTask(new)
		},
		// DeleteFunc: controller.enqueueTask,
	})
	// Set up an event handler for when DMMeta resources change
	metaInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.handleMeta,
		UpdateFunc: func(old, new interface{}) {
			newMeta := new.(*pv1alpha1.DMMeta)
			oldMeta := old.(*pv1alpha1.DMMeta)
			if newMeta.ResourceVersion == oldMeta.ResourceVersion {
				// Periodic resync will send update events for all known Deployments.
				// Two different versions of the same Deployment will always have different RVs.
				return
			}
			controller.handleMeta(new)
		},
		// DeleteFunc: controller.enqueueTask,
	})
	// Set up an event handler for when dm/operator managed resources change. This
	// handler will lookup the owner of the given resource, and if it is
	// owned by dm/operator, will enqueue that resource for
	// processing. This way, we don't need to implement custom logic for
	// handling different resources. More info on this pattern:
	// https://github.com/kubernetes/community/blob/8cafef897a22026d42f5e5bb3f104febe7e29830/contributors/devel/controllers.md
	stsInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.addStatefulset,
		UpdateFunc: func(old, new interface{}) {
			newSts := new.(*appsv1.StatefulSet)
			oldSts := old.(*appsv1.StatefulSet)
			if newSts.ResourceVersion == oldSts.ResourceVersion {
				// Periodic resync will send update events for all known Deployments.
				// Two different versions of the same Deployment will always have different RVs.
				return
			}
			controller.updateStatefulset(new)
		},
		DeleteFunc: controller.deleteStatefulset,
	})
	cmInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.addConfigmap,
		UpdateFunc: func(old, new interface{}) {
			newCM := new.(*corev1.ConfigMap)
			oldCM := old.(*corev1.ConfigMap)
			if newCM.ResourceVersion == oldCM.ResourceVersion {
				// Periodic resync will send update events for all known Deployments.
				// Two different versions of the same Deployment will always have different RVs.
				return
			}

			controller.updateConfigmap(new)
		},
		DeleteFunc: controller.deleteConfigmap,
	})
	svcInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.addService,
		UpdateFunc: func(old, new interface{}) {
			newSvc := new.(*corev1.Service)
			oldSvc := old.(*corev1.Service)
			if newSvc.ResourceVersion == oldSvc.ResourceVersion {
				// Periodic resync will send update events for all known Deployments.
				// Two different versions of the same Deployment will always have different RVs.
				return
			}
			controller.updateService(new)
		},
		DeleteFunc: controller.deleteService,
	})

	return controller
}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shutdown the workqueue and wait for
// workers to finish processing their current work items.
func (c *Controller) Run(threadiness int, stopCh <-chan struct{}) error {
	defer utilruntime.HandleCrash()
	defer c.nodequeue.ShutDown()
	defer c.taskqueue.ShutDown()

	// Start the informer factories to begin populating the informer caches
	klog.Info("Starting DM controller")

	// Wait for the caches to be synced before starting workers
	klog.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh, c.statefulsetsSynced, c.configmapsSynced, c.servicesSynced,
		c.nodesSynced, c.tasksSynced, c.metasSynced, c.pvSynced, c.pvcSynced, c.podSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	klog.Info("Starting workers")
	// Launch two workers to process DMNode resources
	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runNodeWorker, time.Second, stopCh)
	}
	// Launch two workers to process DMTask resources
	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runTaskWorker, time.Second, stopCh)
	}

	klog.Info("Started workers")
	<-stopCh
	klog.Info("Shutting down workers")

	return nil
}

func (c *Controller) handleMeta(obj interface{}) {

	tmeta := obj.(*pv1alpha1.DMMeta)
	meta := tmeta.DeepCopy()
	db := meta.Spec.Mysql
	klog.Infof("Add meta %#v", db)
	if !validDBConfig(db) {
		klog.Errorf("Invalid DB config [%#v] in DMMeta %s/%s", db, meta.Namespace, meta.Name)
		return
	}
	if metaDB.Host != "" && metaDB != db {
		klog.Infof("Ignore DB config change, previous: [%#v], current: [%#v] in DMMeta %s/%s", metaDB, db, meta.Namespace, meta.Name)
		return
	}
	metaDB = db
	klog.Infof("Get DB config [%#v] from DMMeta %s/%s", metaDB, meta.Namespace, meta.Name)
	return
}

func (c *Controller) addStatefulset(obj interface{}) {
	klog.V(4).Infof("Handle statefulset add")
	c.handleObject(obj)
}
func (c *Controller) updateStatefulset(obj interface{}) {
	klog.V(4).Infof("Handle statefulset update")
	c.handleObject(obj)
}
func (c *Controller) deleteStatefulset(obj interface{}) {
	klog.V(4).Infof("Handle statefulset delete")
	c.handleObject(obj)
}
func (c *Controller) addConfigmap(obj interface{}) {
	klog.V(4).Infof("Handle configmap add")
	c.handleObject(obj)
}
func (c *Controller) updateConfigmap(obj interface{}) {
	klog.V(4).Infof("Handle configmap update")
	c.handleObject(obj)
}
func (c *Controller) deleteConfigmap(obj interface{}) {
	klog.V(4).Infof("Handle configmap delete")
	c.handleObject(obj)
}
func (c *Controller) addService(obj interface{}) {
	klog.V(4).Infof("Handle service add")
	c.handleObject(obj)
}
func (c *Controller) updateService(obj interface{}) {
	klog.V(4).Infof("Handle service update")
	c.handleObject(obj)
}
func (c *Controller) deleteService(obj interface{}) {
	klog.V(4).Infof("Handle service delete")
	c.handleObject(obj)
}

// handleObject will take any resource implementing metav1.Object and attempt
// to find the DMNode resource that 'owns' it. It does this by looking at the
// objects metadata.ownerReferences field for an appropriate OwnerReference.
// It then enqueues that DMNode resource to be processed. If the object does not
// have an appropriate OwnerReference, it will simply be skipped.
func (c *Controller) handleObject(obj interface{}) {
	var object metav1.Object
	var ok bool
	if object, ok = obj.(metav1.Object); !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("error decoding object, invalid type"))
			return
		}
		object, ok = tombstone.Obj.(metav1.Object)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("error decoding object tombstone, invalid type"))
			return
		}
		klog.V(4).Infof("Recovered deleted object '%s/%s' from tombstone", object.GetNamespace(), object.GetName())
	}
	klog.V(4).Infof("Processing object: %s/%s", object.GetNamespace(), object.GetName())
	if ownerRef := metav1.GetControllerOf(object); ownerRef != nil {
		// If this object is not owned by a DMNode, we should not do anything more
		// with it.
		if ownerRef.Kind != dmNodeKind.Kind {
			return
		}

		node, err := c.nodesLister.DMNodes(object.GetNamespace()).Get(ownerRef.Name)
		if err != nil {
			klog.V(4).Infof("ignoring orphaned object '%s' of '%s'", object.GetSelfLink(), ownerRef.Name)
			return
		}

		c.enqueueNode(node)
		return
	}
}
