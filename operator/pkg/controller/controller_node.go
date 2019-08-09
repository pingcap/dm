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

	pv1alpha1 "github.com/pingcap/dm/operator/pkg/apis/pingcap.com/v1alpha1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
)

var (
	// dmNodeKind contains the schema.GroupVersionKind for this controller type.
	dmNodeKind = pv1alpha1.SchemeGroupVersion.WithKind("DMNode")
)

// runNodeWorker is a long-running function that will continually call the
// processNextNodeWorkItem function in order to read and process a message on the
// nodequeue.
func (c *Controller) runNodeWorker() {
	for c.processNextNodeWorkItem() {
	}
}

// processNextNodeWorkItem will read a single work item off the nodequeue and
// attempt to process it, by calling the syncNodeHandler.
func (c *Controller) processNextNodeWorkItem() bool {
	obj, shutdown := c.nodequeue.Get()

	if shutdown {
		return false
	}

	// We wrap this block in a func so we can defer c.nodequeue.Done.
	err := func(obj interface{}) error {
		// We call Done here so the nodequeue knows we have finished
		// processing this item. We also must remember to call Forget if we
		// do not want this work item being re-queued. For example, we do
		// not call Forget if a transient error occurs, instead the item is
		// put back on the nodequeue and attempted again after a back-off
		// period.
		defer c.nodequeue.Done(obj)
		var key string
		var ok bool
		// We expect strings to come off the nodequeue. These are of the
		// form namespace/name. We do this as the delayed nature of the
		// nodequeue means the items in the informer cache may actually be
		// more up to date that when the item was initially put onto the
		// nodequeue.
		if key, ok = obj.(string); !ok {
			// As the item in the nodequeue is actually invalid, we call
			// Forget here else we'd go into a loop of attempting to
			// process a work item that is invalid.
			c.nodequeue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected string in nodequeue but got %#v", obj))
			return nil
		}
		// Run the syncNodeHandler, passing it the namespace/name string of the
		// DMNode resource to be synced.
		if err := c.syncNodeHandler(key); err != nil {
			// Put the item back on the nodequeue to handle any transient errors.
			c.nodequeue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}
		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		c.nodequeue.Forget(obj)
		klog.Infof("Successfully synced '%s'", key)
		return nil
	}(obj)

	if err != nil {
		utilruntime.HandleError(err)
		return true
	}

	return true
}

// syncNodeHandler compares the actual state with the desired, and attempts to
// converge the two. It then updates the Status block of the DMNode resource
// with the current status of the resource.
func (c *Controller) syncNodeHandler(key string) error {
	// Convert the namespace/name string into a distinct namespace and name
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	// Get the DMNode resource with this namespace/name
	node, err := c.nodesLister.DMNodes(namespace).Get(name)
	if err != nil {
		// The DMNode resource may no longer exist, in which case we stop
		// processing.
		if errors.IsNotFound(err) {
			utilruntime.HandleError(fmt.Errorf("DMNode '%s' in work queue no longer exists", key))
			return nil
		}
		return err
	}

	labels := node.Labels
	err = validNodeLabels(labels)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("%s: invalid labels %#v: %v", key, labels, err))
		return nil
	}
	err = validDMNode(node)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("%s: invalid CRD %s", key, err))
		return nil
	}
	tmpNode := node.DeepCopy()
	adjustNodeConfig(tmpNode, c.storageClass)

	// syncing all PVs managed by operator's reclaim policy to Retain
	if err = c.syncReclaimPolicy(tmpNode); err != nil {
		return err
	}

	// // cleaning all orphan pods(pd or tikv which don't have a related PVC) managed by operator
	if _, err := c.cleanOrphanPods(tmpNode); err != nil {
		return err
	}

	if err = c.syncNodeService(tmpNode); err != nil {
		klog.Errorf("syncNodeService error for %s: %v", key, err)
		return err
	}
	if err = c.syncNodeHeadlessService(tmpNode); err != nil {
		klog.Errorf("syncNodeHeadlessService error for %s: %v", key, err)
		return err
	}
	if err = c.syncNodeConfigMap(tmpNode); err != nil {
		klog.Errorf("syncNodeConfigMap error for %s: %v", key, err)
		return err
	}
	if err = c.syncNodeStatefulSet(tmpNode); err != nil {
		klog.Errorf("syncNodeStatefulSet error for %s: %v", key, err)
		return err
	}
	// if err = c.syncNodeMeta(tmpNode); err != nil {
	// 	klog.Errorf("syncNodePVPVC error for %s: %v", key, err)
	// 	return err
	// }

	// Finally, we update the status block of the DMNode resource to reflect the
	// current state of the world
	err = c.updateDMNodeStatus(node, tmpNode)
	if err != nil {
		return err
	}

	c.recorder.Event(tmpNode, corev1.EventTypeNormal, SuccessSynced, MessageResourceSynced)
	return nil
}

func (c *Controller) updateDMNodeStatus(node, tmpNode *pv1alpha1.DMNode) error {
	// NEVER modify objects from the store. It's a read-only, local cache.
	// You can use DeepCopy() to make a deep copy of original object and modify this copy
	// Or create a copy manually for better performance
	nodeCopy := node.DeepCopy()
	nodeCopy.Status.StatefulSet = *tmpNode.Status.StatefulSet.DeepCopy()
	// If the CustomResourceSubresources feature gate is not enabled,
	// we must use Update instead of UpdateStatus to update the Status block of the DMNode resource.
	// UpdateStatus will not allow changes to the Spec of the resource,
	// which is ideal for ensuring nothing other than resource status has been updated.
	_, err := c.dmClient.PingcapV1alpha1().DMNodes(node.Namespace).Update(nodeCopy)
	return err
}

// enqueueNode takes a DMNode resource and converts it into a namespace/name
// string which is then put onto the work queue. This method should *not* be
// passed resources of any type other than DMNode.
func (c *Controller) enqueueNode(obj interface{}) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		utilruntime.HandleError(err)
		return
	}
	klog.V(4).Infof("Enqueue %s", key)
	c.nodequeue.Add(key)
}
