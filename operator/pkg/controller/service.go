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
	"strings"

	pv1alpha1 "github.com/pingcap/dm/operator/pkg/apis/pingcap.com/v1alpha1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog"
)

func (c *Controller) syncNodeService(node *pv1alpha1.DMNode) error {
	if node.Spec.Service.Spec.Type == "" {
		return nil
	}
	ns := node.Namespace
	name := node.Name

	newSvc := getNewNodeService(node)
	oldSvcTmp, err := c.servicesLister.Services(ns).Get(name)
	if errors.IsNotFound(err) {
		err = setServiceAnnotation(newSvc, nil)
		if err != nil {
			return err
		}
		return c.CreateService(node, newSvc)
	}
	if err != nil {
		return err
	}

	oldSvc := oldSvcTmp.DeepCopy()

	equal, err := serviceEqual(newSvc, oldSvc)
	if err != nil {
		return err
	}
	if !equal {
		klog.Infof("Need to update service")
		svc := *oldSvc
		svc.Spec = newSvc.Spec
		err = setServiceAnnotation(&svc, newSvc)
		if err != nil {
			return err
		}
		// TODO add unit test
		svc.Spec.ClusterIP = oldSvc.Spec.ClusterIP
		if svc.Spec.Type == corev1.ServiceTypeNodePort || svc.Spec.Type == corev1.ServiceTypeLoadBalancer {
			syncNodePort(&svc, oldSvc)
		}

		_, err = c.UpdateService(node, &svc)
		return err
	}

	return nil
}
func (c *Controller) syncNodeHeadlessService(node *pv1alpha1.DMNode) error {
	ns := node.Namespace
	name := node.Name

	newSvc := getNewNodeHeadlessService(node)
	oldSvc, err := c.servicesLister.Services(ns).Get(peerName(name))
	if errors.IsNotFound(err) {
		err = setServiceAnnotation(newSvc, nil)
		if err != nil {
			return err
		}
		return c.CreateService(node, newSvc)
	}
	if err != nil {
		return err
	}

	equal, err := serviceEqual(newSvc, oldSvc)
	if err != nil {
		return err
	}
	if !equal {
		svc := *oldSvc
		svc.Spec = newSvc.Spec
		err = setServiceAnnotation(&svc, newSvc)
		if err != nil {
			return err
		}
		_, err = c.UpdateService(node, &svc)
		return err
	}

	return nil
}

func (c *Controller) CreateService(node *pv1alpha1.DMNode, svc *corev1.Service) error {
	_, err := c.kubeclientset.CoreV1().Services(svc.Namespace).Create(svc)
	if errors.IsAlreadyExists(err) {
		return err
	}
	c.recordServiceEvent("create", node, svc, err)
	key := svc.Namespace + "/" + svc.Labels[instanceKey]
	masterSvcLock.Lock()
	masterSvc[key] = svc.Name + "." + svc.Namespace
	masterSvcLock.Unlock()
	return err
}

func (c *Controller) UpdateService(node *pv1alpha1.DMNode, svc *corev1.Service) (*corev1.Service, error) {
	ns := svc.Namespace
	svcName := svc.Name
	svcSpec := svc.Spec.DeepCopy()

	var updateSvc *corev1.Service
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		var updateErr error
		updateSvc, updateErr = c.kubeclientset.CoreV1().Services(ns).Update(svc)
		if updateErr == nil {
			klog.Infof("update Service: [%s/%s] successfully", ns, svcName)
			return nil
		}
		klog.Errorf("update Service: [%s/%s] error: %v", ns, svcName, updateErr)
		if updated, err := c.servicesLister.Services(ns).Get(svcName); err != nil {
			utilruntime.HandleError(fmt.Errorf("error getting updated Service %s/%s from lister: %v", ns, svcName, err))
		} else {
			svc = updated.DeepCopy()
			svc.Spec = *svcSpec
		}

		return updateErr
	})
	c.recordServiceEvent("update", node, svc, err)
	return updateSvc, err
}

func (c *Controller) DeleteService(node *pv1alpha1.DMNode, svc *corev1.Service) error {
	err := c.kubeclientset.CoreV1().Services(svc.Namespace).Delete(svc.Name, nil)
	c.recordServiceEvent("delete", node, svc, err)
	return err
}

func (c *Controller) recordServiceEvent(verb string, node *pv1alpha1.DMNode, svc *corev1.Service, err error) {
	name := svc.Name
	ns := svc.Namespace
	if err == nil {
		reason := fmt.Sprintf("Successful%s", strings.Title(verb))
		msg := fmt.Sprintf("%s Service %s/%s successful",
			strings.ToLower(verb), ns, name)
		c.recorder.Event(node, corev1.EventTypeNormal, reason, msg)
	} else {
		reason := fmt.Sprintf("Failed%s", strings.Title(verb))
		msg := fmt.Sprintf("%s Service %s/%s failed error: %s",
			strings.ToLower(verb), ns, name, err)
		c.recorder.Event(node, corev1.EventTypeWarning, reason, msg)
	}
}
