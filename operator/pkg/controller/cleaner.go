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
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog"
)

const (
	skipReasonOrphanPodsCleanerIsNotMasterOrWorker = "orphan pods cleaner: member type is not master or worker"
	skipReasonOrphanPodsCleanerMasterNoPVC         = "orphan pods cleaner: no PVC created for master"
	skipReasonOrphanPodsCleanerPVCNameIsEmpty      = "orphan pods cleaner: pvcName is empty"
	skipReasonOrphanPodsCleanerPVCIsFound          = "orphan pods cleaner: pvc is found"
)

func (c *Controller) cleanOrphanPods(node *pv1alpha1.DMNode) (map[string]string, error) {
	ns := node.Namespace
	// for unit test
	skipReason := map[string]string{}
	instanceName := node.Labels[instanceKey]
	selector := labels.Set{instanceKey: instanceName}.AsSelector()
	pods, err := c.podLister.Pods(ns).List(selector)
	if err != nil {
		return skipReason, err
	}

	for _, pod := range pods {
		podName := pod.Name
		if pod.Labels == nil {
			continue
		}
		v := pod.Labels[componentKey]
		if v != componentMaster && v != componentWorker {
			skipReason[podName] = skipReasonOrphanPodsCleanerIsNotMasterOrWorker
			continue
		}
		if v == componentMaster && !c.masterPVC {
			skipReason[podName] = skipReasonOrphanPodsCleanerMasterNoPVC
			continue
		}

		var pvcName string
		for _, vol := range pod.Spec.Volumes {
			if vol.PersistentVolumeClaim != nil {
				pvcName = vol.PersistentVolumeClaim.ClaimName
				break
			}
		}
		if pvcName == "" {
			skipReason[podName] = skipReasonOrphanPodsCleanerPVCNameIsEmpty
			continue
		}

		_, err := c.pvcLister.PersistentVolumeClaims(ns).Get(pvcName)
		if err == nil {
			skipReason[podName] = skipReasonOrphanPodsCleanerPVCIsFound
			continue
		}

		if !errors.IsNotFound(err) {
			return skipReason, err
		}

		err = c.deletePod(node, pod.Name)
		if err != nil {
			return skipReason, err
		}
	}

	return skipReason, nil
}

func (c *Controller) deletePod(node *pv1alpha1.DMNode, podName string) error {
	ns := node.Namespace
	nodeName := node.Name
	err := c.kubeclientset.CoreV1().Pods(ns).Delete(podName, nil)
	if err != nil {
		klog.Errorf("failed to delete Pod: [%s/%s], DMNode: %s, %v", ns, podName, nodeName, err)
	} else {
		klog.V(4).Infof("delete Pod: [%s/%s] successfully, DMNode: %s", ns, podName, nodeName)
	}
	c.recordPodEvent("delete", node, podName, err)
	return err
}

func (c *Controller) recordPodEvent(verb string, node *pv1alpha1.DMNode, podName string, err error) {
	nodeName := node.GetName()
	if err == nil {
		reason := fmt.Sprintf("Successful%s", strings.Title(verb))
		msg := fmt.Sprintf("%s Pod %s in DMNode %s successful",
			strings.ToLower(verb), podName, nodeName)
		c.recorder.Event(node, corev1.EventTypeNormal, reason, msg)
	} else {
		reason := fmt.Sprintf("Failed%s", strings.Title(verb))
		msg := fmt.Sprintf("%s Pod %s in DMNode %s failed error: %s",
			strings.ToLower(verb), podName, nodeName, err)
		c.recorder.Event(node, corev1.EventTypeWarning, reason, msg)
	}
}
