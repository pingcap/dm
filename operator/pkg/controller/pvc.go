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
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
)

func (c *Controller) syncReclaimPolicy(node *pv1alpha1.DMNode) error {
	ns := node.Namespace
	instanceName := node.Labels[instanceKey]

	l := labels.Set{instanceKey: instanceName}.AsSelector()

	pvcs, err := c.pvcLister.PersistentVolumeClaims(ns).List(l)
	if err != nil {
		return err
	}

	for _, pvc := range pvcs {
		if pvc.Spec.VolumeName == "" {
			continue
		}
		pvName := pvc.Spec.VolumeName
		pv, err := c.pvLister.Get(pvName)
		if err != nil {
			return err
		}

		if pv.Spec.PersistentVolumeReclaimPolicy == node.Spec.PVReclaimPolicy {
			continue
		}

		patchBytes := []byte(fmt.Sprintf(`{"spec":{"persistentVolumeReclaimPolicy":"%s"}}`, node.Spec.PVReclaimPolicy))

		err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
			_, err := c.kubeclientset.CoreV1().PersistentVolumes().Patch(pvName, types.StrategicMergePatchType, patchBytes)
			return err
		})
		c.recordPVEvent("patch", node, pvName, err)

		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) recordPVEvent(verb string, node *pv1alpha1.DMNode, pvName string, err error) {
	nodeName := node.Name
	if err == nil {
		reason := fmt.Sprintf("Successful%s", strings.Title(verb))
		msg := fmt.Sprintf("%s PV %s in DMNode %s successful",
			strings.ToLower(verb), pvName, nodeName)
		c.recorder.Event(node, corev1.EventTypeNormal, reason, msg)
	} else {
		reason := fmt.Sprintf("Failed%s", strings.Title(verb))
		msg := fmt.Sprintf("%s PV %s in DMNode %s failed error: %s",
			strings.ToLower(verb), pvName, nodeName, err)
		c.recorder.Event(node, corev1.EventTypeWarning, reason, msg)
	}
}
