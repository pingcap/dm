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

func (c *Controller) syncNodeConfigMap(node *pv1alpha1.DMNode) error {
	var newCM corev1.ConfigMap
	var err error
	ns := node.Namespace
	name := node.Name
	component := node.Labels[componentKey]
	if component == componentMaster {
		newCM, err = getNewMasterNodeConfigMap(node)
		if err != nil {
			return err
		}
	} else {
		newCM, err = getNewWorkerNodeConfigMap(node)
		if err != nil {
			return err
		}
	}

	oldCMTmp, err := c.configmapsLister.ConfigMaps(ns).Get(name)
	if errors.IsNotFound(err) {
		err = setConfigMapLastAppliedConfigAnnotation(&newCM)
		if err != nil {
			return err
		}
		return c.CreateConfigMap(node, &newCM)
	}
	if err != nil {
		return err
	}

	oldCM := oldCMTmp.DeepCopy()

	equal, err := configmapEqual(&newCM, oldCM)
	if err != nil {
		return err
	}
	if !equal {
		cm := *oldCM
		cm.Data = newCM.Data
		// TODO add unit test
		err = setConfigMapLastAppliedConfigAnnotation(&cm)
		if err != nil {
			return err
		}
		_, err = c.UpdateConfigMap(node, &cm)
		return err
	}

	return nil
}

//CreateConfigMap create new configmap
func (c *Controller) CreateConfigMap(node *pv1alpha1.DMNode, cm *corev1.ConfigMap) error {
	_, err := c.kubeclientset.CoreV1().ConfigMaps(cm.Namespace).Create(cm)
	if errors.IsAlreadyExists(err) {
		return err
	}
	c.recordConfigMapEvent("create", node, cm, err)
	return err
}

//UpdateConfigMap update a configmap
func (c *Controller) UpdateConfigMap(node *pv1alpha1.DMNode, cm *corev1.ConfigMap) (*corev1.ConfigMap, error) {
	ns := cm.Namespace
	cmName := cm.Name
	cmData := make(map[string]string)
	for k, v := range cm.Data {
		cmData[k] = v
	}

	var updateCM *corev1.ConfigMap
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		var updateErr error
		updateCM, updateErr = c.kubeclientset.CoreV1().ConfigMaps(ns).Update(cm)
		if updateErr == nil {
			klog.Infof("update ConfigMap: [%s/%s] successfully", ns, cmName)
			return nil
		}
		klog.Errorf("update ConfigMap: [%s/%s] error: %v", ns, cmName, updateErr)
		if updated, err := c.configmapsLister.ConfigMaps(ns).Get(cmName); err != nil {
			utilruntime.HandleError(fmt.Errorf("error getting updated ConfigMap %s/%s from lister: %v", ns, cmName, err))
		} else {
			cm = updated.DeepCopy()
			cm.Data = cmData
		}

		return updateErr
	})
	c.recordConfigMapEvent("update", node, cm, err)
	return updateCM, err
}

//DeleteConfigMap delete a configmap
func (c *Controller) DeleteConfigMap(node *pv1alpha1.DMNode, cm *corev1.ConfigMap) error {
	err := c.kubeclientset.CoreV1().ConfigMaps(cm.Namespace).Delete(cm.Name, nil)
	c.recordConfigMapEvent("delete", node, cm, err)
	return err
}

func (c *Controller) recordConfigMapEvent(verb string, node *pv1alpha1.DMNode, cm *corev1.ConfigMap, err error) {
	name := cm.Name
	ns := cm.Namespace
	if err == nil {
		reason := fmt.Sprintf("Successful%s", strings.Title(verb))
		msg := fmt.Sprintf("%s ConfigMap %s/%s successful",
			strings.ToLower(verb), ns, name)
		c.recorder.Event(node, corev1.EventTypeNormal, reason, msg)
	} else {
		reason := fmt.Sprintf("Failed%s", strings.Title(verb))
		msg := fmt.Sprintf("%s ConfigMap %s/%s failed error: %s",
			strings.ToLower(verb), ns, name, err)
		c.recorder.Event(node, corev1.EventTypeWarning, reason, msg)
	}
}
