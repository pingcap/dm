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

	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog"
)

func (c *Controller) syncNodeStatefulSet(node *pv1alpha1.DMNode) error {
	ns := node.Namespace
	name := node.Name
	klog.Infof("Sync DMNode %s/%s", ns, name)
	newNodeSet, err := getNewNodeSet(node, c.masterPVC)
	if err != nil {
		return err
	}
	// klog.Infof("Sync DMNode newNodeSet [%#v]", newNodeSet)
	oldNodeSetTmp, err := c.statefulsetsLister.StatefulSets(ns).Get(name)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	if errors.IsNotFound(err) {
		err = setLastAppliedConfigAnnotation(newNodeSet)
		if err != nil {
			return err
		}
		return c.CreateStatefulSet(node, newNodeSet)
	}

	oldNodeSet := oldNodeSetTmp.DeepCopy()
	// klog.Infof("Sync DMNode oldNodeSet [%#v]", oldNodeSet)
	node.Status.StatefulSet = *(oldNodeSet.Status.DeepCopy())

	return c.updateStatefulSet(node, newNodeSet, oldNodeSet)
}
func (c *Controller) updateStatefulSet(node *pv1alpha1.DMNode, newNodeSet, oldNodeSet *apps.StatefulSet) error {
	if !statefulSetEqual(*newNodeSet, *oldNodeSet) {
		set := *oldNodeSet
		set.Spec.Template = newNodeSet.Spec.Template
		*set.Spec.Replicas = *newNodeSet.Spec.Replicas
		set.Spec.UpdateStrategy = newNodeSet.Spec.UpdateStrategy
		err := setLastAppliedConfigAnnotation(&set)
		if err != nil {
			return err
		}
		_, err = c.UpdateStatefulSet(node, &set)
		return err
	}
	return nil
}

// CreateStatefulSet create a StatefulSet in a DMNode.
func (c *Controller) CreateStatefulSet(node *pv1alpha1.DMNode, set *apps.StatefulSet) error {
	_, err := c.kubeclientset.AppsV1().StatefulSets(set.Namespace).Create(set)
	// sink already exists errors
	if errors.IsAlreadyExists(err) {
		return err
	}
	c.recordStatefulSetEvent("create", node, set, err)
	return err
}

// UpdateStatefulSet update a StatefulSet in a DMNode.
func (c *Controller) UpdateStatefulSet(node *pv1alpha1.DMNode, set *apps.StatefulSet) (*apps.StatefulSet, error) {
	ns := set.Namespace
	setName := set.Name
	setSpec := set.Spec.DeepCopy()
	var updatedSS *apps.StatefulSet

	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		// TODO: verify if StatefulSet identity(name, namespace, labels) matches DMNode
		var updateErr error
		updatedSS, updateErr = c.kubeclientset.AppsV1().StatefulSets(ns).Update(set)
		if updateErr == nil {
			klog.Infof("DMNode: [%s/%s]'s StatefulSet: [%s/%s] updated successfully", ns, setName, ns, setName)
			return nil
		}
		klog.Errorf("failed to update DMNode: [%s/%s]'s StatefulSet: [%s/%s], error: %v", ns, setName, ns, setName, updateErr)

		if updated, err := c.statefulsetsLister.StatefulSets(ns).Get(setName); err == nil {
			// make a copy so we don't mutate the shared cache
			set = updated.DeepCopy()
			set.Spec = *setSpec
		} else {
			utilruntime.HandleError(fmt.Errorf("error getting updated StatefulSet %s/%s from lister: %v", ns, setName, err))
		}
		return updateErr
	})

	c.recordStatefulSetEvent("update", node, set, err)
	return updatedSS, err
}

// DeleteStatefulSet delete a StatefulSet in a DMNode.
func (c *Controller) DeleteStatefulSet(node *pv1alpha1.DMNode, set *apps.StatefulSet) error {
	err := c.kubeclientset.AppsV1().StatefulSets(set.Namespace).Delete(set.Name, nil)
	c.recordStatefulSetEvent("delete", node, set, err)
	return err
}

func (c *Controller) recordStatefulSetEvent(verb string, node *pv1alpha1.DMNode, set *apps.StatefulSet, err error) {
	setName := set.Name
	ns := set.Namespace
	if err == nil {
		reason := fmt.Sprintf("Successful%s", strings.Title(verb))
		message := fmt.Sprintf("%s StatefulSet %s/%s successfully",
			strings.ToLower(verb), ns, setName)
		c.recorder.Event(node, corev1.EventTypeNormal, reason, message)
	} else {
		reason := fmt.Sprintf("Failed%s", strings.Title(verb))
		message := fmt.Sprintf("%s StatefulSet %s/%s failed error: %s",
			strings.ToLower(verb), ns, setName, err)
		c.recorder.Event(node, corev1.EventTypeWarning, reason, message)
	}
}
