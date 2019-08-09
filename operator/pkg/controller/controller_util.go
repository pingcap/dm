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
	"encoding/json"
	"fmt"

	pv1alpha1 "github.com/pingcap/dm/operator/pkg/apis/pingcap.com/v1alpha1"

	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog"
)

func validDBConfig(db pv1alpha1.DMDB) bool {
	if db.Host == "" || db.User == "" || db.Password == "" || db.Port < 1 || db.Port > 65535 {
		return false
	}
	return true
}
func validCommonLabels(labels map[string]string) error {
	if labels == nil {
		return fmt.Errorf("No Labels")
	}
	if _, ok := labels[instanceKey]; !ok {
		return fmt.Errorf("%s is required in Labels", instanceKey)
	}
	if _, ok := labels[componentKey]; !ok {
		return fmt.Errorf("%s is required in Labels", componentKey)
	}
	return nil
}

func peerName(name string) string {
	return name + "-peer"
}

func syncNodePort(svc, oldSvc *corev1.Service) {
	for _, port := range oldSvc.Spec.Ports {
		for i, newPort := range svc.Spec.Ports {
			if newPort.Name == port.Name {
				if newPort.NodePort == 0 {
					svc.Spec.Ports[i].NodePort = port.NodePort
				}
			}
		}
	}
}

// setServiceAnnotation set last applied config info to Service's annotation
func setServiceAnnotation(svc, new *corev1.Service) error {
	svcApply, err := encode(svc.Spec)
	if err != nil {
		return err
	}

	svc.Annotations = make(map[string]string)

	svc.Annotations[lastAppliedConfigAnnotation] = svcApply
	if new != nil && new.Annotations != nil {
		for k, v := range new.Annotations {
			svc.Annotations[k] = v
		}
	}

	return nil
}
func encode(obj interface{}) (string, error) {
	b, err := json.Marshal(obj)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// serviceEqual compares the new Service's spec with old Service's last applied config
func serviceEqual(new, old *corev1.Service) (bool, error) {
	if !serviceAnnoEqual(new, old) {
		klog.Infof("Service %s/%s Annotation not equal, old: [%#v], new: [%#v]", new.Namespace, new.Name, old.Annotations, new.Annotations)
		return false, nil
	}
	oldSpec := corev1.ServiceSpec{}
	if lastAppliedConfig, ok := old.Annotations[lastAppliedConfigAnnotation]; ok {
		err := json.Unmarshal([]byte(lastAppliedConfig), &oldSpec)
		if err != nil {
			klog.Errorf("unmarshal ServiceSpec: [%s/%s]'s applied config failed,error: %v", old.Namespace, old.Name, err)
			return false, err
		}
		klog.V(5).Infof("Service %s/%s, oldSpec [%#v], newSpec [%#v]", new.Namespace, new.Name, oldSpec, new.Spec)
		return apiequality.Semantic.DeepEqual(oldSpec, new.Spec), nil
	}
	return false, nil
}

// serviceAnnoEqual compares the new Service's annotation with old Service's annotation
func serviceAnnoEqual(new, old *corev1.Service) bool {
	if new.Annotations == nil {
		return true
	}
	if old.Annotations == nil {
		return false
	}
	for k, v := range new.Annotations {
		vo, ok := old.Annotations[k]
		if ok && vo == v {
			continue
		}
		return false
	}
	return true
}

// setConfigMapLastAppliedConfigAnnotation set last applied config info to ConfigMap's annotation
func setConfigMapLastAppliedConfigAnnotation(cm *corev1.ConfigMap) error {
	cmApply, err := encode(cm.Data)
	if err != nil {
		return err
	}
	if cm.Annotations == nil {
		cm.Annotations = make(map[string]string)
	}
	cm.Annotations[lastAppliedConfigAnnotation] = cmApply
	return nil
}

// configmapEqual compares the new ConfigMap's spec with old ConfigMap's last applied config
func configmapEqual(new, old *corev1.ConfigMap) (bool, error) {
	oldData := make(map[string]string)
	if lastAppliedConfig, ok := old.Annotations[lastAppliedConfigAnnotation]; ok {
		err := json.Unmarshal([]byte(lastAppliedConfig), &oldData)
		if err != nil {
			klog.Errorf("unmarshal ConfigMapSpec: [%s/%s]'s applied config failed,error: %v", old.Namespace, old.Name, err)
			return false, err
		}
		if len(oldData) != len(new.Data) {
			return false, nil
		}
		for k, v := range oldData {
			if _, ok := new.Data[k]; !ok {
				return false, nil
			}
			nv := new.Data[k]
			if nv != v {
				return false, nil
			}
		}

		return true, nil
	}
	return false, nil
}

// setLastAppliedConfigAnnotation set last applied config info to Statefulset's annotation
func setLastAppliedConfigAnnotation(set *apps.StatefulSet) error {
	setApply, err := encode(set.Spec)
	if err != nil {
		return err
	}
	if set.Annotations == nil {
		set.Annotations = map[string]string{}
	}
	set.Annotations[lastAppliedConfigAnnotation] = setApply

	templateApply, err := encode(set.Spec.Template.Spec)
	if err != nil {
		return err
	}
	if set.Spec.Template.Annotations == nil {
		set.Spec.Template.Annotations = map[string]string{}
	}
	set.Spec.Template.Annotations[lastAppliedConfigAnnotation] = templateApply
	return nil
}

// getLastAppliedConfig get last applied config info from Statefulset's annotation
func getLastAppliedConfig(set *apps.StatefulSet) (*apps.StatefulSetSpec, *corev1.PodSpec, error) {
	specAppliedConfig, ok := set.Annotations[lastAppliedConfigAnnotation]
	if !ok {
		return nil, nil, fmt.Errorf("last applied spec config for statefulset:[%s/%s] not found", set.Namespace, set.Name)
	}
	spec := &apps.StatefulSetSpec{}
	err := json.Unmarshal([]byte(specAppliedConfig), spec)
	if err != nil {
		return nil, nil, err
	}

	podSpecAppliedConfig, ok := set.Spec.Template.Annotations[lastAppliedConfigAnnotation]
	if !ok {
		return nil, nil, fmt.Errorf("last applied template.spec config for statefulset:[%s/%s] not found", set.Namespace, set.Name)
	}
	podSpec := &corev1.PodSpec{}
	err = json.Unmarshal([]byte(podSpecAppliedConfig), podSpec)
	if err != nil {
		return nil, nil, err
	}

	return spec, podSpec, nil
}

// statefulSetEqual compares the new Statefulset's spec with old Statefulset's last applied config
func statefulSetEqual(new apps.StatefulSet, old apps.StatefulSet) bool {
	oldConfig := apps.StatefulSetSpec{}
	if lastAppliedConfig, ok := old.Annotations[lastAppliedConfigAnnotation]; ok {
		err := json.Unmarshal([]byte(lastAppliedConfig), &oldConfig)
		if err != nil {
			klog.Errorf("unmarshal Statefulset: [%s/%s]'s applied config failed,error: %v", old.GetNamespace(), old.GetName(), err)
			return false
		}
		klog.V(5).Infof("oldConfig.Replicas [%#v], new.Spec.Replicas [%#v]", *oldConfig.Replicas, *new.Spec.Replicas)
		klog.V(5).Infof("oldConfig.Template [%#v], new.Spec.Template [%#v]", oldConfig.Template, new.Spec.Template)
		klog.V(5).Infof("oldConfig.UpdateStrategy [%#v], new.Spec.UpdateStrategy [%#v]", oldConfig.UpdateStrategy, new.Spec.UpdateStrategy)
		return *oldConfig.Replicas == *new.Spec.Replicas &&
			apiequality.Semantic.DeepEqual(oldConfig.Template, new.Spec.Template) &&
			apiequality.Semantic.DeepEqual(oldConfig.UpdateStrategy, new.Spec.UpdateStrategy)
	}
	return false
}

// templateEqual compares the new podTemplateSpec's spec with old podTemplateSpec's last applied config
func templateEqual(new corev1.PodTemplateSpec, old corev1.PodTemplateSpec) bool {
	oldConfig := corev1.PodSpec{}
	if lastAppliedConfig, ok := old.Annotations[lastAppliedConfigAnnotation]; ok {
		err := json.Unmarshal([]byte(lastAppliedConfig), &oldConfig)
		if err != nil {
			klog.Errorf("unmarshal PodTemplate: [%s/%s]'s last applied config failed, error: %v", old.Namespace, old.Name, err)
			return false
		}
		return apiequality.Semantic.DeepEqual(oldConfig, new.Spec)
	}
	return false
}
func annotationsMountVolume() (corev1.VolumeMount, corev1.Volume) {
	m := corev1.VolumeMount{Name: "annotations", ReadOnly: true, MountPath: "/etc/podinfo"}
	v := corev1.Volume{
		Name: "annotations",
		VolumeSource: corev1.VolumeSource{
			DownwardAPI: &corev1.DownwardAPIVolumeSource{
				Items: []corev1.DownwardAPIVolumeFile{
					{
						Path:     "annotations",
						FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.annotations"},
					},
				},
			},
		},
	}
	return m, v
}

// AnnProm adds annotations for prometheus scraping metrics
func AnnProm(port int) map[string]string {
	return map[string]string{
		"prometheus.io/scrape": "true",
		"prometheus.io/path":   "/metrics",
		"prometheus.io/port":   fmt.Sprintf("%d", port),
	}
}

// CombineAnnotations merges two annotations maps
func CombineAnnotations(a, b map[string]string) map[string]string {
	if a == nil {
		a = make(map[string]string)
	}
	for k, v := range b {
		a[k] = v
	}
	return a
}

// resourceRequirement creates ResourceRequirements for MemberSpec
// Optionally pass in a default value
func resourceRequirement(spec pv1alpha1.ContainerSpec, defaultRequests ...corev1.ResourceRequirements) corev1.ResourceRequirements {
	rr := corev1.ResourceRequirements{}
	if len(defaultRequests) > 0 {
		defaultRequest := defaultRequests[0]
		rr.Requests = make(map[corev1.ResourceName]resource.Quantity)
		rr.Requests[corev1.ResourceCPU] = defaultRequest.Requests[corev1.ResourceCPU]
		rr.Requests[corev1.ResourceMemory] = defaultRequest.Requests[corev1.ResourceMemory]
		rr.Limits = make(map[corev1.ResourceName]resource.Quantity)
		rr.Limits[corev1.ResourceCPU] = defaultRequest.Limits[corev1.ResourceCPU]
		rr.Limits[corev1.ResourceMemory] = defaultRequest.Limits[corev1.ResourceMemory]
	}
	if spec.Requests != nil {
		if rr.Requests == nil {
			rr.Requests = make(map[corev1.ResourceName]resource.Quantity)
		}
		if spec.Requests.CPU != "" {
			if q, err := resource.ParseQuantity(spec.Requests.CPU); err != nil {
				klog.Errorf("failed to parse CPU resource %s to quantity: %v", spec.Requests.CPU, err)
			} else {
				rr.Requests[corev1.ResourceCPU] = q
			}
		}
		if spec.Requests.Memory != "" {
			if q, err := resource.ParseQuantity(spec.Requests.Memory); err != nil {
				klog.Errorf("failed to parse memory resource %s to quantity: %v", spec.Requests.Memory, err)
			} else {
				rr.Requests[corev1.ResourceMemory] = q
			}
		}
	}
	if spec.Limits != nil {
		if rr.Limits == nil {
			rr.Limits = make(map[corev1.ResourceName]resource.Quantity)
		}
		if spec.Limits.CPU != "" {
			if q, err := resource.ParseQuantity(spec.Limits.CPU); err != nil {
				klog.Errorf("failed to parse CPU resource %s to quantity: %v", spec.Limits.CPU, err)
			} else {
				rr.Limits[corev1.ResourceCPU] = q
			}
		}
		if spec.Limits.Memory != "" {
			if q, err := resource.ParseQuantity(spec.Limits.Memory); err != nil {
				klog.Errorf("failed to parse memory resource %s to quantity: %v", spec.Limits.Memory, err)
			} else {
				rr.Limits[corev1.ResourceMemory] = q
			}
		}
	}
	return rr
}
