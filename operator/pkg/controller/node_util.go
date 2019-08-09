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
	"bytes"
	"fmt"

	pv1alpha1 "github.com/pingcap/dm/operator/pkg/apis/pingcap.com/v1alpha1"

	"github.com/BurntSushi/toml"
	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func validNodeLabels(labels map[string]string) error {
	err := validCommonLabels(labels)
	if err != nil {
		return err
	}
	v := labels[componentKey]
	if v != componentMaster && v != componentWorker {
		return fmt.Errorf("Value of %s should be %s or %s in Labels", componentKey, componentMaster, componentWorker)
	}
	return nil
}
func validDMNode(node *pv1alpha1.DMNode) error {
	if node.Spec.Image == "" {
		return fmt.Errorf("Miss image config")
	}

	component := node.Labels[componentKey]
	if component == componentWorker {
		if node.Spec.Source.ID == "" || node.Spec.Source.User == "" || node.Spec.Source.Host == "" ||
			node.Spec.Source.Password == "" || node.Spec.Source.Port < 1 || node.Spec.Source.Port > 65535 {
			return fmt.Errorf("Invalid source config %#v", node.Spec.Source)
		}
		if node.Spec.NodeConfig.ServerID < 1 || node.Spec.NodeConfig.ServerID > 4294967295 {
			return fmt.Errorf("Invalid server-id config %#v", node.Spec.NodeConfig.ServerID)
		}
		if node.Spec.NodeConfig.Flavor == "" {
			return fmt.Errorf("Invalid flavor config")
		}
	}
	return nil
}
func adjustNodeConfig(node *pv1alpha1.DMNode, sc string) {
	labels := node.Labels
	v := labels[componentKey]
	if v == componentWorker {
		labels[sourceKey] = node.Spec.Source.ID
	}

	if node.Spec.StorageClassName == "" {
		node.Spec.StorageClassName = sc
	}
	if node.Spec.Timezone == "" {
		node.Spec.Timezone = "UTC"
	}
	if node.Spec.PVReclaimPolicy == "" {
		node.Spec.PVReclaimPolicy = "Retain"
	}
	if node.Spec.ImagePullPolicy == "" {
		node.Spec.ImagePullPolicy = "IfNotPresent"
	}
	if node.Spec.Replicas < 0 || node.Spec.Replicas > 1 {
		node.Spec.Replicas = 1
	}
	component := node.Labels[componentKey]
	if component == componentWorker {
		if node.Spec.NodeConfig.LogLevel == "" {
			node.Spec.NodeConfig.LogLevel = "info"
		}
		if node.Spec.NodeConfig.RelayDir == "" {
			node.Spec.NodeConfig.RelayDir = "./relay_log"
		}
		node.Spec.NodeConfig.LogFile = ""
		node.Spec.NodeConfig.WorkerAddr = fmt.Sprintf(":%d", workerPort)
	} else {
		if node.Spec.Service.Spec.Type == "" {
			node.Spec.Service.Spec.Type = "NodePort"
		}
		if node.Spec.MasterConfig.LogLevel == "" {
			node.Spec.MasterConfig.LogLevel = "info"
		}
		node.Spec.MasterConfig.LogFile = ""
		node.Spec.MasterConfig.MasterAddr = fmt.Sprintf(":%d", masterPort)
	}
}

// getOwnerRef returns OwnerReference
func getOwnerRef(node *pv1alpha1.DMNode) metav1.OwnerReference {
	controller := true
	blockOwnerDeletion := true
	return metav1.OwnerReference{
		APIVersion:         dmNodeKind.GroupVersion().String(),
		Kind:               dmNodeKind.Kind,
		Name:               node.Name,
		UID:                node.UID,
		Controller:         &controller,
		BlockOwnerDeletion: &blockOwnerDeletion,
	}
}
func getNewNodeService(node *pv1alpha1.DMNode) *corev1.Service {
	ns := node.Namespace
	svcName := node.Name
	labels := node.Labels
	labels[managedByKey] = controllerAgentName

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            svcName,
			Namespace:       ns,
			Labels:          labels,
			OwnerReferences: []metav1.OwnerReference{getOwnerRef(node)},
		},
		Spec: node.Spec.Service.Spec,
	}
	svc.Spec.Ports = []corev1.ServicePort{
		{
			Name:       portName,
			Port:       masterPort,
			TargetPort: intstr.FromInt(masterPort),
			Protocol:   corev1.ProtocolTCP,
		},
	}
	for _, port := range node.Spec.Service.Spec.Ports {
		for i, newPort := range svc.Spec.Ports {
			if newPort.Name == port.Name {
				if port.NodePort != 0 {
					svc.Spec.Ports[i].NodePort = port.NodePort
				}
			}
		}
	}
	if svc.Spec.Selector == nil {
		svc.Spec.Selector = make(map[string]string)
	}
	svc.Spec.Selector = labels
	if node.Spec.Service.Annotations != nil {
		svc.Annotations = make(map[string]string)
		for k, v := range node.Spec.Service.Annotations {
			svc.Annotations[k] = v
		}
	}
	return svc
}

func getNewNodeHeadlessService(node *pv1alpha1.DMNode) *corev1.Service {
	var port int
	ns := node.Namespace
	name := node.Name
	svcName := name
	label := node.Labels
	label[managedByKey] = controllerAgentName
	component := node.Labels[componentKey]
	if component == componentMaster {
		port = masterPort
	} else {
		port = workerPort
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            peerName(svcName),
			Namespace:       ns,
			Labels:          label,
			OwnerReferences: []metav1.OwnerReference{getOwnerRef(node)},
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Ports: []corev1.ServicePort{
				{
					Name:       "peer",
					Port:       int32(port),
					TargetPort: intstr.FromInt(port),
					Protocol:   corev1.ProtocolTCP,
				},
			},
			Selector: label,
		},
	}
	return svc
}

func getNewMasterNodeConfigMap(node *pv1alpha1.DMNode) (corev1.ConfigMap, error) {
	config := node.Spec.MasterConfig
	var depList []*pv1alpha1.DeployMapper
	deployLock.RLock()
	for k, v := range masterDeploy {
		dep := &pv1alpha1.DeployMapper{
			Source: k,
			Worker: v,
		}
		depList = append(depList, dep)
	}
	deployLock.RUnlock()
	config.Deploy = depList
	label := node.Labels
	label[managedByKey] = controllerAgentName
	buf := new(bytes.Buffer)
	if err := toml.NewEncoder(buf).Encode(config); err != nil {
		return corev1.ConfigMap{}, err
	}
	data := make(map[string]string)
	data[masterConfigFile] = buf.String()
	cm := corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            node.Name,
			Labels:          label,
			OwnerReferences: []metav1.OwnerReference{getOwnerRef(node)},
			Namespace:       node.Namespace,
		},
		Data: data,
	}
	return cm, nil
}
func getNewWorkerNodeConfigMap(node *pv1alpha1.DMNode) (corev1.ConfigMap, error) {
	config := node.Spec.NodeConfig
	config.SourceID = node.Spec.Source.ID
	var maxA int
	var amap *int
	if node.Spec.Source.MaxAllowedPacket != nil {
		maxA = *node.Spec.Source.MaxAllowedPacket
		amap = &maxA
	}
	config.From = pv1alpha1.DBConfig{
		DMDB: pv1alpha1.DMDB{
			Host:     node.Spec.Source.Host,
			Port:     node.Spec.Source.Port,
			User:     node.Spec.Source.User,
			Password: node.Spec.Source.Password,
		},
		MaxAllowedPacket: amap,
	}
	label := node.Labels
	label[managedByKey] = controllerAgentName
	buf := new(bytes.Buffer)
	if err := toml.NewEncoder(buf).Encode(config); err != nil {
		return corev1.ConfigMap{}, err
	}
	data := make(map[string]string)
	data[workerConfigFile] = buf.String()
	cm := corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            node.Name,
			Labels:          label,
			OwnerReferences: []metav1.OwnerReference{getOwnerRef(node)},
			Namespace:       node.Namespace,
		},
		Data: data,
	}
	return cm, nil
}

func getNewNodeSet(node *pv1alpha1.DMNode, masterPVC bool) (*apps.StatefulSet, error) {
	var containerName string
	var command []string
	var port int
	var q resource.Quantity
	var err error
	var vct []corev1.PersistentVolumeClaim

	ns := node.Namespace
	name := node.Name
	label := node.Labels
	label[managedByKey] = controllerAgentName
	component := label[componentKey]

	storageClassName := node.Spec.StorageClassName

	if node.Spec.Requests != nil {
		size := node.Spec.Requests.Storage
		q, err = resource.ParseQuantity(size)
		if err != nil {
			return nil, fmt.Errorf("cant' get storage size: %s for DMNode: %s/%s, %v", size, ns, name, err)
		}
	}
	vct = []corev1.PersistentVolumeClaim{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: component,
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteOnce,
				},
				StorageClassName: &storageClassName,
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: q,
					},
				},
			},
		},
	}

	if component == componentMaster {
		port = masterPort
		containerName = componentMaster
		command = []string{"./bin/dm-master", fmt.Sprintf("-config=%s/%s", configDir, masterConfigFile)}
		if !masterPVC {
			vct = []corev1.PersistentVolumeClaim{}
		}
	} else {
		port = workerPort
		containerName = componentWorker
		command = []string{"./bin/dm-worker", fmt.Sprintf("-config=%s/%s", configDir, workerConfigFile)}
	}

	podAnnotations := CombineAnnotations(AnnProm(port), node.Spec.Annotations)

	annMount, annVolume := annotationsMountVolume()
	volMounts := []corev1.VolumeMount{
		annMount,
		{Name: "config", ReadOnly: true, MountPath: configDir},
		{Name: component, MountPath: dataDir},
	}
	vols := []corev1.Volume{
		annVolume,
		{
			Name: "config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: name,
					},
				},
			},
		},
	}

	nodeSet := &apps.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       ns,
			Labels:          label,
			OwnerReferences: []metav1.OwnerReference{getOwnerRef(node)},
		},
		Spec: apps.StatefulSetSpec{
			Replicas: func() *int32 { r := node.Spec.Replicas; return &r }(),
			Selector: &metav1.LabelSelector{MatchLabels: label},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      label,
					Annotations: podAnnotations,
				},
				Spec: corev1.PodSpec{
					Affinity:     node.Spec.Affinity,
					NodeSelector: node.Spec.NodeSelector,
					Containers: []corev1.Container{
						{
							Name:            containerName,
							Image:           node.Spec.Image,
							Command:         command,
							ImagePullPolicy: node.Spec.ImagePullPolicy,
							Ports: []corev1.ContainerPort{
								{
									Name:          "server",
									ContainerPort: int32(port),
									Protocol:      corev1.ProtocolTCP,
								},
							},
							VolumeMounts: volMounts,
							Resources:    resourceRequirement(node.Spec.ContainerSpec),
							Env: []corev1.EnvVar{
								{
									Name: "NAMESPACE",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "metadata.namespace",
										},
									},
								},
								{
									Name:  "PEER_SERVICE_NAME",
									Value: peerName(name),
								},
								{
									Name:  "SERVICE_NAME",
									Value: name,
								},
								{
									Name:  "TZ",
									Value: node.Spec.Timezone,
								},
							},
						},
					},
					RestartPolicy: corev1.RestartPolicyAlways,
					Tolerations:   node.Spec.Tolerations,
					Volumes:       vols,
				},
			},
			VolumeClaimTemplates: vct,
			ServiceName:          peerName(name),
			PodManagementPolicy:  apps.ParallelPodManagement,
			UpdateStrategy: apps.StatefulSetUpdateStrategy{
				Type: apps.RollingUpdateStatefulSetStrategyType,
			},
		},
	}

	return nodeSet, nil
}
