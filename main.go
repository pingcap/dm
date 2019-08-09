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

package main

import (
	"flag"
	"os"
	"time"

	clientset "github.com/pingcap/dm/operator/pkg/client/clientset/versioned"
	informers "github.com/pingcap/dm/operator/pkg/client/informers/externalversions"
	"github.com/pingcap/dm/operator/pkg/controller"
	"github.com/pingcap/dm/operator/pkg/signals"

	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog"
	// Uncomment the following line to load the gcp plugin (only required to authenticate against GKE clusters).
	// _ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
)

var (
	httpReqTimeout          int
	masterURL               string
	kubeconfig              string
	burst                   int
	qps                     int
	workers                 int
	defaultStorageClassName string
	printVersion            bool
	resyncPeriod            int64
	masterPVC               bool
)

const (
	nsENV = "NAMESPACE"
)

func main() {
	klog.InitFlags(nil)
	flag.Parse()

	// set up signals so we handle the first shutdown signal gracefully
	stopCh := signals.SetupSignalHandler()

	ns := os.Getenv(nsENV)
	if ns == "" {
		klog.Fatalf("Error getting namespace env")
	}

	cfg, err := clientcmd.BuildConfigFromFlags(masterURL, kubeconfig)
	if err != nil {
		klog.Fatalf("Error building kubeconfig: %s", err.Error())
	}
	cfg.QPS = float32(qps)
	cfg.Burst = burst
	// klog.Infof("KubeClient config %#v", cfg)

	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("Error building kubernetes clientset: %s", err.Error())
	}

	dmClient, err := clientset.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("Error building example clientset: %s", err.Error())
	}

	kubeInformerFactory := kubeinformers.NewFilteredSharedInformerFactory(kubeClient, time.Second*time.Duration(resyncPeriod), ns, nil)
	dmInformerFactory := informers.NewFilteredSharedInformerFactory(dmClient, time.Second*time.Duration(resyncPeriod), ns, nil)

	controller := controller.NewController(kubeClient, dmClient, kubeInformerFactory,
		dmInformerFactory, httpReqTimeout, masterPVC, defaultStorageClassName)

	// notice that there is no need to run Start methods in a separate goroutine. (i.e. go kubeInformerFactory.Start(stopCh)
	// Start method is non-blocking and runs all registered informers in a dedicated goroutine.
	kubeInformerFactory.Start(stopCh)
	dmInformerFactory.Start(stopCh)

	if err = controller.Run(workers, stopCh); err != nil {
		klog.Fatalf("Error running controller: %s", err.Error())
	}
}

func init() {
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to a kubeconfig. Only required if out-of-cluster.")
	flag.StringVar(&masterURL, "master", "", "The address of the Kubernetes API server. Overrides any value in kubeconfig. Only required if out-of-cluster.")
	flag.IntVar(&burst, "kube-api-burst", 50, "Burst to use while talking with kubernetes apiserver.")
	flag.IntVar(&qps, "kube-api-qps", 30, "QPS to use while talking with kubernetes apiserver.")
	flag.BoolVar(&printVersion, "version", false, "Show version and quit")
	flag.BoolVar(&masterPVC, "master-pvc", false, "Whether to create PVC for dm master")
	flag.IntVar(&workers, "workers", 3, "The number of workers that are allowed to sync concurrently. Larger number = more responsive management, but more CPU (and network) load")
	flag.StringVar(&defaultStorageClassName, "default-storage-class-name", "standard", "Default storage class name")
	flag.Int64Var(&resyncPeriod, "resync-period", 60, "Resync period for resources")
	flag.IntVar(&httpReqTimeout, "http-req-timeout", 2, "Timeout for http request")
}
