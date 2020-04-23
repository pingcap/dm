// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package metricsproxy

import (
	"crypto/md5"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

// Proxy Interface
type Proxy interface {
	GetLabels() map[string]map[string]string
	vecDelete(prometheus.Labels) bool
}

// noteLabelsInMetricsProxy common function in Proxy
func noteLabelsInMetricsProxy(proxy Proxy, labels map[string]string) {
	labelsMd5Sum := labelsMd5Sum(labels)

	if _, ok := proxy.GetLabels()[labelsMd5Sum]; !ok {
		proxy.GetLabels()[labelsMd5Sum] = labels
	}
}

// labelsMd5Sum common function in Proxy
func labelsMd5Sum(labels map[string]string) string {
	var str string
	for _, label := range labels {
		str += label
	}
	return fmt.Sprintf("%x", md5.Sum([]byte(str)))
}

// findAndDeleteLabelsInMetricsProxy common function in Proxy
func findAndDeleteLabelsInMetricsProxy(proxy Proxy, labels prometheus.Labels) bool {
	var (
		deleteLabelsList = make([]map[string]string, 0)
		res              = true
	)
	inputLabelsLen := len(labels)
	for _, ls := range proxy.GetLabels() {
		t := 0
		for k := range labels {
			if ls[k] == labels[k] {
				t++
			}
		}
		if t == inputLabelsLen {
			deleteLabelsList = append(deleteLabelsList, ls)
		}
	}

	for _, deleteLabels := range deleteLabelsList {
		res = proxy.vecDelete(deleteLabels) && res
	}
	return res
}
