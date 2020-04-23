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
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// CounterVecProxy to proxy prometheus.CounterVec
type CounterVecProxy struct {
	mu sync.Mutex

	LabelNames []string
	Labels     map[string]map[string]string
	*prometheus.CounterVec
}

// NewCounterVec creates a new CounterVec based on the provided CounterOpts and
// partitioned by the given label names.
func NewCounterVec(opts prometheus.CounterOpts, labelNames []string) *CounterVecProxy {
	return &CounterVecProxy{
		LabelNames: labelNames,
		Labels:     make(map[string]map[string]string, 0),
		CounterVec: prometheus.NewCounterVec(opts, labelNames),
	}
}

// WithLabelValues works as GetMetricWithLabelValues, but panics where
// GetMetricWithLabelValues would have returned an error. Not returning an
// error allows shortcuts like
//     myVec.WithLabelValues("404", "GET").Add(42)
func (c *CounterVecProxy) WithLabelValues(lvs ...string) prometheus.Counter {
	if len(lvs) > 0 {
		labels := make(map[string]string, len(lvs))
		for index, label := range lvs {
			labels[c.LabelNames[index]] = label
		}
		c.mu.Lock()
		noteLabelsInMetricsProxy(c, labels)
		c.mu.Unlock()
	}
	return c.CounterVec.WithLabelValues(lvs...)
}

// With works as GetMetricWith, but panics where GetMetricWithLabels would have
// returned an error. Not returning an error allows shortcuts like
//     myVec.With(prometheus.Labels{"code": "404", "method": "GET"}).Add(42)
func (c *CounterVecProxy) With(labels prometheus.Labels) prometheus.Counter {
	if len(labels) > 0 {
		c.mu.Lock()
		noteLabelsInMetricsProxy(c, labels)
		c.mu.Unlock()
	}

	return c.CounterVec.With(labels)
}

// DeleteAllAboutLabels Remove all labelsValue with these labels
func (c *CounterVecProxy) DeleteAllAboutLabels(labels prometheus.Labels) bool {
	if len(labels) == 0 {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return findAndDeleteLabelsInMetricsProxy(c, labels)
}

// GetLabels to support get CounterVecProxy's Labels when you use Proxy object
func (c *CounterVecProxy) GetLabels() map[string]map[string]string {
	return c.Labels
}

// vecDelete to support delete CounterVecProxy's Labels when you use Proxy object
func (c *CounterVecProxy) vecDelete(labels prometheus.Labels) bool {
	return c.CounterVec.Delete(labels)
}
