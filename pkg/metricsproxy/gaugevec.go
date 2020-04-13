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

// GaugeVecProxy to proxy prometheus.GaugeVec
type GaugeVecProxy struct {
	mu sync.Mutex

	LabelNames []string
	Labels     map[string]map[string]string
	*prometheus.GaugeVec
}

// NewGaugeVec creates a new GaugeVec based on the provided GaugeOpts and
// partitioned by the given label names.
func NewGaugeVec(opts prometheus.GaugeOpts, labelNames []string) *GaugeVecProxy {
	return &GaugeVecProxy{
		LabelNames: labelNames,
		Labels:     make(map[string]map[string]string, 0),
		GaugeVec:   prometheus.NewGaugeVec(opts, labelNames),
	}
}

// WithLabelValues works as GetMetricWithLabelValues, but panics where
// GetMetricWithLabelValues would have returned an error. Not returning an
// error allows shortcuts like
//     myVec.WithLabelValues("404", "GET").Add(42)
func (c *GaugeVecProxy) WithLabelValues(lvs ...string) prometheus.Gauge {
	if len(lvs) > 0 {
		labels := make(map[string]string, len(lvs))
		for index, label := range lvs {
			labels[c.LabelNames[index]] = label
		}
		c.mu.Lock()
		noteLabelsInMetricsProxy(c, labels)
		c.mu.Unlock()
	}
	return c.GaugeVec.WithLabelValues(lvs...)
}

// With works as GetMetricWith, but panics where GetMetricWithLabels would have
// returned an error. Not returning an error allows shortcuts like
//     myVec.With(prometheus.Labels{"code": "404", "method": "GET"}).Add(42)
func (c *GaugeVecProxy) With(labels prometheus.Labels) prometheus.Gauge {
	if len(labels) > 0 {
		c.mu.Lock()
		noteLabelsInMetricsProxy(c, labels)
		c.mu.Unlock()
	}

	return c.GaugeVec.With(labels)
}

// DeleteAllAboutLabels Remove all labelsValue with these labels
func (c *GaugeVecProxy) DeleteAllAboutLabels(labels prometheus.Labels) bool {
	if len(labels) == 0 {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return findAndDeleteLabelsInMetricsProxy(c, labels)
}

// GetLabels to support get GaugeVecProxy's Labels when you use Proxy object
func (c *GaugeVecProxy) GetLabels() map[string]map[string]string {
	return c.Labels
}

// vecDelete to support delete GaugeVecProxy's Labels when you use Proxy object
func (c *GaugeVecProxy) vecDelete(labels prometheus.Labels) bool {
	return c.GaugeVec.Delete(labels)
}
