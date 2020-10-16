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

// HistogramVecProxy to proxy prometheus.HistogramVec
type HistogramVecProxy struct {
	mu sync.Mutex

	LabelNamesIndex map[string]int
	Labels          map[string][]string
	*prometheus.HistogramVec
}

// NewHistogramVec creates a new HistogramVec based on the provided HistogramOpts and
// partitioned by the given label names.
func NewHistogramVec(opts prometheus.HistogramOpts, labelNames []string) *HistogramVecProxy {
	histogramVecProxy := &HistogramVecProxy{
		LabelNamesIndex: make(map[string]int),
		Labels:          make(map[string][]string),
		HistogramVec:    prometheus.NewHistogramVec(opts, labelNames),
	}
	for idx, v := range labelNames {
		histogramVecProxy.LabelNamesIndex[v] = idx
	}
	return histogramVecProxy
}

// WithLabelValues works as GetMetricWithLabelValues, but panics where
// GetMetricWithLabelValues would have returned an error. Not returning an
// error allows shortcuts like
//     myVec.WithLabelValues("404", "GET").Observe(42.21)
func (c *HistogramVecProxy) WithLabelValues(lvs ...string) prometheus.Observer {
	if len(lvs) > 0 {
		c.mu.Lock()
		noteLabelsInMetricsProxy(c, lvs)
		c.mu.Unlock()
	}
	return c.HistogramVec.WithLabelValues(lvs...)
}

// With works as GetMetricWith but panics where GetMetricWithLabels would have
// returned an error. Not returning an error allows shortcuts like
//     myVec.With(prometheus.Labels{"code": "404", "method": "GET"}).Observe(42.21)
func (c *HistogramVecProxy) With(labels prometheus.Labels) prometheus.Observer {
	if len(labels) > 0 {
		values := make([]string, len(labels))
		labelNameIndex := c.GetLabelNamesIndex()
		for k, v := range labels {
			values[labelNameIndex[k]] = v
		}
		c.mu.Lock()
		noteLabelsInMetricsProxy(c, values)
		c.mu.Unlock()
	}

	return c.HistogramVec.With(labels)
}

// DeleteAllAboutLabels Remove all labelsValue with these labels
func (c *HistogramVecProxy) DeleteAllAboutLabels(labels prometheus.Labels) bool {
	if len(labels) == 0 {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return findAndDeleteLabelsInMetricsProxy(c, labels)
}

// GetLabelNamesIndex to support get HistogramVecProxy's LabelNames when you use Proxy object
func (c *HistogramVecProxy) GetLabelNamesIndex() map[string]int {
	return c.LabelNamesIndex
}

// GetLabels to support get HistogramVecProxy's Labels when you use Proxy object
func (c *HistogramVecProxy) GetLabels() map[string][]string {
	return c.Labels
}

// vecDelete to support delete HistogramVecProxy's Labels when you use Proxy object
func (c *HistogramVecProxy) vecDelete(labels prometheus.Labels) bool {
	return c.HistogramVec.Delete(labels)
}
