package metricsProxy

import (
	"github.com/prometheus/client_golang/prometheus"
)

type CounterVecProxy struct {
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
		noteLabels(c, labels)
	}
	return c.CounterVec.WithLabelValues(lvs...)
}

// With works as GetMetricWith, but panics where GetMetricWithLabels would have
// returned an error. Not returning an error allows shortcuts like
//     myVec.With(prometheus.Labels{"code": "404", "method": "GET"}).Add(42)
func (c *CounterVecProxy) With(labels prometheus.Labels) prometheus.Counter {
	if len(labels) > 0 {
		noteLabels(c, labels)
	}

	return c.CounterVec.With(labels)
}

// Remove all labelsValue with these labels
func (c *CounterVecProxy) DeleteAllAboutLabels(labels prometheus.Labels) bool {
	if len(labels) == 0 {
		return false
	}

	return findAndDeleteLabels(c, labels)
}

// to support get CounterVecProxy's Labels when you use Proxy object
func (c *CounterVecProxy) GetLabels() map[string]map[string]string {
	return c.Labels
}

// to support delete CounterVecProxy's Labels when you use Proxy object
func (c *CounterVecProxy) vecDelete(labels prometheus.Labels) bool {
	return c.CounterVec.Delete(labels)
}
