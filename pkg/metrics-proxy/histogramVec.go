package metrics_proxy

import (
	"github.com/prometheus/client_golang/prometheus"
)

// HistogramVecProxy to proxy prometheus.HistogramVec
type HistogramVecProxy struct {
	LabelNames []string
	Labels     map[string]map[string]string
	*prometheus.HistogramVec
}

// NewHistogramVec creates a new HistogramVec based on the provided HistogramOpts and
// partitioned by the given label names.
func NewHistogramVec(opts prometheus.HistogramOpts, labelNames []string) *HistogramVecProxy {
	return &HistogramVecProxy{
		LabelNames:   labelNames,
		Labels:       make(map[string]map[string]string, 0),
		HistogramVec: prometheus.NewHistogramVec(opts, labelNames),
	}
}

// WithLabelValues works as GetMetricWithLabelValues, but panics where
// GetMetricWithLabelValues would have returned an error. Not returning an
// error allows shortcuts like
//     myVec.WithLabelValues("404", "GET").Observe(42.21)
func (c *HistogramVecProxy) WithLabelValues(lvs ...string) prometheus.Observer {
	if len(lvs) > 0 {
		labels := make(map[string]string, len(lvs))
		for index, label := range lvs {
			labels[c.LabelNames[index]] = label
		}
		noteLabels(c, labels)
	}
	return c.HistogramVec.WithLabelValues(lvs...)
}

// With works as GetMetricWith but panics where GetMetricWithLabels would have
// returned an error. Not returning an error allows shortcuts like
//     myVec.With(prometheus.Labels{"code": "404", "method": "GET"}).Observe(42.21)
func (c *HistogramVecProxy) With(labels prometheus.Labels) prometheus.Observer {
	if len(labels) > 0 {
		noteLabels(c, labels)
	}

	return c.HistogramVec.With(labels)
}

// Remove all labelsValue with these labels
func (c *HistogramVecProxy) DeleteAllAboutLabels(labels prometheus.Labels) bool {
	if len(labels) == 0 {
		return false
	}

	return findAndDeleteLabels(c, labels)
}

// HistogramVecProxy to support get HistogramVecProxy's Labels when you use Proxy object
func (c *HistogramVecProxy) GetLabels() map[string]map[string]string {
	return c.Labels
}

// HistogramVecProxy to support delete HistogramVecProxy's Labels when you use Proxy object
func (c *HistogramVecProxy) vecDelete(labels prometheus.Labels) bool {
	return c.HistogramVec.Delete(labels)
}
