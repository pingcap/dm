package metrics_proxy

import (
	"github.com/prometheus/client_golang/prometheus"
)

// GaugeVecProxy to proxy prometheus.GaugeVec
type GaugeVecProxy struct {
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
		noteLabels(c, labels)
	}
	return c.GaugeVec.WithLabelValues(lvs...)
}

// With works as GetMetricWith, but panics where GetMetricWithLabels would have
// returned an error. Not returning an error allows shortcuts like
//     myVec.With(prometheus.Labels{"code": "404", "method": "GET"}).Add(42)
func (c *GaugeVecProxy) With(labels prometheus.Labels) prometheus.Gauge {
	if len(labels) > 0 {
		noteLabels(c, labels)
	}

	return c.GaugeVec.With(labels)
}

// Remove all labelsValue with these labels
func (c *GaugeVecProxy) DeleteAllAboutLabels(labels prometheus.Labels) bool {
	if len(labels) == 0 {
		return false
	}

	return findAndDeleteLabels(c, labels)
}

// GaugeVecProxy to support get GaugeVecProxy's Labels when you use Proxy object
func (c *GaugeVecProxy) GetLabels() map[string]map[string]string {
	return c.Labels
}

// GaugeVecProxy to support delete GaugeVecProxy's Labels when you use Proxy object
func (c *GaugeVecProxy) vecDelete(labels prometheus.Labels) bool {
	return c.GaugeVec.Delete(labels)
}
