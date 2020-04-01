package metricsProxy

import (
	"github.com/prometheus/client_golang/prometheus"
)

type SummaryVecProxy struct {
	LabelNames []string
	Labels     map[string]map[string]string
	*prometheus.SummaryVec
}

// NewSummaryVec creates a new SummaryVec based on the provided SummaryOpts and
// partitioned by the given label names.
//
// Due to the way a Summary is represented in the Prometheus text format and how
// it is handled by the Prometheus server internally, “quantile” is an illegal
// label name. NewSummaryVec will panic if this label name is used.
func NewSummaryVec(opts prometheus.SummaryOpts, labelNames []string) *SummaryVecProxy {
	return &SummaryVecProxy{
		LabelNames: labelNames,
		Labels:     make(map[string]map[string]string, 0),
		SummaryVec: prometheus.NewSummaryVec(opts, labelNames),
	}
}

// WithLabelValues works as GetMetricWithLabelValues, but panics where
// GetMetricWithLabelValues would have returned an error. Not returning an
// error allows shortcuts like
//     myVec.WithLabelValues("404", "GET").Observe(42.21)
func (c *SummaryVecProxy) WithLabelValues(lvs ...string) prometheus.Observer {
	if len(lvs) > 0 {
		labels := make(map[string]string, len(lvs))
		for index, label := range lvs {
			labels[c.LabelNames[index]] = label
		}
		noteLabels(c, labels)
	}
	return c.SummaryVec.WithLabelValues(lvs...)
}

// With works as GetMetricWith, but panics where GetMetricWithLabels would have
// returned an error. Not returning an error allows shortcuts like
//     myVec.With(prometheus.Labels{"code": "404", "method": "GET"}).Observe(42.21)
func (c *SummaryVecProxy) With(labels prometheus.Labels) prometheus.Observer {
	if len(labels) > 0 {
		noteLabels(c, labels)
	}

	return c.SummaryVec.With(labels)
}

// Remove all labelsValue with these labels
func (c *SummaryVecProxy) DeleteAllAboutLabels(labels prometheus.Labels) bool {
	if len(labels) == 0 {
		return false
	}

	return findAndDeleteLabels(c, labels)
}

// to support get SummaryVecProxy's Labels when you use Proxy object
func (c *SummaryVecProxy) GetLabels() map[string]map[string]string {
	return c.Labels
}

// to support delete SummaryVecProxy's Labels when you use Proxy object
func (c *SummaryVecProxy) vecDelete(labels prometheus.Labels) bool {
	return c.SummaryVec.Delete(labels)
}
