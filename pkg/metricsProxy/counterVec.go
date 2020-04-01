package metricsProxy

import (
	"github.com/prometheus/client_golang/prometheus"
)

type CounterVecProxy struct {
	LabelNames []string
	Labels     map[string]map[string]string
	*prometheus.CounterVec
}

func NewCounterVec(opts prometheus.CounterOpts, labelNames []string) *CounterVecProxy {
	return &CounterVecProxy{
		LabelNames: labelNames,
		Labels:     make(map[string]map[string]string, 0),
		CounterVec: prometheus.NewCounterVec(opts, labelNames),
	}
}

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

func (c *CounterVecProxy) With(labels prometheus.Labels) prometheus.Counter {
	if len(labels) > 0 {
		noteLabels(c, labels)
	}

	return c.CounterVec.With(labels)
}

func (c *CounterVecProxy) DeleteAllAboutLabels(labels prometheus.Labels) bool {
	if len(labels) == 0 {
		return false
	}

	return findAndDeleteLabels(c, labels)
}

func (c *CounterVecProxy) GetLabels() map[string]map[string]string {
	return c.Labels
}

func (c *CounterVecProxy) vecDelete(labels prometheus.Labels) bool {
	return c.CounterVec.Delete(labels)
}
