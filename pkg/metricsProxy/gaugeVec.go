package metricsProxy

import (
	"github.com/prometheus/client_golang/prometheus"
)

type GaugeVecProxy struct {
	LabelNames []string
	Labels     map[string]map[string]string
	*prometheus.GaugeVec
}

func NewGaugeVec(opts prometheus.GaugeOpts, labelNames []string) *GaugeVecProxy {
	return &GaugeVecProxy{
		LabelNames: labelNames,
		Labels:     make(map[string]map[string]string, 0),
		GaugeVec:   prometheus.NewGaugeVec(opts, labelNames),
	}
}

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

func (c *GaugeVecProxy) With(labels prometheus.Labels) prometheus.Gauge {
	if len(labels) > 0 {
		noteLabels(c, labels)
	}

	return c.GaugeVec.With(labels)
}

func (c *GaugeVecProxy) DeleteAllAboutLabels(labels prometheus.Labels) bool {
	if len(labels) == 0 {
		return false
	}

	return findAndDeleteLabels(c, labels)
}

func (c *GaugeVecProxy) GetLabels() map[string]map[string]string {
	return c.Labels
}

func (c *GaugeVecProxy) vecDelete(labels prometheus.Labels) bool {
	return c.GaugeVec.Delete(labels)
}
