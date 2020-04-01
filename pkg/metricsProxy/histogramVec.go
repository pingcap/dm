package metricsProxy

import (
	"github.com/prometheus/client_golang/prometheus"
)

type HistogramVecProxy struct {
	LabelNames []string
	Labels     map[string]map[string]string
	*prometheus.HistogramVec
}

func NewHistogramVec(opts prometheus.HistogramOpts, labelNames []string) *HistogramVecProxy {
	return &HistogramVecProxy{
		LabelNames:   labelNames,
		Labels:       make(map[string]map[string]string, 0),
		HistogramVec: prometheus.NewHistogramVec(opts, labelNames),
	}
}

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

func (c *HistogramVecProxy) With(labels prometheus.Labels) prometheus.Observer {
	if len(labels) > 0 {
		noteLabels(c, labels)
	}

	return c.HistogramVec.With(labels)
}

func (c *HistogramVecProxy) DeleteAllAboutLabels(labels prometheus.Labels) bool {
	if len(labels) == 0 {
		return false
	}

	return findAndDeleteLabels(c, labels)
}

func (c *HistogramVecProxy) GetLabels() map[string]map[string]string {
	return c.Labels
}

func (c *HistogramVecProxy) vecDelete(labels prometheus.Labels) bool {
	return c.HistogramVec.Delete(labels)
}
