package metricsProxy

import (
	"github.com/prometheus/client_golang/prometheus"
)

type SummaryVecProxy struct {
	LabelNames []string
	Labels     map[string]map[string]string
	*prometheus.SummaryVec
}

func NewSummaryVec(opts prometheus.SummaryOpts, labelNames []string) *SummaryVecProxy {
	return &SummaryVecProxy{
		LabelNames: labelNames,
		Labels:     make(map[string]map[string]string, 0),
		SummaryVec: prometheus.NewSummaryVec(opts, labelNames),
	}
}

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

func (c *SummaryVecProxy) With(labels prometheus.Labels) prometheus.Observer {
	if len(labels) > 0 {
		noteLabels(c, labels)
	}

	return c.SummaryVec.With(labels)
}

func (c *SummaryVecProxy) DeleteAllAboutLabels(labels prometheus.Labels) bool {
	if len(labels) == 0 {
		return false
	}

	return findAndDeleteLabels(c, labels)
}

func (c *SummaryVecProxy) GetLabels() map[string]map[string]string {
	return c.Labels
}

func (c *SummaryVecProxy) vecDelete(labels prometheus.Labels) bool {
	return c.SummaryVec.Delete(labels)
}
