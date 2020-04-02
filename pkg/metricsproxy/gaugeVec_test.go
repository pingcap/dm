package metricsproxy

import (
	"math/rand"

	. "github.com/pingcap/check"

	"github.com/prometheus/client_golang/prometheus"
)

func (t *testMetricsProxySuite) TestGaugeVecProxy(c *C) {
	for _, oneCase := range testCases {
		gauge := NewGaugeVec(prometheus.GaugeOpts{
			Namespace:   "dm",
			Subsystem:   "metricsProxy",
			Name:        "Test_Gauge",
			Help:        "dm gauge metrics proxy test",
			ConstLabels: nil,
		}, oneCase.LabelsNames)
		for _, aArgs := range oneCase.AddArgs {
			if rand.Intn(199)%2 == 0 {
				gauge.WithLabelValues(aArgs...).Add(float64(rand.Intn(199)))
			} else {
				labels := make(prometheus.Labels, 0)
				for k, labelName := range oneCase.LabelsNames {
					labels[labelName] = aArgs[k]
				}
				gauge.With(labels)
			}
		}
		for _, dArgs := range oneCase.DeleteArgs {
			gauge.DeleteAllAboutLabels(dArgs)
		}

		cOutput := make(chan prometheus.Metric, len(oneCase.AddArgs)*3)

		gauge.Collect(cOutput)

		c.Assert(len(cOutput), Equals, oneCase.WantResLength)
	}
}
