package metricsproxy

import (
	"math/rand"

	. "github.com/pingcap/check"

	"github.com/prometheus/client_golang/prometheus"
)

func (t *testMetricsProxySuite) TestCounterVecProxy(c *C) {
	for _, oneCase := range testCases {
		counter := NewCounterVec(prometheus.CounterOpts{
			Namespace:   "dm",
			Subsystem:   "metricsProxy",
			Name:        "Test_Counter",
			Help:        "dm counter metrics proxy test",
			ConstLabels: nil,
		}, oneCase.LabelsNames)
		for _, aArgs := range oneCase.AddArgs {
			if rand.Intn(199)%2 == 0 {
				counter.WithLabelValues(aArgs...).Add(float64(rand.Intn(199)))
			} else {
				labels := make(prometheus.Labels, 0)
				for k, labelName := range oneCase.LabelsNames {
					labels[labelName] = aArgs[k]
				}
				counter.With(labels)
			}
		}
		for _, dArgs := range oneCase.DeleteArgs {
			counter.DeleteAllAboutLabels(dArgs)
		}

		cOutput := make(chan prometheus.Metric, len(oneCase.AddArgs)*3)

		counter.Collect(cOutput)

		c.Assert(len(cOutput), Equals, oneCase.WantResLength)
	}
}
