package metricsproxy

import (
	"math/rand"
	"time"

	. "github.com/pingcap/check"

	"github.com/prometheus/client_golang/prometheus"
)

func (t *testMetricsProxySuite) TestHistogramVecProxy(c *C) {
	rand.Seed(time.Now().UnixNano())
	for _, oneCase := range testCases {
		histogram := NewHistogramVec(prometheus.HistogramOpts{
			Namespace:   "dm",
			Subsystem:   "metricsProxy",
			Name:        "Test_Histogram",
			Help:        "dm histogram metrics proxy test",
			ConstLabels: nil,
		}, oneCase.LabelsNames)
		for _, aArgs := range oneCase.AddArgs {
			if rand.Intn(199)%2 == 0 {
				histogram.WithLabelValues(aArgs...).Observe(float64(rand.Intn(199)))
			} else {
				labels := make(prometheus.Labels, 0)
				for k, labelName := range oneCase.LabelsNames {
					labels[labelName] = aArgs[k]
				}
				histogram.With(labels)
			}
		}
		for _, dArgs := range oneCase.DeleteArgs {
			histogram.DeleteAllAboutLabels(dArgs)
		}

		cOutput := make(chan prometheus.Metric, len(oneCase.AddArgs)*3)

		histogram.Collect(cOutput)

		c.Assert(len(cOutput), Equals, oneCase.WantResLength)
	}
}
