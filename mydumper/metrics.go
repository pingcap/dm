package mydumper

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// should alert
	mydumperExitWithErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "mydumper",
			Name:      "exit_with_error_count",
			Help:      "counter for mydumper exit with error",
		}, []string{"task"})
)

// RegisterMetrics registers metrics.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(mydumperExitWithErrorCounter)
}
