package loader

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// should error
	tidbExecutionErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "loader",
			Name:      "tidb_execution_error",
			Help:      "Total count of tidb execution errors",
		}, []string{"task"})

	queryHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "loader",
			Name:      "query_duration_time",
			Help:      "Bucketed histogram of query time (s) of a txn.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 16),
		}, []string{"task"})

	txnHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "loader",
			Name:      "txn_duration_time",
			Help:      "Bucketed histogram of processing time (s) of a txn.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 16),
		}, []string{"task"})

	dataFileCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "loader",
			Name:      "data_file_count",
			Help:      "data files in total",
		}, []string{"task"})

	tableCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "loader",
			Name:      "table_count",
			Help:      "tables in total",
		}, []string{"task"})

	dataSizeCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "loader",
			Name:      "data_size_count",
			Help:      "data size in total",
		}, []string{"task"})

	progressGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "loader",
			Name:      "progress",
			Help:      "the processing progress of loader in percentage",
		}, []string{"task"})

	// should alert
	loaderExitWithErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "loader",
			Name:      "exit_with_error_count",
			Help:      "counter for loader exits with error",
		}, []string{"task"})
)

// RegisterMetrics registers metrics
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(tidbExecutionErrorCounter)
	registry.MustRegister(txnHistogram)
	registry.MustRegister(queryHistogram)
	registry.MustRegister(dataFileCounter)
	registry.MustRegister(tableCounter)
	registry.MustRegister(dataSizeCounter)
	registry.MustRegister(progressGauge)
	registry.MustRegister(loaderExitWithErrorCounter)
}
