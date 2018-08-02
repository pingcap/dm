package loader

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	tidbUnknownErrorCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "loader",
			Name:      "tidb_unknown_error",
			Help:      "Total count of tidb unknown errors",
		})

	txnHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "loader",
			Name:      "txn_duration_time",
			Help:      "Bucketed histogram of processing time (s) of a txn.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		})

	dataFileCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "loader",
			Name:      "data_file_count",
			Help:      "data files in total",
		})

	databaseCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "loader",
			Name:      "database_count",
			Help:      "databases in total",
		})

	tableCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "loader",
			Name:      "table_count",
			Help:      "tables in total",
		})

	dataSizeCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "loader",
			Name:      "data_size_count",
			Help:      "data size in total",
		})

	progressGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "loader",
			Name:      "progress",
			Help:      "the processing progress of loader in percentage",
		})
)

func init() {
	prometheus.MustRegister(tidbUnknownErrorCount)
	prometheus.MustRegister(txnHistogram)
	prometheus.MustRegister(dataFileCount)
	prometheus.MustRegister(databaseCount)
	prometheus.MustRegister(tableCount)
	prometheus.MustRegister(dataSizeCount)
	prometheus.MustRegister(progressGauge)
}
