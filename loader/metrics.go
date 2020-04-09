// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

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
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 18),
		}, []string{"task"})

	txnHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "loader",
			Name:      "txn_duration_time",
			Help:      "Bucketed histogram of processing time (s) of a txn.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 18),
		}, []string{"task"})

	stmtHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "loader",
			Name:      "stmt_duration_time",
			Help:      "Bucketed histogram of every statement query time (s).",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 18),
		}, []string{"type", "task"})

	dataFileGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "loader",
			Name:      "data_file_gauge",
			Help:      "data files in total",
		}, []string{"task"})

	tableGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "loader",
			Name:      "table_gauge",
			Help:      "tables in total",
		}, []string{"task"})

	dataSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "loader",
			Name:      "data_size_gauge",
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
	registry.MustRegister(stmtHistogram)
	registry.MustRegister(dataFileGauge)
	registry.MustRegister(tableGauge)
	registry.MustRegister(dataSizeGauge)
	registry.MustRegister(progressGauge)
	registry.MustRegister(loaderExitWithErrorCounter)
}
