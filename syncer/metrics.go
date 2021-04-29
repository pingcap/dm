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

package syncer

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/metricsproxy"
	"github.com/pingcap/dm/pkg/utils"
)

var (
	binlogReadDurationHistogram = metricsproxy.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "read_binlog_duration",
			Help:      "bucketed histogram of read time (s) for single binlog event from the relay log or master.",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"task", "source_id"})

	binlogEventSizeHistogram = metricsproxy.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "binlog_event_size",
			Help:      "size of a binlog event",
			Buckets:   prometheus.ExponentialBuckets(16, 2, 20),
		}, []string{"task", "source_id"})

	binlogEventCostHistogram = metricsproxy.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "binlog_transform_cost",
			Help:      "cost of binlog event transform",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"type", "task", "source_id"})

	conflictDetectDurationHistogram = metricsproxy.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "conflict_detect_duration",
			Help:      "bucketed histogram of conflict detect time (s) for single DML statement",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"task", "source_id"})

	addJobDurationHistogram = metricsproxy.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "add_job_duration",
			Help:      "bucketed histogram of add a job to the queue time (s)",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"type", "task", "queueNo", "source_id"})

	// dispatch/add multiple jobs for one binlog event.
	// NOTE: only observe for DML now.
	dispatchBinlogDurationHistogram = metricsproxy.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "dispatch_binlog_duration",
			Help:      "bucketed histogram of dispatch a binlog event time (s)",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"type", "task", "source_id"})

	skipBinlogDurationHistogram = metricsproxy.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "skip_binlog_duration",
			Help:      "bucketed histogram of skip a binlog event time (s)",
			Buckets:   prometheus.ExponentialBuckets(0.0000005, 2, 25), // this should be very fast.
		}, []string{"type", "task", "source_id"})

	addedJobsTotalCounter = metricsproxy.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "added_jobs_total",
			Help:      "total number of added jobs",
		}, []string{"type", "task", "queueNo", "source_id"})

	finishedJobsTotalCounter = metricsproxy.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "finished_jobs_total",
			Help:      "total number of finished jobs",
		}, []string{"type", "task", "queueNo", "source_id"})

	queueSizeGauge = metricsproxy.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "queue_size",
			Help:      "remain size of the DML queue",
		}, []string{"task", "queueNo", "source_id"})

	binlogPosGauge = metricsproxy.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "binlog_pos",
			Help:      "current binlog pos",
		}, []string{"node", "task", "source_id"})

	binlogFileGauge = metricsproxy.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "binlog_file",
			Help:      "current binlog file index",
		}, []string{"node", "task", "source_id"})

	sqlRetriesTotalCounter = metricsproxy.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "sql_retries_total",
			Help:      "total number of sql retries",
		}, []string{"type", "task"})

	txnHistogram = metricsproxy.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "txn_duration_time",
			Help:      "Bucketed histogram of processing time (s) of a txn.",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"task"})

	queryHistogram = metricsproxy.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "query_duration_time",
			Help:      "Bucketed histogram of query time (s).",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"task"})

	// TODO(ehco): refine this metrics in baseConn.ExecuteSQL.
	stmtHistogram = metricsproxy.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "stmt_duration_time",
			Help:      "Bucketed histogram of every statement query time (s).",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"type", "task"})

	// should alert.
	syncerExitWithErrorCounter = metricsproxy.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "exit_with_error_count",
			Help:      "counter for syncer exits with error",
		}, []string{"task", "source_id"})

	// some problems with it.
	replicationLagGauge = metricsproxy.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "replication_lag",
			Help:      "replication lag in second between mysql and syncer",
		}, []string{"task"})

	remainingTimeGauge = metricsproxy.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "remaining_time",
			Help:      "the remaining time in second to catch up master",
		}, []string{"task", "source_id"})

	unsyncedTableGauge = metricsproxy.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "unsynced_table_number",
			Help:      "number of unsynced tables in the subtask",
		}, []string{"task", "table", "source_id"})

	shardLockResolvingGauge = metricsproxy.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "shard_lock_resolving",
			Help:      "waiting shard DDL lock to be resolved",
		}, []string{"task", "source_id"})

	heartbeatUpdateErrCounter = metricsproxy.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "heartbeat_update_error",
			Help:      "number of error when update heartbeat timestamp",
		}, []string{"server_id"})
)

// RegisterMetrics registers metrics.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(binlogReadDurationHistogram)
	registry.MustRegister(binlogEventSizeHistogram)
	registry.MustRegister(binlogEventCostHistogram)
	registry.MustRegister(conflictDetectDurationHistogram)
	registry.MustRegister(addJobDurationHistogram)
	registry.MustRegister(dispatchBinlogDurationHistogram)
	registry.MustRegister(skipBinlogDurationHistogram)
	registry.MustRegister(addedJobsTotalCounter)
	registry.MustRegister(finishedJobsTotalCounter)
	registry.MustRegister(queueSizeGauge)
	registry.MustRegister(binlogPosGauge)
	registry.MustRegister(binlogFileGauge)
	registry.MustRegister(sqlRetriesTotalCounter)
	registry.MustRegister(txnHistogram)
	registry.MustRegister(queryHistogram)
	registry.MustRegister(stmtHistogram)
	registry.MustRegister(syncerExitWithErrorCounter)
	registry.MustRegister(replicationLagGauge)
	registry.MustRegister(remainingTimeGauge)
	registry.MustRegister(unsyncedTableGauge)
	registry.MustRegister(shardLockResolvingGauge)
	registry.MustRegister(heartbeatUpdateErrCounter)
}

// InitStatusAndMetrics register prometheus metrics and listen for status port.
func InitStatusAndMetrics(addr string) {
	go func() {
		http.HandleFunc("/status", func(w http.ResponseWriter, req *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			text := utils.GetRawInfo()
			//nolint:errcheck
			w.Write([]byte(text))
		})

		registry := prometheus.NewRegistry()
		registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
		registry.MustRegister(prometheus.NewGoCollector())
		RegisterMetrics(registry)
		prometheus.DefaultGatherer = registry

		// HTTP path for prometheus.
		http.Handle("/metrics", promhttp.Handler())
		log.L().Info("listening for status and metrics report.", zap.String("address", addr))
		err := http.ListenAndServe(addr, nil)
		if err != nil {
			log.L().Fatal(err.Error())
		}
	}()
}

func (s *Syncer) removeLabelValuesWithTaskInMetrics(task string) {
	binlogReadDurationHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	binlogEventSizeHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	binlogEventCostHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	conflictDetectDurationHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	addJobDurationHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	dispatchBinlogDurationHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	skipBinlogDurationHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	addedJobsTotalCounter.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	finishedJobsTotalCounter.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	queueSizeGauge.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	binlogPosGauge.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	binlogFileGauge.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	sqlRetriesTotalCounter.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	txnHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	queryHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	stmtHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	syncerExitWithErrorCounter.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	replicationLagGauge.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	remainingTimeGauge.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	unsyncedTableGauge.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	shardLockResolvingGauge.DeleteAllAboutLabels(prometheus.Labels{"task": task})
}

// SetBinlogReadDurationHistogram is a setter for binlogReadDurationHistogram.
func SetBinlogReadDurationHistogram(duration float64, labels ...string) {
	binlogReadDurationHistogram.WithLabelValues(labels...).Observe(duration)
}

// SetBinlogEventSizeHistogram is a setter for binlogEventSizeHistogram.
func SetBinlogEventSizeHistogram(size float64, labels ...string) {
	binlogEventSizeHistogram.WithLabelValues(labels...).Observe(size)
}

// SetBinlogEventHistogram is a setter for binlogEventCostHistogram.
func SetBinlogEventHistogram(size float64, labels ...string) {
	binlogEventCostHistogram.WithLabelValues(labels...).Observe(size)
}

// SetConflictDetectDurationHistogram is a setter for conflictDetectDurationHistogram.
func SetConflictDetectDurationHistogram(size float64, labels ...string) {
	conflictDetectDurationHistogram.WithLabelValues(labels...).Observe(size)
}

// SetAddJobDurationHistogram is a setter for addJobDurationHistogram.
func SetAddJobDurationHistogram(size float64, labels ...string) {
	addJobDurationHistogram.WithLabelValues(labels...).Observe(size)
}

// SetDispatchBinlogDurationHistogram is a setter for dispatchBinlogDurationHistogram.
func SetDispatchBinlogDurationHistogram(size float64, labels ...string) {
	dispatchBinlogDurationHistogram.WithLabelValues(labels...).Observe(size)
}

// SetSkipBinlogDurationHistogram is a setter for skipBinlogDurationHistogram.
func SetSkipBinlogDurationHistogram(size float64, labels ...string) {
	skipBinlogDurationHistogram.WithLabelValues(labels...).Observe(size)
}

// IncrAddedJobsTotalCounter is a wrapper for addedJobsTotalCounter.
func IncrAddedJobsTotalCounter(labels ...string) {
	addedJobsTotalCounter.WithLabelValues(labels...).Inc()
}

// IncrFinishedJobsTotalCounter is a wrapper for finishedJobsTotalCounter.
func IncrFinishedJobsTotalCounter(labels ...string) {
	finishedJobsTotalCounter.WithLabelValues(labels...).Inc()
}

// SetQueueSizeGauge is a setter for queueSizeGauge.
func SetQueueSizeGauge(size float64, labels ...string) {
	queueSizeGauge.WithLabelValues(labels...).Set(size)
}

// SetBinlogPosGauge is a setter for binlogPosGauge.
func SetBinlogPosGauge(size float64, labels ...string) {
	binlogPosGauge.WithLabelValues(labels...).Set(size)
}

// SetBinlogFileGauge is a setter for binlogFileGauge.
func SetBinlogFileGauge(size float64, labels ...string) {
	binlogPosGauge.WithLabelValues(labels...).Set(size)
}

// AddSQLRetriesTotalCounter is adder for sqlRetriesTotalCounter.
func AddSQLRetriesTotalCounter(num float64, labels ...string) {
	sqlRetriesTotalCounter.WithLabelValues(labels...).Add(num)
}

// SetTxnHistogram is a setter for txnHistogram.
func SetTxnHistogram(size float64, labels ...string) {
	txnHistogram.WithLabelValues(labels...).Observe(size)
}

// SetQueryHistogram is a setter for queryHistogram.
func SetQueryHistogram(size float64, labels ...string) {
	queryHistogram.WithLabelValues(labels...).Observe(size)
}

// AddSyncerExitWithErrorCounter is adder for syncerExitWithErrorCounter.
func AddSyncerExitWithErrorCounter(num float64, labels ...string) {
	syncerExitWithErrorCounter.WithLabelValues(labels...).Add(num)
}

// SetReplicationLagGauge is a setter for replicationLagGauge.
func SetReplicationLagGauge(lag float64, labels ...string) {
	replicationLagGauge.WithLabelValues(labels...).Set(lag)
}

// SetRemainingTimeGauge is a setter for remainingTimeGauge.
func SetRemainingTimeGauge(val float64, labels ...string) {
	remainingTimeGauge.WithLabelValues(labels...).Set(val)
}

// SetUnsyncedTableGauge is a setter for unsyncedTableGauge.
func SetUnsyncedTableGauge(lag float64, labels ...string) {
	unsyncedTableGauge.WithLabelValues(labels...).Set(lag)
}

// SetShardLockResolvingGauge is a setter for shardLockResolvingGauge.
func SetShardLockResolvingGauge(lag float64, labels ...string) {
	shardLockResolvingGauge.WithLabelValues(labels...).Set(lag)
}

// AddHeartbeatUpdateErrCounter is adder for heartbeatUpdateErrCounter.
func AddHeartbeatUpdateErrCounter(num float64, labels ...string) {
	heartbeatUpdateErrCounter.WithLabelValues(labels...).Add(num)
}
