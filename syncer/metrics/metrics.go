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

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/pingcap/dm/pkg/metricsproxy"
)

// for BinlogEventCost metric stage field.
const (
	BinlogEventCostStageDDLExec = "ddl-exec"
	BinlogEventCostStageDMLExec = "dml-exec"

	BinlogEventCostStageGenWriteRows  = "gen-write-rows"
	BinlogEventCostStageGenUpdateRows = "gen-update-rows"
	BinlogEventCostStageGenDeleteRows = "gen-delete-rows"
	BinlogEventCostStageGenQuery      = "gen-query"
)

// below variables are exported to syncer package.
var (
	BinlogReadDurationHistogram = metricsproxy.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "read_binlog_duration",
			Help:      "bucketed histogram of read time (s) for single binlog event from the relay log or master.",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"task", "source_id"})

	BinlogEventSizeHistogram = metricsproxy.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "binlog_event_size",
			Help:      "size of a binlog event",
			Buckets:   prometheus.ExponentialBuckets(16, 2, 20),
		}, []string{"task", "worker", "source_id"})

	BinlogEventCost = metricsproxy.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "binlog_transform_cost",
			Help:      "cost of binlog event transform",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"stage", "task", "worker", "source_id"})

	ConflictDetectDurationHistogram = metricsproxy.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "conflict_detect_duration",
			Help:      "bucketed histogram of conflict detect time (s) for single DML statement",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"task", "source_id"})

	AddJobDurationHistogram = metricsproxy.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "add_job_duration",
			Help:      "bucketed histogram of add a job to the queue time (s)",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"type", "task", "queueNo", "source_id"})

	// dispatch/add multiple jobs for one binlog event.
	// NOTE: only observe for DML now.
	DispatchBinlogDurationHistogram = metricsproxy.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "dispatch_binlog_duration",
			Help:      "bucketed histogram of dispatch a binlog event time (s)",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"type", "task", "source_id"})

	SkipBinlogDurationHistogram = metricsproxy.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "skip_binlog_duration",
			Help:      "bucketed histogram of skip a binlog event time (s)",
			Buckets:   prometheus.ExponentialBuckets(0.0000005, 2, 25), // this should be very fast.
		}, []string{"type", "task", "source_id"})

	AddedJobsTotal = metricsproxy.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "added_jobs_total",
			Help:      "total number of added jobs",
		}, []string{"type", "task", "queueNo", "source_id", "worker", "target_schema", "target_table"})

	FinishedJobsTotal = metricsproxy.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "finished_jobs_total",
			Help:      "total number of finished jobs",
		}, []string{"type", "task", "queueNo", "source_id", "worker", "target_schema", "target_table"})

	IdealQPS = metricsproxy.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "ideal_qps",
			Help:      "remain size of the DML queue",
		}, []string{"task", "worker", "source_id"})

	QueueSizeGauge = metricsproxy.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "queue_size",
			Help:      "remain size of the DML queue",
		}, []string{"task", "queueNo", "source_id"})

	BinlogPosGauge = metricsproxy.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "binlog_pos",
			Help:      "current binlog pos",
		}, []string{"node", "task", "source_id"})

	BinlogFileGauge = metricsproxy.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "binlog_file",
			Help:      "current binlog file index",
		}, []string{"node", "task", "source_id"})

	BinlogEventRowGauge = metricsproxy.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "binlog_event_row",
			Help:      "current bin log event row number",
		}, []string{"worker", "task", "source_id"})

	SQLRetriesTotal = metricsproxy.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "sql_retries_total",
			Help:      "total number of sql retries",
		}, []string{"type", "task"})

	TxnHistogram = metricsproxy.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "txn_duration_time",
			Help:      "Bucketed histogram of processing time (s) of a txn.",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"task", "worker", "source_id"})

	QueryHistogram = metricsproxy.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "query_duration_time",
			Help:      "Bucketed histogram of query time (s).",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"task", "worker", "source_id"})

	StmtHistogram = metricsproxy.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "stmt_duration_time",
			Help:      "Bucketed histogram of every statement query time (s).",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"type", "task"})

	// should alert.
	SyncerExitWithErrorCounter = metricsproxy.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "exit_with_error_count",
			Help:      "counter for syncer exits with error",
		}, []string{"task", "source_id"})

	ReplicationLagGauge = metricsproxy.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "replication_lag",
			Help:      "replication lag in second between mysql and syncer",
		}, []string{"task", "source_id", "worker"})

	RemainingTimeGauge = metricsproxy.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "remaining_time",
			Help:      "the remaining time in second to catch up master",
		}, []string{"task", "source_id", "worker"})

	UnsyncedTableGauge = metricsproxy.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "unsynced_table_number",
			Help:      "number of unsynced tables in the subtask",
		}, []string{"task", "table", "source_id"})

	ShardLockResolving = metricsproxy.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "shard_lock_resolving",
			Help:      "waiting shard DDL lock to be resolved",
		}, []string{"task", "source_id"})

	HeartbeatUpdateErr = metricsproxy.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "heartbeat_update_error",
			Help:      "number of error when update heartbeat timestamp",
		}, []string{"server_id"})

	FinishedTransactionTotal = metricsproxy.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "finished_transaction_total",
			Help:      "total number of finished transaction",
		}, []string{"task", "worker", "source_id"})

	ReplicationTransactionBatch = metricsproxy.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "replication_transaction_batch",
			Help:      "number of sql's contained in a transaction that executed to downstream",
		}, []string{"worker", "task", "source_id", "queueNo"})

	FlushCheckPointsTotal = metricsproxy.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "flush_checkpoints_count",
			Help:      "total number of checkpoint flushed",
		}, []string{"worker", "task", "source_id"})
)

// RegisterMetrics registers metrics.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(BinlogReadDurationHistogram)
	registry.MustRegister(BinlogEventSizeHistogram)
	registry.MustRegister(BinlogEventCost)
	registry.MustRegister(BinlogEventRowGauge)
	registry.MustRegister(ConflictDetectDurationHistogram)
	registry.MustRegister(AddJobDurationHistogram)
	registry.MustRegister(DispatchBinlogDurationHistogram)
	registry.MustRegister(SkipBinlogDurationHistogram)
	registry.MustRegister(AddedJobsTotal)
	registry.MustRegister(FinishedJobsTotal)
	registry.MustRegister(QueueSizeGauge)
	registry.MustRegister(SQLRetriesTotal)
	registry.MustRegister(BinlogPosGauge)
	registry.MustRegister(BinlogFileGauge)
	registry.MustRegister(TxnHistogram)
	registry.MustRegister(StmtHistogram)
	registry.MustRegister(QueryHistogram)
	registry.MustRegister(SyncerExitWithErrorCounter)
	registry.MustRegister(ReplicationLagGauge)
	registry.MustRegister(RemainingTimeGauge)
	registry.MustRegister(UnsyncedTableGauge)
	registry.MustRegister(ShardLockResolving)
	registry.MustRegister(HeartbeatUpdateErr)

	registry.MustRegister(IdealQPS)
	registry.MustRegister(FinishedTransactionTotal)
	registry.MustRegister(ReplicationTransactionBatch)
	registry.MustRegister(FlushCheckPointsTotal)
}

// RemoveLabelValuesWithTaskInMetrics cleans metrics.
func RemoveLabelValuesWithTaskInMetrics(task string) {
	BinlogReadDurationHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	BinlogEventSizeHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	BinlogEventCost.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	BinlogEventRowGauge.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	ConflictDetectDurationHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	AddJobDurationHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	DispatchBinlogDurationHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	SkipBinlogDurationHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	AddedJobsTotal.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	FinishedJobsTotal.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	QueueSizeGauge.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	SQLRetriesTotal.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	BinlogPosGauge.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	BinlogFileGauge.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	TxnHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	StmtHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	QueryHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	SyncerExitWithErrorCounter.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	ReplicationLagGauge.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	RemainingTimeGauge.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	UnsyncedTableGauge.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	ShardLockResolving.DeleteAllAboutLabels(prometheus.Labels{"task": task})

	IdealQPS.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	FinishedTransactionTotal.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	ReplicationTransactionBatch.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	FlushCheckPointsTotal.DeleteAllAboutLabels(prometheus.Labels{"task": task})
}
