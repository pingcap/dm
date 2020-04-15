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
	"context"
	"net/http"
	"time"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/metricsproxy"

	cpu "github.com/pingcap/tidb-tools/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

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
		}, []string{"task"})

	binlogEventSizeHistogram = metricsproxy.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "binlog_event_size",
			Help:      "size of a binlog event",
			Buckets:   prometheus.ExponentialBuckets(16, 2, 20),
		}, []string{"task"})

	binlogEvent = metricsproxy.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "binlog_transform_cost",
			Help:      "cost of binlog event transform",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"type", "task"})

	conflictDetectDurationHistogram = metricsproxy.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "conflict_detect_duration",
			Help:      "bucketed histogram of conflict detect time (s) for single DML statement",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"task"})

	addJobDurationHistogram = metricsproxy.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "add_job_duration",
			Help:      "bucketed histogram of add a job to the queue time (s)",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"type", "task", "queueNo"})

	// dispatch/add multiple jobs for one binlog event.
	// NOTE: only observe for DML now.
	dispatchBinlogDurationHistogram = metricsproxy.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "dispatch_binlog_duration",
			Help:      "bucketed histogram of dispatch a binlog event time (s)",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"type", "task"})

	skipBinlogDurationHistogram = metricsproxy.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "skip_binlog_duration",
			Help:      "bucketed histogram of skip a binlog event time (s)",
			Buckets:   prometheus.ExponentialBuckets(0.0000005, 2, 25), // this should be very fast.
		}, []string{"type", "task"})

	addedJobsTotal = metricsproxy.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "added_jobs_total",
			Help:      "total number of added jobs",
		}, []string{"type", "task", "queueNo"})

	finishedJobsTotal = metricsproxy.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "finished_jobs_total",
			Help:      "total number of finished jobs",
		}, []string{"type", "task", "queueNo"})

	queueSizeGauge = metricsproxy.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "queue_size",
			Help:      "remain size of the DML queue",
		}, []string{"task", "queueNo"})

	binlogPosGauge = metricsproxy.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "binlog_pos",
			Help:      "current binlog pos",
		}, []string{"node", "task"})

	binlogFileGauge = metricsproxy.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "binlog_file",
			Help:      "current binlog file index",
		}, []string{"node", "task"})

	sqlRetriesTotal = metricsproxy.NewCounterVec(
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

	stmtHistogram = metricsproxy.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "stmt_duration_time",
			Help:      "Bucketed histogram of every statement query time (s).",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		}, []string{"type", "task"})

	// FIXME: should I move it to dm-worker?
	cpuUsageGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "cpu_usage",
			Help:      "the cpu usage of syncer process",
		})

	// should alert
	syncerExitWithErrorCounter = metricsproxy.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "exit_with_error_count",
			Help:      "counter for syncer exits with error",
		}, []string{"task"})

	// some problems with it
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
		}, []string{"task"})

	unsyncedTableGauge = metricsproxy.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "unsynced_table_number",
			Help:      "number of unsynced tables in the subtask",
		}, []string{"task", "table"})

	shardLockResolving = metricsproxy.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "syncer",
			Name:      "shard_lock_resolving",
			Help:      "waiting shard DDL lock to be resolved",
		}, []string{"task"})
)

// RegisterMetrics registers metrics
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(binlogReadDurationHistogram)
	registry.MustRegister(binlogEventSizeHistogram)
	registry.MustRegister(binlogEvent)
	registry.MustRegister(conflictDetectDurationHistogram)
	registry.MustRegister(addJobDurationHistogram)
	registry.MustRegister(dispatchBinlogDurationHistogram)
	registry.MustRegister(skipBinlogDurationHistogram)
	registry.MustRegister(addedJobsTotal)
	registry.MustRegister(finishedJobsTotal)
	registry.MustRegister(queueSizeGauge)
	registry.MustRegister(sqlRetriesTotal)
	registry.MustRegister(binlogPosGauge)
	registry.MustRegister(binlogFileGauge)
	registry.MustRegister(txnHistogram)
	registry.MustRegister(stmtHistogram)
	registry.MustRegister(queryHistogram)
	registry.MustRegister(cpuUsageGauge)
	registry.MustRegister(syncerExitWithErrorCounter)
	registry.MustRegister(replicationLagGauge)
	registry.MustRegister(remainingTimeGauge)
	registry.MustRegister(unsyncedTableGauge)
	registry.MustRegister(shardLockResolving)
}

func (s *Syncer) runBackgroundJob(ctx context.Context) {
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.collectMetrics()

		case <-ctx.Done():
			return
		}
	}
}

// Note: handle error inside the function with returning it.
func (s *Syncer) collectMetrics() {
	// CPU usage metric
	cpuUsage := cpu.GetCPUPercentage()
	cpuUsageGauge.Set(cpuUsage)
}

// InitStatusAndMetrics register prometheus metrics and listen for status port.
func InitStatusAndMetrics(addr string) {

	go func() {
		http.HandleFunc("/status", func(w http.ResponseWriter, req *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			text := utils.GetRawInfo()
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
	binlogEvent.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	conflictDetectDurationHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	addJobDurationHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	dispatchBinlogDurationHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	skipBinlogDurationHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	addedJobsTotal.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	finishedJobsTotal.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	queueSizeGauge.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	sqlRetriesTotal.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	binlogPosGauge.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	binlogFileGauge.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	txnHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	stmtHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	queryHistogram.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	syncerExitWithErrorCounter.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	replicationLagGauge.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	remainingTimeGauge.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	unsyncedTableGauge.DeleteAllAboutLabels(prometheus.Labels{"task": task})
	shardLockResolving.DeleteAllAboutLabels(prometheus.Labels{"task": task})
}
