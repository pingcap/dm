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

package relay

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/metricsproxy"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

var (
	relayLogPosGauge = metricsproxy.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "relay",
			Name:      "binlog_pos",
			Help:      "current binlog pos in current binlog file",
		}, []string{"node"})

	relayLogFileGauge = metricsproxy.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "relay",
			Name:      "binlog_file",
			Help:      "current binlog file index",
		}, []string{"node"})

	// split sub directory info from relayLogPosGauge / relayLogFileGauge
	// to make compare relayLogFileGauge for master / relay more easier.
	relaySubDirIndex = metricsproxy.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "relay",
			Name:      "sub_dir_index",
			Help:      "current relay sub directory index",
		}, []string{"node", "uuid"})

	// should alert if available space < 10G.
	relayLogSpaceGauge = metricsproxy.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "relay",
			Name:      "space",
			Help:      "the space of storage for relay component",
		}, []string{"type"}) // type can be 'capacity' and 'available'.

	// should alert.
	relayLogDataCorruptionCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "relay",
			Name:      "data_corruption",
			Help:      "counter of relay log data corruption",
		})

	relayLogWriteSizeHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "relay",
			Name:      "write_size",
			Help:      "write relay log size",
			Buckets:   prometheus.ExponentialBuckets(16, 2, 20),
		})

	relayLogWriteDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "relay",
			Name:      "write_duration",
			Help:      "bucketed histogram of write time (s) of single relay log event",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		})

	// should alert.
	relayLogWriteErrorCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "relay",
			Name:      "write_error_count",
			Help:      "write relay log error count",
		})

	// should alert.
	binlogReadErrorCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "relay",
			Name:      "read_error_count",
			Help:      "read binlog from master error count",
		})

	binlogReadDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "relay",
			Name:      "read_binlog_duration",
			Help:      "bucketed histogram of read time (s) of single binlog event from the master.",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		})

	binlogTransformDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "relay",
			Name:      "read_transform_duration",
			Help:      "bucketed histogram of transform time (s) of single binlog event.",
			Buckets:   prometheus.ExponentialBuckets(0.000005, 2, 25),
		})

	// should alert.
	relayExitWithErrorCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "relay",
			Name:      "exit_with_error_count",
			Help:      "counter of relay unit exits with error",
		})
)

// RegisterMetrics register metrics.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(relayLogPosGauge)
	registry.MustRegister(relayLogFileGauge)
	registry.MustRegister(relaySubDirIndex)
	registry.MustRegister(relayLogSpaceGauge)
	registry.MustRegister(relayLogDataCorruptionCounter)
	registry.MustRegister(relayLogWriteSizeHistogram)
	registry.MustRegister(relayLogWriteDurationHistogram)
	registry.MustRegister(relayLogWriteErrorCounter)
	registry.MustRegister(binlogReadErrorCounter)
	registry.MustRegister(binlogReadDurationHistogram)
	registry.MustRegister(binlogTransformDurationHistogram)
	registry.MustRegister(relayExitWithErrorCounter)
}

func reportRelayLogSpaceInBackground(ctx context.Context, dirpath string) error {
	if len(dirpath) == 0 {
		return terror.ErrRelayLogDirpathEmpty.Generate()
	}

	go func() {
		ticker := time.NewTicker(time.Second * 10)
		defer ticker.Stop()

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			size, err := utils.GetStorageSize(dirpath)
			if err != nil {
				log.L().Error("fail to update relay log storage size", log.ShortError(err))
			} else {
				SetRelayLogSpaceGauge(float64(size.Capacity), "capacity")
				SetRelayLogSpaceGauge(float64(size.Available), "available")
			}
		}
	}()

	return nil
}

// SetRelayLogPosGauge is a setter for relayLogPosGauge.
func SetRelayLogPosGauge(name string, pos float64) {
	relayLogPosGauge.WithLabelValues(name).Set(pos)
}

// SetRelayLogFileGauge is a setter for relayLogFileGauge.
func SetRelayLogFileGauge(name string, index float64) {
	relayLogFileGauge.WithLabelValues(name).Set(index)
}

// SetRelaySubDirIndex is a setter for relaySubDirIndex.
func SetRelaySubDirIndex(suffix float64, labels ...string) {
	relaySubDirIndex.WithLabelValues(labels...).Set(suffix)
}

// SetRelayLogSpaceGauge is a setter for relayLogSpaceGauge.
func SetRelayLogSpaceGauge(space float64, labels ...string) {
	relayLogSpaceGauge.WithLabelValues(labels...).Set(space)
}

// IncrRelayLogDataCorruptionCounter is a wrapper for relayLogDataCorruptionCounter.
func IncrRelayLogDataCorruptionCounter() {
	relayLogDataCorruptionCounter.Inc()
}

// SetRelayLogWriteSizeHistogram is a setter for relayLogWriteSizeHistogram.
func SetRelayLogWriteSizeHistogram(size float64) {
	relayLogWriteSizeHistogram.Observe(size)
}

// SetRelayLogWriteDurationHistogram is a setter for relayLogWriteDurationHistogram.
func SetRelayLogWriteDurationHistogram(duration float64) {
	relayLogWriteDurationHistogram.Observe(duration)
}

// IncrRelayLogWriteErrorCounter is a wrapper for relayLogWriteErrorCounter.
func IncrRelayLogWriteErrorCounter() {
	relayLogWriteErrorCounter.Inc()
}

// IncrBinlogReadErrorCounter is a wrapper for binlogReadErrorCounter.
func IncrBinlogReadErrorCounter() {
	binlogReadErrorCounter.Inc()
}

// SetBinlogReadDurationHistogram is a setter for binlogReadDurationHistogram.
func SetBinlogReadDurationHistogram(duration float64) {
	binlogReadDurationHistogram.Observe(duration)
}

// SetBinlogTransformDurationHistogram is a setter for binlogTransformDurationHistogram.
func SetBinlogTransformDurationHistogram(duration float64) {
	binlogTransformDurationHistogram.Observe(duration)
}

// IncrRelayExitWithErrorCounter is a wrapper for relayExitWithErrorCounter.
func IncrRelayExitWithErrorCounter() {
	relayExitWithErrorCounter.Inc()
}
