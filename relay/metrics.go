package relay

import (
	"reflect"
	"time"

	"github.com/ngaut/log"

	"github.com/juju/errors"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sys/unix"
)

var (
	relayLogPosGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "relay",
			Name:      "binlog_pos",
			Help:      "current binlog pos in current binlog file",
		})

	relayLogFileGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "relay",
			Name:      "binlog_file",
			Help:      "current binlog file index",
		})

	relayLogSpaceGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "relay",
			Name:      "space",
			Help:      "the used or free space of relay log",
		}, []string{"type"}) // type can be 'capacity' and 'available'.

	// should alert
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
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 18),
		})

	relayLogWriteErrorCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "relay",
			Name:      "write_error_count",
			Help:      "write relay log error count",
		})

	// should alert
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
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 18),
		})

	// should alert
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
	registry.MustRegister(relayLogSpaceGauge)
	registry.MustRegister(relayLogDataCorruptionCounter)
	registry.MustRegister(relayLogWriteSizeHistogram)
	registry.MustRegister(relayLogWriteDurationHistogram)
	registry.MustRegister(relayLogWriteErrorCounter)
	registry.MustRegister(binlogReadErrorCounter)
	registry.MustRegister(binlogReadDurationHistogram)
	registry.MustRegister(relayExitWithErrorCounter)
}

func reportRelayLogSpaceInBackground(dirpath string) error {
	if len(dirpath) == 0 {
		return errors.New("dirpath is empty")
	}

	go func() {
		ticker := time.NewTicker(time.Second * 10)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				size, err := getStorageSize(dirpath)
				if err != nil {
					log.Error("update sotrage size err: ", err)
				} else {
					relayLogSpaceGauge.WithLabelValues("capacity").Set(float64(size.capacity))
					relayLogSpaceGauge.WithLabelValues("available").Set(float64(size.available))
				}
			}
		}
	}()

	return nil
}

// Learn from tidb-binlog source code.
type storageSize struct {
	capacity  int
	available int
}

func getStorageSize(dir string) (size storageSize, err error) {
	var stat unix.Statfs_t

	err = unix.Statfs(dir, &stat)
	if err != nil {
		return size, errors.Trace(err)
	}

	// When container is run in MacOS, `bsize` obtained by `statfs` syscall is not the fundamental block size,
	// but the `iosize` (optimal transfer block size) instead, it's usually 1024 times larger than the `bsize`.
	// for example `4096 * 1024`. To get the correct block size, we should use `frsize`. But `frsize` isn't
	// guaranteed to be supported everywhere, so we need to check whether it's supported before use it.
	// For more details, please refer to: https://github.com/docker/for-mac/issues/2136
	bSize := uint64(stat.Bsize)
	field := reflect.ValueOf(&stat).Elem().FieldByName("Frsize")
	if field.IsValid() {
		if field.Kind() == reflect.Uint64 {
			bSize = field.Uint()
		} else {
			bSize = uint64(field.Int())
		}
	}

	// Available blocks * size per block = available space in bytes
	size.available = int(stat.Bavail) * int(bSize)
	size.capacity = int(stat.Blocks) * int(bSize)

	return
}
