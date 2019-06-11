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
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/failpoint"

	"github.com/pingcap/dm/dm/pb"
)

var (
	printStatusInterval = time.Second * 5
)

// Status implements SubTaskUnit.Status
func (l *Loader) Status() interface{} {
	finishedSize := l.finishedDataSize.Get()
	totalSize := l.totalDataSize.Get()
	progress := percent(finishedSize, totalSize)
	s := &pb.LoadStatus{
		FinishedBytes: finishedSize,
		TotalBytes:    totalSize,
		Progress:      progress,
		MetaBinlog:    l.metaBinlog.Get(),
	}
	return s
}

// Error implements SubTaskUnit.Error
func (l *Loader) Error() interface{} {
	return &pb.LoadError{}
}

// PrintStatus prints status like progress percentage.
func (l *Loader) PrintStatus(ctx context.Context) {
	failpoint.Inject("PrintStatusCheckSeconds", func(val failpoint.Value) {
		if seconds, ok := val.(int); ok {
			printStatusInterval = time.Duration(seconds) * time.Second
			l.logger.Info("set loader printStatusInterval", zap.String("feature", "failpoint"), zap.Int("printStatusInterval", seconds))
		}
	})

	ticker := time.NewTicker(printStatusInterval)
	defer ticker.Stop()

	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var done bool
	for {
		select {
		case <-newCtx.Done():
			done = true
		case <-ticker.C:
		}

		finishedSize := l.finishedDataSize.Get()
		totalSize := l.totalDataSize.Get()
		totalFileCount := l.totalFileCount.Get()
		l.logger.Info("progress status of load",
			zap.Int64("finished_bytes", finishedSize),
			zap.Int64("total_bytes", totalSize),
			zap.Int64("total_file_count", totalFileCount),
			zap.String("progress", percent(finishedSize, totalSize)))
		progressGauge.WithLabelValues(l.cfg.Name).Set(float64(finishedSize) / float64(totalSize))
		if done {
			return
		}
	}
}
