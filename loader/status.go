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

	"github.com/pingcap/failpoint"

	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/log"
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
			log.Infof("[failpoint] set loader printStatusInterval to %d", seconds)
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
		log.Infof("[loader] finished_bytes = %d, total_bytes = %d, total_file_count = %d, progress = %s", finishedSize, totalSize, totalFileCount, percent(finishedSize, totalSize))
		progressGauge.WithLabelValues(l.cfg.Name).Set(float64(finishedSize) / float64(totalSize))
		if done {
			return
		}
	}
}
