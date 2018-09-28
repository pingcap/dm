package loader

import (
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb-enterprise-tools/dm/pb"
	"golang.org/x/net/context"
)

const (
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
	}
	return s
}

// PrintStatus prints status like progress percentage.
func (l *Loader) PrintStatus(ctx context.Context) {
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
		log.Infof("[loader] finished_bytes = %d, total_bytes = GetAllRestoringFiles%d, progress = %s", finishedSize, totalSize, percent(finishedSize, totalSize))
		progressGauge.WithLabelValues(l.cfg.Name).Set(float64(finishedSize) / float64(totalSize))
		if done {
			return
		}
	}
}
