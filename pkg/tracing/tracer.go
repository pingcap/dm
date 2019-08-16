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

package tracing

import (
	"context"
	"sync"
	"time"

	"github.com/siddontang/go/sync2"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
)

var (
	uploadInterval = 1 * time.Minute
)

// Tracer is a tracing service provider
type Tracer struct {
	sync.Mutex
	wg     sync.WaitGroup
	closed sync2.AtomicBool

	ctx    context.Context
	cancel context.CancelFunc

	cfg Config
	cli pb.TracerClient

	logger log.Logger

	tso   *tsoGenerator
	idGen *IDGenerator

	jobs       map[EventType]chan *Job
	jobsStatus struct {
		sync.RWMutex
		closed bool
	}
	rpcWg sync.WaitGroup
}

// NewTracer creates a new Tracer
func NewTracer(cfg Config) *Tracer {
	t := Tracer{
		cfg:    cfg,
		tso:    newTsoGenerator(),
		idGen:  NewIDGen(),
		logger: log.With(zap.String("component", "tracer client")),
	}
	t.setJobChansClosed(true)
	t.ctx, t.cancel = context.WithCancel(context.Background())
	return &t
}

// Enable returns whether tracing is enabled
func (t *Tracer) Enable() bool {
	// current we don't support update `Enable` online, so we don't use any lock
	// to avoid race condition
	return t.cfg.Enable
}

// Start starts tracing service
func (t *Tracer) Start() {
	conn, err := grpc.Dial(t.cfg.TracerAddr, grpc.WithInsecure(), grpc.WithBackoffMaxDelay(3*time.Second))
	if err != nil {
		t.logger.Error("grpc dial failed", log.ShortError(err))
		return
	}
	t.cli = pb.NewTracerClient(conn)
	t.closed.Set(false)
	t.newJobChans()

	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		ctx2, cancel := context.WithCancel(t.ctx)
		t.tsoProcessor(ctx2)
		cancel()
	}()

	for _, ch := range t.jobs {
		t.wg.Add(1)
		go func(c <-chan *Job) {
			defer t.wg.Done()
			ctx2, cancel := context.WithCancel(t.ctx)
			t.jobProcessor(ctx2, c)
			cancel()
		}(ch)
	}
}

// Stop stops tracer
func (t *Tracer) Stop() {
	t.Lock()
	defer t.Unlock()
	if t.closed.Get() {
		return
	}
	if t.cancel != nil {
		t.cancel()
	}
	t.closed.Set(true)
	t.wg.Wait()
}

func (t *Tracer) tsoProcessor(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(1 * time.Minute):
			err := t.syncTS()
			if err != nil {
				t.logger.Error("sync timestamp failed", log.ShortError(err))
			}
		}
	}
}

// Assume we send GetTSO with local timestamp t1, timestamp gap between local
// and remote tso service is tx, remote tso service get GetTSO at timestamp t2
// and send response back as soon as possible. We get RPC response at local
// timestamp t3. then we have t1 + tx + ttl = t2; t2 - tx + ttl = t3
// so the real sending RPC request timestamp is t1 + tx = t2 + t1/2 - t3/2
func (t *Tracer) syncTS() error {
	t.rpcWg.Add(1)
	defer t.rpcWg.Done()
	t.tso.Lock()
	defer t.tso.Unlock()
	t.tso.localTS = time.Now().UnixNano()
	req := &pb.GetTSORequest{Id: t.cfg.Source}
	resp, err := t.cli.GetTSO(t.ctx, req)
	if err != nil {
		return terror.ErrTracingGetTSO.Delegate(err)
	}
	if !resp.Result {
		return terror.ErrTracingGetTSO.Generatef("sync ts with error: %s", resp.Msg)
	}
	currentTS := time.Now().UnixNano()
	oldSyncedTS := t.tso.syncedTS
	t.tso.syncedTS = resp.Ts + t.tso.localTS/2 - currentTS/2
	t.logger.Debug("sync TS", zap.Int64("old synced TS", oldSyncedTS), zap.Int64("current synced TS", t.tso.syncedTS), zap.Int64("local TS", t.tso.localTS))
	return nil
}

func (t *Tracer) newJobChans() {
	t.closeJobChans()
	t.jobs = make(map[EventType]chan *Job)
	for i := EventSyncerBinlog; i < EventFlush; i++ {
		t.jobs[i] = make(chan *Job, t.cfg.BatchSize)
	}
	t.setJobChansClosed(false)
}

func (t *Tracer) setJobChansClosed(closed bool) {
	t.jobsStatus.Lock()
	t.jobsStatus.closed = closed
	t.jobsStatus.Unlock()
}

func (t *Tracer) closeJobChans() {
	t.jobsStatus.Lock()
	defer t.jobsStatus.Unlock()
	if t.jobsStatus.closed {
		return
	}
	for _, ch := range t.jobs {
		close(ch)
	}
	t.jobsStatus.closed = true
}

func (t *Tracer) jobProcessor(ctx context.Context, jobChan <-chan *Job) {
	var (
		count = t.cfg.BatchSize
		jobs  = make([]*Job, 0, count)
	)

	clearJobs := func() {
		jobs = jobs[:0]
	}

	processError := func(err error) {
		t.logger.Error("problem with job processor", log.ShortError(err))
	}

	var err error
	for {
		select {
		case <-ctx.Done():
			err = t.ProcessTraceEvents(jobs)
			if err != nil {
				processError(err)
			}
			clearJobs()
			return
		case <-time.After(uploadInterval):
			err = t.ProcessTraceEvents(jobs)
			if err != nil {
				processError(err)
			}
			clearJobs()
		case job, ok := <-jobChan:
			if !ok {
				return
			}

			if job.Tp != EventFlush {
				jobs = append(jobs, job)
			}

			if len(jobs) >= count || job.Tp == EventFlush {
				err = t.ProcessTraceEvents(jobs)
				if err != nil {
					processError(err)
				}
				clearJobs()
			}
		}
	}
}

// AddJob add a job to tracer
func (t *Tracer) AddJob(job *Job) {
	t.jobsStatus.RLock()
	defer t.jobsStatus.RUnlock()
	if t.jobsStatus.closed {
		t.logger.Warn("jobs channel already closed, add job failed", zap.Reflect("job", job))
		return
	}
	if job.Tp == EventFlush {
		for _, tp := range dispatchEventType {
			t.jobs[tp] <- job
		}
	} else {
		t.jobs[job.Tp] <- job
	}
}

func (t *Tracer) collectBaseEvent(source, traceID, traceGID string, traceType pb.TraceType) (*pb.BaseEvent, error) {
	file, line, err := GetTraceCode(3)
	if err != nil {
		return nil, err
	}

	tso := t.GetTSO()
	if traceID == "" {
		traceID = t.GetTraceID(source)
	}
	return &pb.BaseEvent{
		Filename: file,
		Line:     int32(line),
		Tso:      tso,
		TraceID:  traceID,
		GroupID:  traceGID,
		Type:     traceType,
	}, nil
}

// GetTSO returns current tso
func (t *Tracer) GetTSO() int64 {
	t.tso.RLock()
	defer t.tso.RUnlock()
	currentTS := time.Now().UnixNano()
	return t.tso.syncedTS + currentTS - t.tso.localTS
}

// GetTraceID returns a new traceID for tracing
func (t *Tracer) GetTraceID(source string) string {
	return t.idGen.NextID(source, 0)
}
