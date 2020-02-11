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
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	tc "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go/sync2"
	"github.com/soheilhy/cmux"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/log"
)

var (
	cmuxReadTimeout = 10 * time.Second
)

// TraceEvent used for tracing event abstraction
type TraceEvent struct {
	Type  pb.TraceType
	Event interface{}
}

// MockServer is a mock tracing server
type MockServer struct {
	sync.Mutex
	closed sync2.AtomicBool

	addr string

	rootLis net.Listener
	svr     *grpc.Server

	check struct {
		sync.RWMutex
		m map[string][]*TraceEvent
	}

	checkCh chan interface{}
}

// NewServer creates a new Mock Server
func NewMockServer(addr string, ch chan interface{}) *MockServer {
	s := MockServer{
		addr:    addr,
		checkCh: ch,
	}
	s.check.m = make(map[string][]*TraceEvent)
	s.closed.Set(true)
	return &s
}

// Start starts to serving
func (s *MockServer) Start() error {
	var err error
	s.rootLis, err = net.Listen("tcp", s.addr)
	if err != nil {
		return errors.Trace(err)
	}

	s.closed.Set(false)

	// create a cmux
	m := cmux.New(s.rootLis)
	// set a timeout
	m.SetReadTimeout(cmuxReadTimeout)

	grpcL := m.MatchWithWriters(cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))

	s.svr = grpc.NewServer()
	pb.RegisterTracerServer(s.svr, s)
	go func() {
		err2 := s.svr.Serve(grpcL)
		if err2 != nil && !common.IsErrNetClosing(err2) && err2 != cmux.ErrListenerClosed {
			log.L().Error("gRPC server return with error", log.ShortError(err2))
		}
	}()

	log.L().Info("listening on tracing address for gRPC API and status request", zap.String("tracing address", s.addr))
	err = m.Serve()
	if err != nil && common.IsErrNetClosing(err) {
		err = nil
	}
	return errors.Trace(err)
}

// Close close the RPC server
func (s *MockServer) Close() {
	s.Lock()
	defer s.Unlock()
	if s.closed.Get() {
		return
	}

	err := s.rootLis.Close()
	if err != nil && !common.IsErrNetClosing(err) {
		log.L().Error("close net listener with error", log.ShortError(err))
	}
	if s.svr != nil {
		s.svr.Stop()
	}

	s.closed.Set(true)
}

// GetTSO implements TracerServer.GetTSO
func (s *MockServer) GetTSO(ctx context.Context, req *pb.GetTSORequest) (*pb.GetTSOResponse, error) {
	resp := &pb.GetTSOResponse{
		Result: true,
		Ts:     time.Now().UnixNano(),
	}
	return resp, nil
}

// UploadSyncerBinlogEvent implements TracerServer.UploadSyncerBinlogEvent
func (s *MockServer) UploadSyncerBinlogEvent(ctx context.Context, req *pb.UploadSyncerBinlogEventRequest) (*pb.CommonUploadResponse, error) {
	defer func() {
		s.checkCh <- struct{}{}
	}()

	for _, e := range req.Events {
		s.check.Lock()
		if _, ok := s.check.m[e.Base.TraceID]; !ok {
			s.check.m[e.Base.TraceID] = make([]*TraceEvent, 0)
		}
		s.check.m[e.Base.TraceID] = append(s.check.m[e.Base.TraceID], &TraceEvent{Type: pb.TraceType_BinlogEvent, Event: e})
		s.check.Unlock()
	}
	return &pb.CommonUploadResponse{Result: true}, nil
}

// UploadSyncerJobEvent implements TracerServer.UploadSyncerJobEvent
func (s *MockServer) UploadSyncerJobEvent(ctx context.Context, req *pb.UploadSyncerJobEventRequest) (*pb.CommonUploadResponse, error) {
	defer func() {
		s.checkCh <- struct{}{}
	}()
	for _, e := range req.Events {
		s.check.Lock()
		if _, ok := s.check.m[e.Base.TraceID]; !ok {
			s.check.m[e.Base.TraceID] = make([]*TraceEvent, 0)
		}
		s.check.m[e.Base.TraceID] = append(s.check.m[e.Base.TraceID], &TraceEvent{Type: pb.TraceType_JobEvent, Event: e})
		s.check.Unlock()
	}
	return &pb.CommonUploadResponse{Result: true}, nil
}

// CheckEvent checks mock server has received the specific event
func (s *MockServer) CheckEvent(traceID string, e *TraceEvent, c *tc.C) {
	select {
	case <-s.checkCh:
	case <-time.After(time.Second):
		c.Fail()
	}

	s.check.RLock()
	defer s.check.RUnlock()
	evs, ok := s.check.m[traceID]
	if !ok {
		c.Fail()
	}
	for _, ev := range evs {
		if ev.Type != e.Type {
			continue
		}
		switch e.Type {
		case pb.TraceType_JobEvent:
			e2, ok1 := e.Event.(*pb.SyncerJobEvent)
			ev2, ok2 := ev.Event.(*pb.SyncerJobEvent)
			c.Assert(ok1, tc.IsTrue)
			c.Assert(ok2, tc.IsTrue)
			if e2.String() == ev2.String() {
				return
			}
		case pb.TraceType_BinlogEvent:
			e2, ok1 := e.Event.(*pb.SyncerBinlogEvent)
			ev2, ok2 := ev.Event.(*pb.SyncerBinlogEvent)
			c.Assert(ok1, tc.IsTrue)
			c.Assert(ok2, tc.IsTrue)
			if e2.String() == ev2.String() {
				return
			}
		}
	}
	c.Fail()
}

type TracerTestSuite struct {
	server  *MockServer
	bind    string
	tracer  *Tracer
	cfg     Config
	checkCh chan interface{}
}

var _ = tc.Suite(new(TracerTestSuite))

func TestSuite(t *testing.T) {
	tc.TestingT(t)
}

const retryTime = 100

func (ts *TracerTestSuite) waitUntilServerOnline() error {
	for i := 0; i < retryTime; i++ {
		resp, err := ts.tracer.cli.UploadSyncerBinlogEvent(context.Background(), &pb.UploadSyncerBinlogEventRequest{})
		if err == nil && resp.Result {
			return nil
		}
		time.Sleep(time.Millisecond * 10)
	}
	return errors.Errorf("failed to connect rpc port for %d retries in every 10ms", retryTime)
}

func (ts *TracerTestSuite) startServer(c *tc.C) {
	ts.prepare(c)
	ts.server = NewMockServer(ts.bind, ts.checkCh)
	go ts.server.Start()
	err := ts.waitUntilServerOnline()
	c.Assert(err, tc.IsNil)
}

func (ts *TracerTestSuite) stopServer(c *tc.C) {
	ts.tracer.Stop()
	if ts.server != nil {
		ts.server.Close()
	}
}

func (ts *TracerTestSuite) prepare(c *tc.C) {
	ts.bind = ":8263"
	ts.checkCh = make(chan interface{}, 100)
	ts.cfg = Config{
		Enable:     true,
		Source:     "mysql-replica-01",
		TracerAddr: fmt.Sprintf("127.0.0.1%s", ts.bind),
		BatchSize:  1,
	}
	ts.tracer = InitTracerHub(ts.cfg)
	ts.tracer.Start()
}

func (ts *TracerTestSuite) TestCollectSyncerEvent(c *tc.C) {
	ts.startServer(c)
	defer ts.stopServer(c)

ForEnd:
	for {
		select {
		case <-ts.checkCh:
		default:
			break ForEnd
		}
	}

	source := fmt.Sprintf("%s.syncer.%s", ts.cfg.Source, "test-task")
	event, err := ts.tracer.CollectSyncerBinlogEvent(source, true, false, mysql.Position{Name: "bin|000001.000004", Pos: 1515}, mysql.Position{Name: "bin|000001.000004", Pos: 1626}, 1, 2)
	c.Assert(err, tc.IsNil)
	ts.server.CheckEvent(event.Base.TraceID, &TraceEvent{Type: pb.TraceType_BinlogEvent, Event: event}, c)

	event, err = ts.tracer.CollectSyncerBinlogEvent(source, true, false, mysql.Position{Name: "bin|000001.000004", Pos: 1626}, mysql.Position{Name: "bin|000001.000004", Pos: 1873}, 1, 2)
	c.Assert(err, tc.IsNil)
	ts.server.CheckEvent(event.Base.TraceID, &TraceEvent{Type: pb.TraceType_BinlogEvent, Event: event}, c)

	event2, err2 := ts.tracer.CollectSyncerJobEvent(event.Base.TraceID, "", 1, mysql.Position{Name: "bin|000001.000004", Pos: 1626}, mysql.Position{Name: "bin|000001.000004", Pos: 1873},
		"q_1", "REPLACE INTO `test`.`t_target` (`id`,`ct`,`name`) VALUES (?,?,?);", []string{}, []interface{}{1, "2019-03-12 12:13:00", "test"}, pb.SyncerJobState_queued)
	c.Assert(err2, tc.IsNil)
	ts.server.CheckEvent(event2.Base.TraceID, &TraceEvent{Type: pb.TraceType_JobEvent, Event: event2}, c)

	event2, err2 = ts.tracer.CollectSyncerJobEvent(event.Base.TraceID, "", 1, mysql.Position{Name: "bin|000001.000004", Pos: 1626}, mysql.Position{Name: "bin|000001.000004", Pos: 1873},
		"q_1", "REPLACE INTO `test`.`t_target` (`id`,`ct`,`name`) VALUES (?,?,?);", []string{}, []interface{}{1, "2019-03-12 12:13:00", "test"}, pb.SyncerJobState_success)
	c.Assert(err2, tc.IsNil)
	ts.server.CheckEvent(event2.Base.TraceID, &TraceEvent{Type: pb.TraceType_JobEvent, Event: event2}, c)

	ts.tracer.rpcWg.Wait()
}
