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

package tracer

import (
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/errors"
	"github.com/siddontang/go/sync2"
	"github.com/soheilhy/cmux"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/tracing"
)

var (
	cmuxReadTimeout = 10 * time.Second
)

// Server accepts tracing RPC requests and sends RPC responses back
type Server struct {
	sync.Mutex
	wg     sync.WaitGroup
	closed sync2.AtomicBool

	cfg *Config

	rootLis net.Listener
	svr     *grpc.Server

	eventStore   *EventStore
	idGen        *tracing.IDGenerator
	statusServer *http.Server
}

// NewServer creates a new Server
func NewServer(cfg *Config) *Server {
	s := Server{
		cfg:        cfg,
		eventStore: NewEventStore(),
		idGen:      tracing.NewIDGen(),
	}
	s.closed.Set(true)
	return &s
}

// Start starts to serving
func (s *Server) Start() error {
	var err error
	s.rootLis, err = net.Listen("tcp", s.cfg.TracerAddr)
	if err != nil {
		return errors.Trace(err)
	}

	s.closed.Set(false)

	// create a cmux
	m := cmux.New(s.rootLis)
	m.SetReadTimeout(cmuxReadTimeout) // set a timeout, ref: https://github.com/pingcap/tidb-binlog/pull/352

	grpcL := m.Match(cmux.HTTP2HeaderField("content-type", "application/grpc"))
	httpL := m.Match(cmux.HTTP1Fast())

	s.svr = grpc.NewServer()
	pb.RegisterTracerServer(s.svr, s)
	go func() {
		err2 := s.svr.Serve(grpcL)
		if err2 != nil && !common.IsErrNetClosing(err2) && err2 != cmux.ErrListenerClosed {
			log.Errorf("[server] gRPC server return with error %s", err2.Error())
		}
	}()
	go s.startHTTPServer(httpL)

	log.Infof("[server] listening on %v for gRPC API and status request", s.cfg.TracerAddr)
	err = m.Serve()
	if err != nil && common.IsErrNetClosing(err) {
		err = nil
	}
	return errors.Trace(err)
}

// Close close the RPC server
func (s *Server) Close() {
	s.Lock()
	defer s.Unlock()
	if s.closed.Get() {
		return
	}

	err := s.rootLis.Close()
	if err != nil && !common.IsErrNetClosing(err) {
		log.Errorf("[server] close net listener with error %s", err.Error())
	}
	if s.svr != nil {
		// GracefulStop can not cancel active stream RPCs
		// and the stream RPC may block on Recv or Send
		// so we use Stop instead to cancel all active RPCs
		s.svr.Stop()
	}

	s.closed.Set(true)
}

// GetTSO implements TracerServer.GetTSO
func (s *Server) GetTSO(ctx context.Context, req *pb.GetTSORequest) (*pb.GetTSOResponse, error) {
	log.Debugf("[server] receives GetTSO request %+v", req)
	resp := &pb.GetTSOResponse{
		Result: true,
		Ts:     time.Now().UnixNano(),
	}
	return resp, nil
}

// UploadSyncerBinlogEvent implements TracerServer.UploadSyncerBinlogEvent
func (s *Server) UploadSyncerBinlogEvent(ctx context.Context, req *pb.UploadSyncerBinlogEventRequest) (*pb.CommonUploadResponse, error) {
	log.Debugf("[server] receives UploadSyncerBinlogEvent request %+v", req)
	for _, e := range req.Events {
		err := s.eventStore.addNewEvent(&TraceEvent{
			Type:  pb.TraceType_BinlogEvent,
			Event: e,
		})
		if err != nil {
			return &pb.CommonUploadResponse{
				Result: false,
				Msg:    errors.ErrorStack(err),
			}, nil
		}
	}
	return &pb.CommonUploadResponse{Result: true}, nil
}

// UploadSyncerJobEvent implements TracerServer.UploadSyncerJobEvent
func (s *Server) UploadSyncerJobEvent(ctx context.Context, req *pb.UploadSyncerJobEventRequest) (*pb.CommonUploadResponse, error) {
	log.Debugf("[server] receives UploadSyncerJobEvent request %+v", req)
	for _, e := range req.Events {
		err := s.eventStore.addNewEvent(&TraceEvent{
			Type:  pb.TraceType_JobEvent,
			Event: e,
		})
		if err != nil {
			return &pb.CommonUploadResponse{
				Result: false,
				Msg:    errors.ErrorStack(err),
			}, nil
		}
	}
	return &pb.CommonUploadResponse{Result: true}, nil
}
