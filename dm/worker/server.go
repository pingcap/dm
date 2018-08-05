// Copyright 2018 PingCAP, Inc.
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

package worker

import (
	"fmt"
	"net"
	"sync"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-enterprise-tools/dm/config"
	"github.com/pingcap/tidb-enterprise-tools/dm/pb"
	"github.com/siddontang/go/sync2"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// Server accepts RPC requests
// dispatches requests to worker
// sends responses to RPC client
type Server struct {
	sync.Mutex
	wg     sync.WaitGroup
	closed sync2.AtomicBool

	cfg *Config

	svr    *grpc.Server
	worker *Worker
}

// NewServer creates a new Server
func NewServer(cfg *Config) *Server {
	s := Server{
		cfg:    cfg,
		worker: NewWorker(cfg),
	}
	s.closed.Set(true) // not start yet
	return &s
}

// Start starts to serving
func (s *Server) Start() error {
	lis, err := net.Listen("tcp", s.cfg.WorkerAddr)
	if err != nil {
		return errors.Trace(err)
	}

	s.closed.Set(false)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		// start running worker to handle requests
		s.worker.Start()
	}()

	log.Infof("[server] listening on %v for API request", s.cfg.WorkerAddr)

	s.svr = grpc.NewServer()
	pb.RegisterWorkerServer(s.svr, s)
	return errors.Trace(s.svr.Serve(lis))
}

// Close close the RPC server
func (s *Server) Close() {
	s.Lock()
	defer s.Unlock()
	if s.closed.Get() {
		return
	}

	if s.svr != nil {
		s.svr.GracefulStop()
	}

	// close worker and wait for return
	s.worker.Close()
	s.wg.Wait()

	s.closed.Set(true)
}

// StartSubTask implements WorkerServer.StartSubTask
func (s *Server) StartSubTask(ctx context.Context, req *pb.StartSubTaskRequest) (*pb.CommonWorkerResponse, error) {
	log.Infof("[server] receive StartSubTask request %+v", req)

	cfg := config.NewSubTaskConfig()
	err := cfg.Decode(req.Task)
	if err != nil {
		log.Errorf("[server] decode config from request %+v error %v", req.Task, errors.ErrorStack(err))
		return &pb.CommonWorkerResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}, nil
	}

	err = s.worker.StartSubTask(cfg)
	if err != nil {
		log.Errorf("[server] start sub task %s error %v", cfg.Name, errors.ErrorStack(err))
		return &pb.CommonWorkerResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}, nil
	}

	return &pb.CommonWorkerResponse{
		Result: true,
		Msg:    "",
	}, nil
}

func (s *Server) OperateSubTask(ctx context.Context, req *pb.OperateSubTaskRequest) (*pb.OperateSubTaskResponse, error) {
	log.Infof("[server] receive OperateSubTask request %+v", req)

	resp := &pb.OperateSubTaskResponse{
		Op:     req.Op,
		Result: false,
	}

	name := req.Name
	var err error
	switch req.Op {
	case pb.TaskOp_Stop:
		err = s.worker.StopSubTask(name)
	case pb.TaskOp_Pause:
		err = s.worker.PauseSubTask(name)
	case pb.TaskOp_Resume:
		err = s.worker.ResumeSubTask(name)
	default:
		resp.Msg = fmt.Sprintf("invalid operate %s on sub task", req.Op.String())
		return resp, nil
	}

	if err != nil {
		log.Errorf("[server] operate(%s) sub task %s error %v", req.Op.String(), name, errors.ErrorStack(err))
		resp.Msg = errors.ErrorStack(err)
		return resp, nil
	}

	resp.Result = true
	return resp, nil
}

// UpdateSubTask implements WorkerServer.UpdateSubTask
// Note: zxc will implement it when the master-worker-ctl framework completed
func (s *Server) UpdateSubTask(ctx context.Context, req *pb.UpdateSubTaskRequest) (*pb.CommonWorkerResponse, error) {
	log.Infof("[server] receive UpdateSubTask request %+v", req)
	return &pb.CommonWorkerResponse{
		Result: false,
		Msg:    "not implement",
	}, nil
}

// QueryStatus implements WorkerServer.QueryStatus
func (s *Server) QueryStatus(ctx context.Context, req *pb.QueryStatusRequest) (*pb.QueryStatusResponse, error) {
	log.Infof("[server] receive QueryStatus request %+v", req)

	resp := &pb.QueryStatusResponse{
		Result:        true,
		SubTaskStatus: s.worker.QueryStatus(req.Name),
		RelayStatus:   s.worker.relay.Status().(*pb.RelayStatus),
	}

	if len(resp.SubTaskStatus) == 0 {
		resp.Msg = "no sub task started"
	}
	return resp, nil
}

// HandleSQLs implements WorkerServer.HandleSQLs
// Note: zxc will implement it when the error-handle completed
func (s *Server) HandleSQLs(ctx context.Context, req *pb.HandleSQLsRequest) (*pb.CommonWorkerResponse, error) {
	log.Infof("[server] receive HandleSQLs request %+v", req)
	return &pb.CommonWorkerResponse{
		Result: false,
		Msg:    "not implement",
	}, nil
}
