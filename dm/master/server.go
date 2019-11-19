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

package master

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/etcd"
	"github.com/siddontang/go/sync2"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/pingcap/dm/checker"
	"github.com/pingcap/dm/dm/config"
	operator "github.com/pingcap/dm/dm/master/sql-operator"
	"github.com/pingcap/dm/dm/master/workerrpc"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/tracing"
	"github.com/pingcap/dm/syncer"
)

var (
	fetchDDLInfoRetryTimeout = 5 * time.Second
)

// Server handles RPC requests for dm-master
type Server struct {
	sync.Mutex

	cfg *Config

	// the embed etcd server, and the gRPC/HTTP API server also attached to it.
	etcd *embed.Etcd

	// the client of etcd
	etcdClient *etcd.Client

	// WaitGroup for background functions.
	bgFunWg sync.WaitGroup

	// dm-worker-ID(host:ip) -> dm-worker client management
	workerClients map[string]workerrpc.Client

	// task-name -> worker-list
	taskWorkers map[string][]string

	// DDL lock keeper
	lockKeeper *LockKeeper

	// SQL operator holder
	sqlOperatorHolder *operator.Holder

	// trace group id generator
	idGen *tracing.IDGenerator

	// agent pool
	ap *AgentPool

	closed sync2.AtomicBool
}

// NewServer creates a new Server
func NewServer(cfg *Config) *Server {
	server := Server{
		cfg:               cfg,
		workerClients:     make(map[string]workerrpc.Client),
		taskWorkers:       make(map[string][]string),
		lockKeeper:        NewLockKeeper(),
		sqlOperatorHolder: operator.NewHolder(),
		idGen:             tracing.NewIDGen(),
		ap:                NewAgentPool(&RateLimitConfig{rate: cfg.RPCRateLimit, burst: cfg.RPCRateBurst}),
	}
	server.closed.Set(true)

	return &server
}

// Start starts to serving
func (s *Server) Start(ctx context.Context) (err error) {
	// prepare config to join an existing cluster
	err = prepareJoinEtcd(s.cfg)
	if err != nil {
		return
	}
	log.L().Info("config after join prepared", zap.Stringer("config", s.cfg))

	// generates embed etcd config before any concurrent gRPC calls.
	// potential concurrent gRPC calls:
	//   - workerrpc.NewGRPCClient
	//   - getHTTPAPIHandler
	// no `String` method exists for embed.Config, and can not marshal it to join too.
	// but when starting embed etcd server, the etcd pkg will log the config.
	// https://github.com/etcd-io/etcd/blob/3cf2f69b5738fb702ba1a935590f36b52b18979b/embed/etcd.go#L299
	etcdCfg, err := s.cfg.genEmbedEtcdConfig()
	if err != nil {
		return
	}

	// create clients to DM-workers
	for _, workerAddr := range s.cfg.DeployMap {
		s.workerClients[workerAddr], err = workerrpc.NewGRPCClient(workerAddr)
		if err != nil {
			return
		}
	}

	// get an HTTP to gRPC API handler.
	apiHandler, err := getHTTPAPIHandler(ctx, s.cfg.MasterAddr)
	if err != nil {
		return
	}

	// HTTP handlers on etcd's client IP:port
	// no `metrics` for DM-master now, add it later.
	// NOTE: after received any HTTP request from chrome browser,
	// the server may be blocked when closing sometime.
	// And any request to etcd's builtin handler has the same problem.
	// And curl or safari browser does trigger this problem.
	// But I haven't figured it out.
	// (maybe more requests are sent from chrome or its extensions).
	userHandles := map[string]http.Handler{
		"/apis/":  apiHandler,
		"/status": getStatusHandle(),
		"/debug/": getDebugHandler(),
	}

	// gRPC API server
	gRPCSvr := func(gs *grpc.Server) { pb.RegisterMasterServer(gs, s) }

	// start embed etcd server, gRPC API server and HTTP (API, status and debug) server.
	s.etcd, err = startEtcd(etcdCfg, gRPCSvr, userHandles)
	if err != nil {
		return
	}

	s.etcdClient, err = getEtcdClient(s.cfg.MasterAddr)
	if err != nil {
		return
	}

	s.closed.Set(false) // the server started now.

	s.bgFunWg.Add(1)
	go func() {
		defer s.bgFunWg.Done()
		s.WatchRequest(ctx)
	}()

	s.bgFunWg.Add(1)
	go func() {
		defer s.bgFunWg.Done()
		s.ap.Start(ctx)
	}()

	s.bgFunWg.Add(1)
	go func() {
		defer s.bgFunWg.Done()
		select {
		case <-ctx.Done():
			return
		case <-time.After(3 * time.Second):
			// update task -> workers after started
			s.updateTaskWorkers(ctx)
		}
	}()

	s.bgFunWg.Add(1)
	go func() {
		defer s.bgFunWg.Done()
		// fetch DDL info from dm-workers to sync sharding DDL
		s.fetchWorkerDDLInfo(ctx)
	}()

	log.L().Info("listening gRPC API and status request", zap.String("address", s.cfg.MasterAddr))
	return
}

// Close close the RPC server, this function can be called multiple times
func (s *Server) Close() {
	s.Lock()
	defer s.Unlock()
	if s.closed.Get() {
		return
	}
	log.L().Info("closing server")

	// wait for background functions returned
	s.bgFunWg.Wait()

	// close the etcd and other attached servers
	if s.etcd != nil {
		s.etcd.Close()
	}
	s.closed.Set(true)
}

func errorCommonWorkerResponse(msg string, worker string) *pb.CommonWorkerResponse {
	return &pb.CommonWorkerResponse{
		Result: false,
		Worker: worker,
		Msg:    msg,
	}
}

// StartTask implements MasterServer.StartTask
func (s *Server) StartTask(ctx context.Context, req *pb.StartTaskRequest) (*pb.StartTaskResponse, error) {
	log.L().Info("receive request and will save it to etcd", zap.Stringer("payload", req), zap.String("request", "StartTask"))

	responseErr := func(err error) *pb.StartTaskResponse {
		return &pb.StartTaskResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}
	}

	reqBytes, err := req.Marshal()
	if err != nil {
		return responseErr(err), nil
	}
	responseBytes, err := s.saveRequestAndWaitResponse(ctx, pb.OperateType_StartTask, reqBytes)
	if err != nil {
		return responseErr(err), nil
	}

	response := &pb.StartTaskResponse{}
	err = response.Unmarshal(responseBytes)
	if err != nil {
		return responseErr(err), nil
	}

	return response, nil
}

func (s *Server) startTask(ctx context.Context, req *pb.StartTaskRequest) (*pb.StartTaskResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "StartTask"))

	cfg, stCfgs, err := s.generateSubTask(ctx, req.Task)
	if err != nil {
		return &pb.StartTaskResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}, nil
	}
	log.L().Info("", zap.String("task name", cfg.Name), zap.Stringer("task", cfg), zap.String("request", "StartTask"))

	workerRespCh := make(chan *pb.CommonWorkerResponse, len(stCfgs)+len(req.Workers))
	if len(req.Workers) > 0 {
		// specify only start task on partial dm-workers
		workerCfg := make(map[string]*config.SubTaskConfig)
		for _, stCfg := range stCfgs {
			worker, ok := s.cfg.DeployMap[stCfg.SourceID]
			if ok {
				workerCfg[worker] = stCfg
			}
		}
		stCfgs = make([]*config.SubTaskConfig, 0, len(req.Workers))
		for _, worker := range req.Workers {
			if stCfg, ok := workerCfg[worker]; ok {
				stCfgs = append(stCfgs, stCfg)
			} else {
				workerRespCh <- errorCommonWorkerResponse("worker not found in task's config or deployment config", worker)
			}
		}
	}

	validWorkerCh := make(chan string, len(stCfgs))
	var wg sync.WaitGroup
	for _, stCfg := range stCfgs {
		wg.Add(1)
		go s.ap.Emit(ctx, 0, func(args ...interface{}) {
			defer wg.Done()
			cli, worker, stCfgToml, taskName, err := s.taskConfigArgsExtractor(args...)
			if err != nil {
				workerRespCh <- errorCommonWorkerResponse(err.Error(), worker)
				return
			}
			validWorkerCh <- worker
			request := &workerrpc.Request{
				Type:         workerrpc.CmdStartSubTask,
				StartSubTask: &pb.StartSubTaskRequest{Task: stCfgToml},
			}
			resp, err := cli.SendRequest(ctx, request, s.cfg.RPCTimeout)
			workerResp := s.handleOperationResult(ctx, cli, taskName, worker, err, resp)
			workerResp.Meta.Worker = worker
			workerRespCh <- workerResp.Meta
		}, func(args ...interface{}) {
			defer wg.Done()
			_, worker, _, _, err := s.taskConfigArgsExtractor(args...)
			if err != nil {
				workerRespCh <- errorCommonWorkerResponse(err.Error(), worker)
				return
			}
			workerRespCh <- errorCommonWorkerResponse(terror.ErrMasterNoEmitToken.Generate(worker).Error(), worker)
		}, stCfg)
	}
	wg.Wait()

	workerRespMap := make(map[string]*pb.CommonWorkerResponse, len(stCfgs))
	workers := make([]string, 0, len(stCfgs))
	for len(workerRespCh) > 0 {
		workerResp := <-workerRespCh
		workerRespMap[workerResp.Worker] = workerResp
		workers = append(workers, workerResp.Worker)
	}

	// TODO: simplify logic of response sort
	sort.Strings(workers)
	workerResps := make([]*pb.CommonWorkerResponse, 0, len(workers))
	for _, worker := range workers {
		workerResps = append(workerResps, workerRespMap[worker])
	}

	// record task -> workers map
	validWorkers := make([]string, 0, len(validWorkerCh))
	for len(validWorkerCh) > 0 {
		worker := <-validWorkerCh
		validWorkers = append(validWorkers, worker)
	}
	replace := len(req.Workers) == 0 // a fresh start
	s.addTaskWorkers(cfg.Name, validWorkers, replace)

	return &pb.StartTaskResponse{
		Result:  true,
		Workers: workerResps,
	}, nil
}

// OperateTask implements MasterServer.OperateTask
func (s *Server) OperateTask(ctx context.Context, req *pb.OperateTaskRequest) (*pb.OperateTaskResponse, error) {
	log.L().Info("receive request and will save it to etcd", zap.Stringer("payload", req), zap.String("request", "OperateTask"))

	responseErr := func(err error) *pb.OperateTaskResponse {
		return &pb.OperateTaskResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}
	}

	reqBytes, err := req.Marshal()
	if err != nil {
		return responseErr(err), nil
	}

	responseBytes, err := s.saveRequestAndWaitResponse(ctx, pb.OperateType_OperateTask, reqBytes)
	if err != nil {
		return responseErr(err), nil
	}

	response := &pb.OperateTaskResponse{}
	err = response.Unmarshal(responseBytes)
	if err != nil {
		return responseErr(err), nil
	}

	return response, nil
}

func (s *Server) operateTask(ctx context.Context, req *pb.OperateTaskRequest) (*pb.OperateTaskResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "OperateTask"))

	resp := &pb.OperateTaskResponse{
		Op:     req.Op,
		Result: false,
	}

	workers := s.getTaskWorkers(req.Name)
	if len(workers) == 0 {
		resp.Msg = fmt.Sprintf("task %s has no workers or not exist, can try `refresh-worker-tasks` cmd first", req.Name)
		return resp, nil
	}
	if len(req.Workers) > 0 {
		workers = req.Workers // specify only do operation on partial dm-workers
	}
	workerRespCh := make(chan *pb.OperateSubTaskResponse, len(workers))

	handleErr := func(err error, worker string) {
		log.L().Error("response error", zap.Error(err))
		workerResp := &pb.OperateSubTaskResponse{
			Meta: errorCommonWorkerResponse(err.Error(), worker),
			Op:   req.Op,
		}
		workerRespCh <- workerResp
	}

	subReq := &workerrpc.Request{
		Type: workerrpc.CmdOperateSubTask,
		OperateSubTask: &pb.OperateSubTaskRequest{
			Op:   req.Op,
			Name: req.Name,
		},
	}

	var wg sync.WaitGroup
	for _, worker := range workers {
		wg.Add(1)
		go s.ap.Emit(ctx, 0, func(args ...interface{}) {
			defer wg.Done()
			cli, worker1, err := s.workerArgsExtractor(args...)
			if err != nil {
				handleErr(err, worker1)
				return
			}
			resp, err := cli.SendRequest(ctx, subReq, s.cfg.RPCTimeout)
			workerResp := s.handleOperationResult(ctx, cli, req.Name, worker1, err, resp)
			workerResp.Op = req.Op
			workerResp.Meta.Worker = worker1
			workerRespCh <- workerResp
		}, func(args ...interface{}) {
			defer wg.Done()
			_, worker1, err := s.workerArgsExtractor(args...)
			if err != nil {
				handleErr(err, worker1)
				return
			}
			handleErr(terror.ErrMasterNoEmitToken.Generate(worker1), worker1)
		}, worker)
	}
	wg.Wait()

	validWorkers := make([]string, 0, len(workers))
	workerRespMap := make(map[string]*pb.OperateSubTaskResponse, len(workers))
	for len(workerRespCh) > 0 {
		workerResp := <-workerRespCh
		workerRespMap[workerResp.Meta.Worker] = workerResp
		if len(workerResp.Meta.Msg) == 0 { // no error occurred
			validWorkers = append(validWorkers, workerResp.Meta.Worker)
		}
	}

	workerResps := make([]*pb.OperateSubTaskResponse, 0, len(workers))
	for _, worker := range workers {
		workerResps = append(workerResps, workerRespMap[worker])
	}

	if req.Op == pb.TaskOp_Stop {
		// remove (partial / all) workers for a task
		s.removeTaskWorkers(req.Name, validWorkers)
	}

	resp.Result = true
	resp.Workers = workerResps

	return resp, nil
}

// UpdateTask implements MasterServer.UpdateTask
func (s *Server) UpdateTask(ctx context.Context, req *pb.UpdateTaskRequest) (*pb.UpdateTaskResponse, error) {
	log.L().Info("receive request and will save it to etcd", zap.Stringer("payload", req), zap.String("request", "UpdateTask"))

	responseErr := func(err error) *pb.UpdateTaskResponse {
		return &pb.UpdateTaskResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}
	}
	reqBytes, err := req.Marshal()
	if err != nil {
		return responseErr(err), nil
	}
	responseBytes, err := s.saveRequestAndWaitResponse(ctx, pb.OperateType_UpdateTask, reqBytes)
	if err != nil {
		return responseErr(err), nil
	}

	response := &pb.UpdateTaskResponse{}
	err = response.Unmarshal(responseBytes)
	if err != nil {
		return responseErr(err), nil
	}

	return response, nil
}

func (s *Server) updateTask(ctx context.Context, req *pb.UpdateTaskRequest) (*pb.UpdateTaskResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "UpdateTask"))

	cfg, stCfgs, err := s.generateSubTask(ctx, req.Task)
	if err != nil {
		return &pb.UpdateTaskResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}, nil
	}
	log.L().Info("update task", zap.String("task name", cfg.Name), zap.Stringer("task", cfg))

	workerRespCh := make(chan *pb.CommonWorkerResponse, len(stCfgs)+len(req.Workers))
	if len(req.Workers) > 0 {
		// specify only update task on partial dm-workers
		// filter sub-task-configs through user specified workers
		// if worker not exist, an error message will return
		workerCfg := make(map[string]*config.SubTaskConfig)
		for _, stCfg := range stCfgs {
			worker, ok := s.cfg.DeployMap[stCfg.SourceID]
			if ok {
				workerCfg[worker] = stCfg
			} // only record existed workers
		}
		stCfgs = make([]*config.SubTaskConfig, 0, len(req.Workers))
		for _, worker := range req.Workers {
			if stCfg, ok := workerCfg[worker]; ok {
				stCfgs = append(stCfgs, stCfg)
			} else {
				workerRespCh <- errorCommonWorkerResponse("worker not found in task's config or deployment config", worker)
			}
		}
	}

	var wg sync.WaitGroup
	for _, stCfg := range stCfgs {
		wg.Add(1)
		go s.ap.Emit(ctx, 0, func(args ...interface{}) {
			defer wg.Done()
			cli, worker, stCfgToml, taskName, err := s.taskConfigArgsExtractor(args...)
			if err != nil {
				workerRespCh <- errorCommonWorkerResponse(err.Error(), worker)
				return
			}
			request := &workerrpc.Request{
				Type:          workerrpc.CmdUpdateSubTask,
				UpdateSubTask: &pb.UpdateSubTaskRequest{Task: stCfgToml},
			}
			resp, err := cli.SendRequest(ctx, request, s.cfg.RPCTimeout)
			workerResp := s.handleOperationResult(ctx, cli, taskName, worker, err, resp)
			workerResp.Meta.Worker = worker
			workerRespCh <- workerResp.Meta
		}, func(args ...interface{}) {
			defer wg.Done()
			_, worker, _, _, err := s.taskConfigArgsExtractor(args...)
			if err != nil {
				workerRespCh <- errorCommonWorkerResponse(err.Error(), worker)
				return
			}
			workerRespCh <- errorCommonWorkerResponse(terror.ErrMasterNoEmitToken.Generate(worker).Error(), worker)
		}, stCfg)
	}
	wg.Wait()

	workerRespMap := make(map[string]*pb.CommonWorkerResponse, len(stCfgs))
	workers := make([]string, 0, len(stCfgs))
	for len(workerRespCh) > 0 {
		workerResp := <-workerRespCh
		workerRespMap[workerResp.Worker] = workerResp
		workers = append(workers, workerResp.Worker)
	}

	sort.Strings(workers)
	workerResps := make([]*pb.CommonWorkerResponse, 0, len(workers))
	for _, worker := range workers {
		workerResps = append(workerResps, workerRespMap[worker])
	}

	return &pb.UpdateTaskResponse{
		Result:  true,
		Workers: workerResps,
	}, nil
}

// QueryStatus implements MasterServer.QueryStatus
func (s *Server) QueryStatus(ctx context.Context, req *pb.QueryStatusListRequest) (*pb.QueryStatusListResponse, error) {
	log.L().Info("receive request and will save it to etcd", zap.Stringer("payload", req), zap.String("request", "QueryStatus"))

	responseErr := func(err error) *pb.QueryStatusListResponse {
		return &pb.QueryStatusListResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}
	}

	reqBytes, err := req.Marshal()
	if err != nil {
		return responseErr(err), nil
	}
	responseBytes, err := s.saveRequestAndWaitResponse(ctx, pb.OperateType_QueryStatus, reqBytes)
	if err != nil {
		return responseErr(err), nil
	}

	response := &pb.QueryStatusListResponse{}
	err = response.Unmarshal(responseBytes)
	if err != nil {
		return responseErr(err), nil
	}

	return response, nil
}

func (s *Server) queryStatus(ctx context.Context, req *pb.QueryStatusListRequest) (*pb.QueryStatusListResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "QueryStatus"))

	workers := make([]string, 0, len(s.workerClients))
	if len(req.Workers) > 0 {
		// query specified dm-workers
		invalidWorkers := make([]string, 0, len(req.Workers))
		for _, worker := range req.Workers {
			if _, ok := s.workerClients[worker]; !ok {
				invalidWorkers = append(invalidWorkers, worker)
			}
		}
		if len(invalidWorkers) > 0 {
			return &pb.QueryStatusListResponse{
				Result: false,
				Msg:    fmt.Sprintf("%s relevant worker-client not found", strings.Join(invalidWorkers, ", ")),
			}, nil
		}
		workers = req.Workers
	} else if len(req.Name) > 0 {
		// query specified task's workers
		workers = s.getTaskWorkers(req.Name)
		if len(workers) == 0 {
			return &pb.QueryStatusListResponse{
				Result: false,
				Msg:    fmt.Sprintf("task %s has no workers or not exist, can try `refresh-worker-tasks` cmd first", req.Name),
			}, nil
		}
	} else {
		// query all workers
		for worker := range s.workerClients {
			workers = append(workers, worker)
		}
	}

	workerRespCh := s.getStatusFromWorkers(ctx, workers, req.Name)

	workerRespMap := make(map[string]*pb.QueryStatusResponse, len(workers))
	for len(workerRespCh) > 0 {
		workerResp := <-workerRespCh
		workerRespMap[workerResp.Worker] = workerResp
	}

	sort.Strings(workers)
	workerResps := make([]*pb.QueryStatusResponse, 0, len(workers))
	for _, worker := range workers {
		workerResps = append(workerResps, workerRespMap[worker])
	}
	resp := &pb.QueryStatusListResponse{
		Result:  true,
		Workers: workerResps,
	}
	return resp, nil
}

// QueryError implements MasterServer.QueryError
func (s *Server) QueryError(ctx context.Context, req *pb.QueryErrorListRequest) (*pb.QueryErrorListResponse, error) {
	log.L().Info("receive request and save it to etcd", zap.Stringer("payload", req), zap.String("request", "QueryError"))

	responseErr := func(err error) *pb.QueryErrorListResponse {
		return &pb.QueryErrorListResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}
	}

	reqBytes, err := req.Marshal()
	if err != nil {
		return responseErr(err), nil
	}
	responseBytes, err := s.saveRequestAndWaitResponse(ctx, pb.OperateType_QueryError, reqBytes)
	if err != nil {
		return responseErr(err), nil
	}

	response := &pb.QueryErrorListResponse{}
	err = response.Unmarshal(responseBytes)
	if err != nil {
		return responseErr(err), nil
	}

	return response, nil
}

func (s *Server) queryError(ctx context.Context, req *pb.QueryErrorListRequest) (*pb.QueryErrorListResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "QueryError"))

	workers := make([]string, 0, len(s.workerClients))
	if len(req.Workers) > 0 {
		// query specified dm-workers
		invalidWorkers := make([]string, 0, len(req.Workers))
		for _, worker := range req.Workers {
			if _, ok := s.workerClients[worker]; !ok {
				invalidWorkers = append(invalidWorkers, worker)
			}
		}
		if len(invalidWorkers) > 0 {
			return &pb.QueryErrorListResponse{
				Result: false,
				Msg:    fmt.Sprintf("%s relevant worker-client not found", strings.Join(invalidWorkers, ", ")),
			}, nil
		}
		workers = req.Workers
	} else if len(req.Name) > 0 {
		// query specified task's workers
		workers = s.getTaskWorkers(req.Name)
		if len(workers) == 0 {
			return &pb.QueryErrorListResponse{
				Result: false,
				Msg:    fmt.Sprintf("task %s has no workers or not exist, can try `refresh-worker-tasks` cmd first", req.Name),
			}, nil
		}
	} else {
		// query all workers
		for worker := range s.workerClients {
			workers = append(workers, worker)
		}
	}

	workerRespCh := s.getErrorFromWorkers(ctx, workers, req.Name)

	workerRespMap := make(map[string]*pb.QueryErrorResponse, len(workers))
	for len(workerRespCh) > 0 {
		workerResp := <-workerRespCh
		workerRespMap[workerResp.Worker] = workerResp
	}

	sort.Strings(workers)
	workerResps := make([]*pb.QueryErrorResponse, 0, len(workers))
	for _, worker := range workers {
		workerResps = append(workerResps, workerRespMap[worker])
	}
	resp := &pb.QueryErrorListResponse{
		Result:  true,
		Workers: workerResps,
	}
	return resp, nil
}

// ShowDDLLocks implements MasterServer.ShowDDLLocks
func (s *Server) ShowDDLLocks(ctx context.Context, req *pb.ShowDDLLocksRequest) (*pb.ShowDDLLocksResponse, error) {
	log.L().Info("receive request and will save it to etcd", zap.Stringer("payload", req), zap.String("request", "ShowDDLLocks"))

	responseErr := func(err error) *pb.ShowDDLLocksResponse {
		return &pb.ShowDDLLocksResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}
	}

	reqBytes, err := req.Marshal()
	if err != nil {
		return responseErr(err), nil
	}
	responseBytes, err := s.saveRequestAndWaitResponse(ctx, pb.OperateType_ShowDDLLocks, reqBytes)
	if err != nil {
		return responseErr(err), nil
	}

	response := &pb.ShowDDLLocksResponse{}
	err = response.Unmarshal(responseBytes)
	if err != nil {
		return responseErr(err), nil
	}

	return response, nil
}

func (s *Server) showDDLLocks(ctx context.Context, req *pb.ShowDDLLocksRequest) (*pb.ShowDDLLocksResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "ShowDDLLocks"))

	resp := &pb.ShowDDLLocksResponse{
		Result: true,
	}

	locks := s.lockKeeper.Locks()
	resp.Locks = make([]*pb.DDLLock, 0, len(locks))
	for _, lock := range locks {
		if len(req.Task) > 0 && req.Task != lock.Task {
			continue // specify task and mismatch
		}
		ready := lock.Ready()
		if len(req.Workers) > 0 {
			for _, worker := range req.Workers {
				if _, ok := ready[worker]; ok {
					goto FOUND
				}
			}
			continue // specify workers and mismatch
		}
	FOUND:
		l := &pb.DDLLock{
			ID:       lock.ID,
			Task:     lock.Task,
			Owner:    lock.Owner,
			DDLs:     lock.Stmts,
			Synced:   make([]string, 0, len(ready)),
			Unsynced: make([]string, 0, len(ready)),
		}
		for worker, synced := range ready {
			if synced {
				l.Synced = append(l.Synced, worker)
			} else {
				l.Unsynced = append(l.Unsynced, worker)
			}
		}
		resp.Locks = append(resp.Locks, l)
	}

	if len(resp.Locks) == 0 {
		resp.Msg = "no DDL lock exists"
	}
	return resp, nil
}

// UnlockDDLLock implements MasterServer.UnlockDDLLock
func (s *Server) UnlockDDLLock(ctx context.Context, req *pb.UnlockDDLLockRequest) (*pb.UnlockDDLLockResponse, error) {
	log.L().Info("receive request and will save it to etcd", zap.String("lock ID", req.ID), zap.Stringer("payload", req), zap.String("request", "UnlockDDLLock"))

	responseErr := func(err error) *pb.UnlockDDLLockResponse {
		return &pb.UnlockDDLLockResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}
	}

	reqBytes, err := req.Marshal()
	if err != nil {
		return responseErr(err), nil
	}
	responseBytes, err := s.saveRequestAndWaitResponse(ctx, pb.OperateType_UnlockDDLLock, reqBytes)
	if err != nil {
		return responseErr(err), nil
	}

	response := &pb.UnlockDDLLockResponse{}
	err = response.Unmarshal(responseBytes)
	if err != nil {
		return responseErr(err), nil
	}

	return response, nil
}

func (s *Server) unlockDDLLock(ctx context.Context, req *pb.UnlockDDLLockRequest) (*pb.UnlockDDLLockResponse, error) {
	log.L().Info("", zap.String("lock ID", req.ID), zap.Stringer("payload", req), zap.String("request", "UnlockDDLLock"))

	workerResps, err := s.resolveDDLLock(ctx, req.ID, req.ReplaceOwner, req.Workers)
	resp := &pb.UnlockDDLLockResponse{
		Result:  true,
		Workers: workerResps,
	}
	if err != nil {
		resp.Result = false
		resp.Msg = errors.ErrorStack(err)
		log.L().Error("fail to unlock ddl", zap.String("ID", req.ID), zap.String("request", "UnlockDDLLock"), zap.Error(err))

		if req.ForceRemove {
			s.lockKeeper.RemoveLock(req.ID)
			log.L().Warn("force to remove DDL lock", zap.String("request", "UnlockDDLLock"), zap.String("ID", req.ID))
		}
	} else {
		log.L().Info("unlock ddl successfully", zap.String("ID", req.ID), zap.String("request", "UnlockDDLLock"))
	}

	return resp, nil
}

// BreakWorkerDDLLock implements MasterServer.BreakWorkerDDLLock
func (s *Server) BreakWorkerDDLLock(ctx context.Context, req *pb.BreakWorkerDDLLockRequest) (*pb.BreakWorkerDDLLockResponse, error) {
	log.L().Info("", zap.String("lock ID", req.RemoveLockID), zap.Stringer("payload", req), zap.String("request", "BreakWorkerDDLLock"))

	responseErr := func(err error) *pb.BreakWorkerDDLLockResponse {
		return &pb.BreakWorkerDDLLockResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}
	}

	reqBytes, err := req.Marshal()
	if err != nil {
		return responseErr(err), nil
	}
	responseBytes, err := s.saveRequestAndWaitResponse(ctx, pb.OperateType_BreakWorkerDDLLock, reqBytes)
	if err != nil {
		return responseErr(err), nil
	}

	response := &pb.BreakWorkerDDLLockResponse{}
	err = response.Unmarshal(responseBytes)
	if err != nil {
		return responseErr(err), nil
	}

	return response, nil
}

func (s *Server) breakWorkerDDLLock(ctx context.Context, req *pb.BreakWorkerDDLLockRequest) (*pb.BreakWorkerDDLLockResponse, error) {
	log.L().Info("", zap.String("lock ID", req.RemoveLockID), zap.Stringer("payload", req), zap.String("request", "BreakWorkerDDLLock"))

	request := &workerrpc.Request{
		Type: workerrpc.CmdBreakDDLLock,
		BreakDDLLock: &pb.BreakDDLLockRequest{
			Task:         req.Task,
			RemoveLockID: req.RemoveLockID,
			ExecDDL:      req.ExecDDL,
			SkipDDL:      req.SkipDDL,
		},
	}

	workerRespCh := make(chan *pb.CommonWorkerResponse, len(req.Workers))
	var wg sync.WaitGroup
	for _, worker := range req.Workers {
		wg.Add(1)
		go func(worker string) {
			defer wg.Done()
			cli, ok := s.workerClients[worker]
			if !ok {
				workerRespCh <- errorCommonWorkerResponse(fmt.Sprintf("worker %s relevant worker-client not found", worker), worker)
				return
			}
			resp, err := cli.SendRequest(ctx, request, s.cfg.RPCTimeout)
			workerResp := &pb.CommonWorkerResponse{}
			if err != nil {
				workerResp = errorCommonWorkerResponse(errors.ErrorStack(err), "")
			} else {
				workerResp = resp.BreakDDLLock
			}
			workerResp.Worker = worker
			workerRespCh <- workerResp
		}(worker)
	}
	wg.Wait()

	workerRespMap := make(map[string]*pb.CommonWorkerResponse, len(req.Workers))
	for len(workerRespCh) > 0 {
		workerResp := <-workerRespCh
		workerRespMap[workerResp.Worker] = workerResp
	}

	sort.Strings(req.Workers)
	workerResps := make([]*pb.CommonWorkerResponse, 0, len(req.Workers))
	for _, worker := range req.Workers {
		workerResps = append(workerResps, workerRespMap[worker])
	}

	return &pb.BreakWorkerDDLLockResponse{
		Result:  true,
		Workers: workerResps,
	}, nil
}

// HandleSQLs implements MasterServer.HandleSQLs
func (s *Server) HandleSQLs(ctx context.Context, req *pb.HandleSQLsRequest) (*pb.HandleSQLsResponse, error) {
	log.L().Info("", zap.String("task name", req.Name), zap.Stringer("payload", req), zap.String("request", "HandleSQLs"))

	responseErr := func(err error) *pb.HandleSQLsResponse {
		return &pb.HandleSQLsResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}
	}

	reqBytes, err := req.Marshal()
	if err != nil {
		return responseErr(err), nil
	}
	responseBytes, err := s.saveRequestAndWaitResponse(ctx, pb.OperateType_HandleSQLs, reqBytes)
	if err != nil {
		return responseErr(err), nil
	}

	response := &pb.HandleSQLsResponse{}
	err = response.Unmarshal(responseBytes)
	if err != nil {
		return responseErr(err), nil
	}

	return response, nil
}

func (s *Server) handleSQLs(ctx context.Context, req *pb.HandleSQLsRequest) (*pb.HandleSQLsResponse, error) {
	log.L().Info("", zap.String("task name", req.Name), zap.Stringer("payload", req), zap.String("request", "HandleSQLs"))

	// save request for --sharding operation
	if req.Sharding {
		err := s.sqlOperatorHolder.Set(req)
		if err != nil {
			return &pb.HandleSQLsResponse{
				Result: false,
				Msg:    fmt.Sprintf("save request with --sharding error:\n%s", errors.ErrorStack(err)),
			}, nil
		}
		log.L().Info("handle sqls request was saved", zap.String("task name", req.Name), zap.String("request", "HandleSQLs"))
		return &pb.HandleSQLsResponse{
			Result: true,
			Msg:    "request with --sharding saved and will be sent to DDL lock's owner when resolving DDL lock",
		}, nil
	}

	resp := &pb.HandleSQLsResponse{
		Result: false,
		Msg:    "",
	}

	if !s.checkTaskAndWorkerMatch(req.Name, req.Worker) {
		resp.Msg = fmt.Sprintf("task %s and worker %s not match, can try `refresh-worker-tasks` cmd first", req.Name, req.Worker)
		return resp, nil
	}

	// execute grpc call
	subReq := &workerrpc.Request{
		Type: workerrpc.CmdHandleSubTaskSQLs,
		HandleSubTaskSQLs: &pb.HandleSubTaskSQLsRequest{
			Name:       req.Name,
			Op:         req.Op,
			Args:       req.Args,
			BinlogPos:  req.BinlogPos,
			SqlPattern: req.SqlPattern,
		},
	}
	cli, ok := s.workerClients[req.Worker]
	if !ok {
		resp.Msg = fmt.Sprintf("worker %s client not found in %v", req.Worker, s.workerClients)
		return resp, nil
	}
	response, err := cli.SendRequest(ctx, subReq, s.cfg.RPCTimeout)
	workerResp := &pb.CommonWorkerResponse{}
	if err != nil {
		workerResp = errorCommonWorkerResponse(errors.ErrorStack(err), "")
	} else {
		workerResp = response.HandleSubTaskSQLs
	}
	resp.Workers = []*pb.CommonWorkerResponse{workerResp}
	resp.Result = true
	return resp, nil
}

// PurgeWorkerRelay implements MasterServer.PurgeWorkerRelay
func (s *Server) PurgeWorkerRelay(ctx context.Context, req *pb.PurgeWorkerRelayRequest) (*pb.PurgeWorkerRelayResponse, error) {
	log.L().Info("receive request and save it to etcd", zap.Stringer("payload", req), zap.String("request", "PurgeWorkerRelay"))

	responseErr := func(err error) *pb.PurgeWorkerRelayResponse {
		return &pb.PurgeWorkerRelayResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}
	}

	reqBytes, err := req.Marshal()
	if err != nil {
		return responseErr(err), nil
	}
	responseBytes, err := s.saveRequestAndWaitResponse(ctx, pb.OperateType_PurgeWorkerRelay, reqBytes)
	if err != nil {
		return responseErr(err), nil
	}

	response := &pb.PurgeWorkerRelayResponse{}
	err = response.Unmarshal(responseBytes)
	if err != nil {
		return responseErr(err), nil
	}

	return response, nil
}

func (s *Server) purgeWorkerRelay(ctx context.Context, req *pb.PurgeWorkerRelayRequest) (*pb.PurgeWorkerRelayResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "PurgeWorkerRelay"))

	workerReq := &workerrpc.Request{
		Type: workerrpc.CmdPurgeRelay,
		PurgeRelay: &pb.PurgeRelayRequest{
			Inactive: req.Inactive,
			Time:     req.Time,
			Filename: req.Filename,
			SubDir:   req.SubDir,
		},
	}

	workerRespCh := make(chan *pb.CommonWorkerResponse, len(req.Workers))
	var wg sync.WaitGroup
	for _, worker := range req.Workers {
		wg.Add(1)
		go func(worker string) {
			defer wg.Done()
			cli, ok := s.workerClients[worker]
			if !ok {
				workerRespCh <- errorCommonWorkerResponse(fmt.Sprintf("worker %s relevant worker-client not found", worker), worker)
				return
			}
			resp, err := cli.SendRequest(ctx, workerReq, s.cfg.RPCTimeout)
			workerResp := &pb.CommonWorkerResponse{}
			if err != nil {
				workerResp = errorCommonWorkerResponse(errors.ErrorStack(err), "")
			} else {
				workerResp = resp.PurgeRelay
			}
			workerResp.Worker = worker
			workerRespCh <- workerResp
		}(worker)
	}
	wg.Wait()

	workerRespMap := make(map[string]*pb.CommonWorkerResponse, len(req.Workers))
	for len(workerRespCh) > 0 {
		workerResp := <-workerRespCh
		workerRespMap[workerResp.Worker] = workerResp
	}

	sort.Strings(req.Workers)
	workerResps := make([]*pb.CommonWorkerResponse, 0, len(req.Workers))
	for _, worker := range req.Workers {
		workerResps = append(workerResps, workerRespMap[worker])
	}

	return &pb.PurgeWorkerRelayResponse{
		Result:  true,
		Workers: workerResps,
	}, nil
}

// SwitchWorkerRelayMaster implements MasterServer.SwitchWorkerRelayMaster
func (s *Server) SwitchWorkerRelayMaster(ctx context.Context, req *pb.SwitchWorkerRelayMasterRequest) (*pb.SwitchWorkerRelayMasterResponse, error) {
	log.L().Info("receive request and will save it to etcd", zap.Stringer("payload", req), zap.String("request", "SwitchWorkerRelayMaster"))

	responseErr := func(err error) *pb.SwitchWorkerRelayMasterResponse {
		return &pb.SwitchWorkerRelayMasterResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}
	}

	reqBytes, err := req.Marshal()
	if err != nil {
		return responseErr(err), nil
	}
	responseBytes, err := s.saveRequestAndWaitResponse(ctx, pb.OperateType_SwitchWorkerRelayMaster, reqBytes)
	if err != nil {
		return responseErr(err), nil
	}

	response := &pb.SwitchWorkerRelayMasterResponse{}
	err = response.Unmarshal(responseBytes)
	if err != nil {
		return responseErr(err), nil
	}

	return response, nil
}

func (s *Server) switchWorkerRelayMaster(ctx context.Context, req *pb.SwitchWorkerRelayMasterRequest) (*pb.SwitchWorkerRelayMasterResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "SwitchWorkerRelayMaster"))

	workerRespCh := make(chan *pb.CommonWorkerResponse, len(req.Workers))

	handleErr := func(err error, worker string) {
		log.L().Error("response error", zap.Error(err))
		resp := &pb.CommonWorkerResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
			Worker: worker,
		}
		workerRespCh <- resp
	}

	var wg sync.WaitGroup
	for _, worker := range req.Workers {
		wg.Add(1)
		go s.ap.Emit(ctx, 0, func(args ...interface{}) {
			defer wg.Done()
			cli, worker1, err := s.workerArgsExtractor(args...)
			if err != nil {
				handleErr(err, worker1)
				return
			}
			request := &workerrpc.Request{
				Type:              workerrpc.CmdSwitchRelayMaster,
				SwitchRelayMaster: &pb.SwitchRelayMasterRequest{},
			}
			resp, err := cli.SendRequest(ctx, request, s.cfg.RPCTimeout)
			workerResp := &pb.CommonWorkerResponse{}
			if err != nil {
				workerResp = errorCommonWorkerResponse(errors.ErrorStack(err), "")
			} else {
				workerResp = resp.SwitchRelayMaster
			}
			workerResp.Worker = worker1
			workerRespCh <- workerResp
		}, func(args ...interface{}) {
			defer wg.Done()
			_, worker1, err := s.workerArgsExtractor(args...)
			if err != nil {
				handleErr(err, worker1)
				return
			}
			workerRespCh <- errorCommonWorkerResponse(terror.ErrMasterNoEmitToken.Generate(worker1).Error(), worker1)
		}, worker)
	}
	wg.Wait()

	workerRespMap := make(map[string]*pb.CommonWorkerResponse, len(req.Workers))
	for len(workerRespCh) > 0 {
		workerResp := <-workerRespCh
		workerRespMap[workerResp.Worker] = workerResp
	}

	sort.Strings(req.Workers)
	workerResps := make([]*pb.CommonWorkerResponse, 0, len(req.Workers))
	for _, worker := range req.Workers {
		workerResps = append(workerResps, workerRespMap[worker])
	}

	return &pb.SwitchWorkerRelayMasterResponse{
		Result:  true,
		Workers: workerResps,
	}, nil
}

// OperateWorkerRelayTask implements MasterServer.OperateWorkerRelayTask
func (s *Server) OperateWorkerRelayTask(ctx context.Context, req *pb.OperateWorkerRelayRequest) (*pb.OperateWorkerRelayResponse, error) {
	log.L().Info("receive request and will save it to etcd", zap.Stringer("payload", req), zap.String("request", "OperateWorkerRelayTask"))

	responseErr := func(err error) *pb.OperateWorkerRelayResponse {
		return &pb.OperateWorkerRelayResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}
	}

	reqBytes, err := req.Marshal()
	if err != nil {
		return responseErr(err), nil
	}
	responseBytes, err := s.saveRequestAndWaitResponse(ctx, pb.OperateType_OperateWorkerRelay, reqBytes)
	if err != nil {
		return responseErr(err), nil
	}

	response := &pb.OperateWorkerRelayResponse{}
	err = response.Unmarshal(responseBytes)
	if err != nil {
		return responseErr(err), nil
	}

	return response, nil
}

func (s *Server) operateWorkerRelayTask(ctx context.Context, req *pb.OperateWorkerRelayRequest) (*pb.OperateWorkerRelayResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "OperateWorkerRelayTask"))

	request := &workerrpc.Request{
		Type:         workerrpc.CmdOperateRelay,
		OperateRelay: &pb.OperateRelayRequest{Op: req.Op},
	}
	workerRespCh := make(chan *pb.OperateRelayResponse, len(req.Workers))
	var wg sync.WaitGroup
	for _, worker := range req.Workers {
		wg.Add(1)
		go func(worker string) {
			defer wg.Done()
			cli, ok := s.workerClients[worker]
			if !ok {
				workerResp := &pb.OperateRelayResponse{
					Op:     req.Op,
					Result: false,
					Worker: worker,
					Msg:    fmt.Sprintf("%s relevant worker-client not found", worker),
				}
				workerRespCh <- workerResp
				return
			}
			resp, err := cli.SendRequest(ctx, request, s.cfg.RPCTimeout)
			workerResp := &pb.OperateRelayResponse{}
			if err != nil {
				workerResp = &pb.OperateRelayResponse{
					Result: false,
					Msg:    errors.ErrorStack(err),
				}
			} else {
				workerResp = resp.OperateRelay
			}
			workerResp.Op = req.Op
			workerResp.Worker = worker
			workerRespCh <- workerResp
		}(worker)
	}
	wg.Wait()

	workerRespMap := make(map[string]*pb.OperateRelayResponse, len(req.Workers))
	for len(workerRespCh) > 0 {
		workerResp := <-workerRespCh
		workerRespMap[workerResp.Worker] = workerResp
	}

	sort.Strings(req.Workers)
	workerResps := make([]*pb.OperateRelayResponse, 0, len(req.Workers))
	for _, worker := range req.Workers {
		workerResps = append(workerResps, workerRespMap[worker])
	}

	return &pb.OperateWorkerRelayResponse{
		Result:  true,
		Workers: workerResps,
	}, nil
}

// RefreshWorkerTasks implements MasterServer.RefreshWorkerTasks
func (s *Server) RefreshWorkerTasks(ctx context.Context, req *pb.RefreshWorkerTasksRequest) (*pb.RefreshWorkerTasksResponse, error) {
	log.L().Info("receive request and will save it to etcd", zap.Stringer("payload", req), zap.String("request", "RefreshWorkerTasks"))

	responseErr := func(err error) *pb.RefreshWorkerTasksResponse {
		return &pb.RefreshWorkerTasksResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}
	}

	reqBytes, err := req.Marshal()
	if err != nil {
		return responseErr(err), nil
	}
	responseBytes, err := s.saveRequestAndWaitResponse(ctx, pb.OperateType_RefreshWorkerTasks, reqBytes)
	if err != nil {
		return responseErr(err), nil
	}

	response := &pb.RefreshWorkerTasksResponse{}
	err = response.Unmarshal(responseBytes)
	if err != nil {
		return responseErr(err), nil
	}

	return response, nil
}

func (s *Server) refreshWorkerTasks(ctx context.Context, req *pb.RefreshWorkerTasksRequest) (*pb.RefreshWorkerTasksResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "RefreshWorkerTasks"))

	taskWorkers, workerMsgMap := s.fetchTaskWorkers(ctx)
	// always update task workers mapping in memory
	s.replaceTaskWorkers(taskWorkers)
	log.L().Info("refresh workers of task", zap.Reflect("workers of task", taskWorkers), zap.String("request", "RefreshWorkerTasks"))

	workers := make([]string, 0, len(workerMsgMap))
	for worker := range workerMsgMap {
		workers = append(workers, worker)
	}
	sort.Strings(workers)

	workerMsgs := make([]*pb.RefreshWorkerTasksMsg, 0, len(workers))
	for _, worker := range workers {
		workerMsgs = append(workerMsgs, &pb.RefreshWorkerTasksMsg{
			Worker: worker,
			Msg:    workerMsgMap[worker],
		})
	}
	return &pb.RefreshWorkerTasksResponse{
		Result:  true,
		Workers: workerMsgs,
	}, nil
}

// addTaskWorkers adds a task-workers pair
// replace indicates whether replace old workers
func (s *Server) addTaskWorkers(task string, workers []string, replace bool) {
	if len(workers) == 0 {
		return
	}

	valid := make([]string, 0, len(workers))
	for _, worker := range workers {
		if _, ok := s.workerClients[worker]; ok {
			valid = append(valid, worker)
		}
	}

	s.Lock()
	defer s.Unlock()
	if !replace {
		// merge with old workers
		old, ok := s.taskWorkers[task]
		if ok {
			exist := make(map[string]struct{})
			for _, worker := range valid {
				exist[worker] = struct{}{}
			}
			for _, worker := range old {
				if _, ok := exist[worker]; !ok {
					valid = append(valid, worker)
				}
			}
		}
	}

	sort.Strings(valid)
	s.taskWorkers[task] = valid
	log.L().Info("update workers of task", zap.String("task", task), zap.Strings("workers", valid))
}

// replaceTaskWorkers replaces the whole task-workers mapper
func (s *Server) replaceTaskWorkers(taskWorkers map[string][]string) {
	for task := range taskWorkers {
		sort.Strings(taskWorkers[task])
	}
	s.Lock()
	defer s.Unlock()
	s.taskWorkers = taskWorkers
}

// removeTaskWorkers remove (partial / all) workers for a task
func (s *Server) removeTaskWorkers(task string, workers []string) {
	toRemove := make(map[string]struct{})
	for _, w := range workers {
		toRemove[w] = struct{}{}
	}

	s.Lock()
	defer s.Unlock()
	if _, ok := s.taskWorkers[task]; !ok {
		log.L().Warn("not found workers", zap.String("task", task))
		return
	}
	remain := make([]string, 0, len(s.taskWorkers[task]))
	for _, worker := range s.taskWorkers[task] {
		if _, ok := toRemove[worker]; !ok {
			remain = append(remain, worker)
		}
	}
	if len(remain) == 0 {
		delete(s.taskWorkers, task)
		log.L().Info("remove task from taskWorker", zap.String("task", task))
	} else {
		s.taskWorkers[task] = remain
		log.L().Info("update workers of task", zap.String("task", task), zap.Strings("reamin workers", remain))
	}
}

// getTaskWorkers gets workers relevant to specified task
func (s *Server) getTaskWorkers(task string) []string {
	s.Lock()
	defer s.Unlock()
	workers, ok := s.taskWorkers[task]
	if !ok {
		return []string{}
	}
	// do a copy
	ret := make([]string, 0, len(workers))
	ret = append(ret, workers...)
	return ret
}

// containWorker checks whether worker in workers
func (s *Server) containWorker(workers []string, worker string) bool {
	for _, w := range workers {
		if w == worker {
			return true
		}
	}
	return false
}

// getStatusFromWorkers does RPC request to get status from dm-workers
func (s *Server) getStatusFromWorkers(ctx context.Context, workers []string, taskName string) chan *pb.QueryStatusResponse {
	workerReq := &workerrpc.Request{
		Type:        workerrpc.CmdQueryStatus,
		QueryStatus: &pb.QueryStatusRequest{Name: taskName},
	}
	workerRespCh := make(chan *pb.QueryStatusResponse, len(workers))

	handleErr := func(err error, worker string) bool {
		log.L().Error("response error", zap.Error(err))
		resp := &pb.QueryStatusResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
			Worker: worker,
		}
		workerRespCh <- resp
		return false
	}

	var wg sync.WaitGroup
	for _, worker := range workers {
		wg.Add(1)
		go s.ap.Emit(ctx, 0, func(args ...interface{}) {
			defer wg.Done()
			cli, worker1, err := s.workerArgsExtractor(args...)
			if err != nil {
				handleErr(err, worker1)
				return
			}
			resp, err := cli.SendRequest(ctx, workerReq, s.cfg.RPCTimeout)
			workerStatus := &pb.QueryStatusResponse{}
			if err != nil {
				workerStatus = &pb.QueryStatusResponse{
					Result: false,
					Msg:    errors.ErrorStack(err),
				}
			} else {
				workerStatus = resp.QueryStatus
			}
			workerStatus.Worker = worker1
			workerRespCh <- workerStatus
		}, func(args ...interface{}) {
			defer wg.Done()
			_, worker1, err := s.workerArgsExtractor(args...)
			if err != nil {
				handleErr(err, worker1)
				return
			}
			handleErr(terror.ErrMasterNoEmitToken.Generate(worker1), worker1)
		}, worker)
	}
	wg.Wait()
	return workerRespCh
}

// getErrorFromWorkers does RPC request to get error information from dm-workers
func (s *Server) getErrorFromWorkers(ctx context.Context, workers []string, taskName string) chan *pb.QueryErrorResponse {
	workerReq := &workerrpc.Request{
		Type:       workerrpc.CmdQueryError,
		QueryError: &pb.QueryErrorRequest{Name: taskName},
	}
	workerRespCh := make(chan *pb.QueryErrorResponse, len(workers))

	handleErr := func(err error, worker string) bool {
		log.L().Error("response error", zap.Error(err))
		resp := &pb.QueryErrorResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
			Worker: worker,
		}
		workerRespCh <- resp
		return false
	}

	var wg sync.WaitGroup
	for _, worker := range workers {
		wg.Add(1)
		go s.ap.Emit(ctx, 0, func(args ...interface{}) {
			defer wg.Done()
			cli, worker1, err := s.workerArgsExtractor(args...)
			if err != nil {
				handleErr(err, worker1)
				return
			}
			resp, err := cli.SendRequest(ctx, workerReq, s.cfg.RPCTimeout)
			workerError := &pb.QueryErrorResponse{}
			if err != nil {
				workerError = &pb.QueryErrorResponse{
					Result: false,
					Msg:    errors.ErrorStack(err),
				}
			} else {
				workerError = resp.QueryError
			}
			workerError.Worker = worker1
			workerRespCh <- workerError
		}, func(args ...interface{}) {
			defer wg.Done()
			_, worker1, err := s.workerArgsExtractor(args...)
			if err != nil {
				handleErr(err, worker1)
				return
			}
			handleErr(terror.ErrMasterNoEmitToken.Generate(worker1), worker1)
		}, worker)
	}
	wg.Wait()
	return workerRespCh
}

// updateTaskWorkers fetches task-workers mapper from dm-workers and update s.taskWorkers
func (s *Server) updateTaskWorkers(ctx context.Context) {
	taskWorkers, _ := s.fetchTaskWorkers(ctx)
	if len(taskWorkers) == 0 {
		return // keep the old
	}
	// simple replace, maybe we can do more accurate update later
	s.replaceTaskWorkers(taskWorkers)
	log.L().Info("update workers of task", zap.Reflect("workers", taskWorkers))
}

// fetchTaskWorkers fetches task-workers mapper from workers based on deployment
func (s *Server) fetchTaskWorkers(ctx context.Context) (map[string][]string, map[string]string) {
	workers := make([]string, 0, len(s.workerClients))
	for worker := range s.workerClients {
		workers = append(workers, worker)
	}

	workerRespCh := s.getStatusFromWorkers(ctx, workers, "")

	taskWorkerMap := make(map[string][]string)
	workerMsgMap := make(map[string]string)
	for len(workerRespCh) > 0 {
		workerResp := <-workerRespCh
		worker := workerResp.Worker
		if len(workerResp.Msg) > 0 {
			workerMsgMap[worker] = workerResp.Msg
		} else if !workerResp.Result {
			workerMsgMap[worker] = "got response but with failed result"
		}
		if workerResp.SubTaskStatus == nil {
			continue
		}
		for _, status := range workerResp.SubTaskStatus {
			if status.Stage == pb.Stage_InvalidStage {
				continue // invalid status
			}
			task := status.Name
			_, ok := taskWorkerMap[task]
			if !ok {
				taskWorkerMap[task] = make([]string, 0, 10)
			}
			taskWorkerMap[task] = append(taskWorkerMap[task], worker)
		}
	}

	return taskWorkerMap, workerMsgMap
}

// return true means match, false means mismatch.
func (s *Server) checkTaskAndWorkerMatch(taskname string, targetWorker string) bool {
	// find worker
	workers := s.getTaskWorkers(taskname)
	if len(workers) == 0 {
		return false
	}
	for _, worker := range workers {
		if worker == targetWorker {
			return true
		}
	}
	return false
}

// fetchWorkerDDLInfo fetches DDL info from all dm-workers
// and sends DDL lock info back to dm-workers
func (s *Server) fetchWorkerDDLInfo(ctx context.Context) {
	var wg sync.WaitGroup

	request := &workerrpc.Request{Type: workerrpc.CmdFetchDDLInfo}
	for worker, client := range s.workerClients {
		wg.Add(1)
		go func(worker string, cli workerrpc.Client) {
			defer wg.Done()
			var doRetry bool

			for {
				if doRetry {
					select {
					case <-ctx.Done():
						return
					case <-time.After(fetchDDLInfoRetryTimeout):
					}
				}
				doRetry = false // reset

				select {
				case <-ctx.Done():
					return
				default:
					resp, err := cli.SendRequest(ctx, request, s.cfg.RPCTimeout)
					if err != nil {
						log.L().Error("create FetchDDLInfo stream", zap.String("worker", worker), log.ShortError(err))
						doRetry = true
						continue
					}
					stream := resp.FetchDDLInfo
					for {
						in, err := stream.Recv()
						if err == io.EOF {
							doRetry = true
							break
						}
						select {
						case <-ctx.Done(): // check whether canceled again
							return
						default:
						}
						if err != nil {
							log.L().Error("receive ddl info", zap.String("worker", worker), log.ShortError(err))
							doRetry = true
							break
						}
						log.L().Info("receive ddl info", zap.Stringer("ddl info", in), zap.String("worker", worker))

						workers := s.getTaskWorkers(in.Task)
						if len(workers) == 0 {
							// should happen only when starting and before updateTaskWorkers return
							log.L().Error("try to sync shard DDL, but with no workers", zap.String("task", in.Task))
							doRetry = true
							break
						}
						if !s.containWorker(workers, worker) {
							// should not happen
							log.L().Error("try to sync shard DDL, but worker is not in workers", zap.String("task", in.Task), zap.String("worker", worker), zap.Strings("workers", workers))
							doRetry = true
							break
						}

						lockID, synced, remain, err := s.lockKeeper.TrySync(in.Task, in.Schema, in.Table, worker, in.DDLs, workers)
						if err != nil {
							log.L().Error("fail to sync lock", zap.String("worker", worker), log.ShortError(err))
							doRetry = true
							break
						}

						out := &pb.DDLLockInfo{
							Task: in.Task,
							ID:   lockID,
						}
						err = stream.Send(out)
						if err != nil {
							log.L().Error("fail to send ddl lock info", zap.Stringer("ddl lock info", out), zap.String("worker", worker), log.ShortError(err))
							doRetry = true
							break
						}

						if !synced {
							// still need wait other workers to sync
							log.L().Info("shard DDL is in syncing", zap.String("lock ID", lockID), zap.Int("remain worker count", remain))
							continue
						}

						log.L().Info("shard DDL was synced", zap.String("lock ID", lockID))

						// resolve DDL lock
						wg.Add(1)
						go func(lockID string) {
							defer wg.Done()
							resps, err := s.resolveDDLLock(ctx, lockID, "", nil)
							if err == nil {
								log.L().Info("resolve DDL lock successfully", zap.String("lock ID", lockID))
							} else {
								log.L().Error("fail to resolve DDL lock", zap.String("lock ID", lockID), zap.Reflect("responses", resps), zap.Error(err))
								lock := s.lockKeeper.FindLock(lockID)
								if lock != nil {
									lock.AutoRetry.Set(true) // need auto-retry resolve at intervals
								}
							}
						}(lockID)
					}
					stream.CloseSend()
				}
			}

		}(worker, client)
	}

	wg.Wait()
}

// resolveDDLLock resolves DDL lock
// requests DDL lock's owner to execute the DDL
// requests DDL lock's non-owner dm-workers to ignore (skip) the DDL
func (s *Server) resolveDDLLock(ctx context.Context, lockID string, replaceOwner string, prefWorkers []string) ([]*pb.CommonWorkerResponse, error) {
	lock := s.lockKeeper.FindLock(lockID)
	if lock == nil {
		// should not happen even when dm-master restarted
		return nil, terror.ErrMasterLockNotFound.Generate(lockID)
	}

	if lock.Resolving.Get() {
		return nil, terror.ErrMasterLockIsResolving.Generate(lockID)
	}
	lock.Resolving.Set(true)
	defer lock.Resolving.Set(false) //reset

	ready := lock.Ready() // Ready contain all dm-workers and whether they were synced

	// request the owner to execute DDL
	owner := lock.Owner
	if len(replaceOwner) > 0 {
		owner = replaceOwner
	}
	cli, ok := s.workerClients[owner]
	if !ok {
		return nil, terror.ErrMasterWorkerCliNotFound.Generate(owner)
	}
	if _, ok := ready[owner]; !ok {
		return nil, terror.ErrMasterWorkerNotWaitLock.Generate(owner, lockID)
	}

	// try send handle SQLs request to owner if exists
	key, oper := s.sqlOperatorHolder.Get(lock.Task, lock.DDLs())
	if oper != nil {
		ownerReq := &workerrpc.Request{
			Type: workerrpc.CmdHandleSubTaskSQLs,
			HandleSubTaskSQLs: &pb.HandleSubTaskSQLsRequest{
				Name:       oper.Req.Name,
				Op:         oper.Req.Op,
				Args:       oper.Req.Args,
				BinlogPos:  oper.Req.BinlogPos,
				SqlPattern: oper.Req.SqlPattern,
			},
		}
		resp, err := cli.SendRequest(ctx, ownerReq, s.cfg.RPCTimeout)
		if err != nil {
			return nil, terror.Annotatef(err, "send handle SQLs request %s to DDL lock %s owner %s fail", ownerReq.HandleSubTaskSQLs, lockID, owner)
		}
		ownerResp := resp.HandleSubTaskSQLs
		if !ownerResp.Result {
			return nil, terror.ErrMasterHandleSQLReqFail.Generate(lockID, owner, ownerReq.HandleSubTaskSQLs, ownerResp.Msg)
		}
		log.L().Info("sent handle --sharding DDL request", zap.Stringer("payload", ownerReq.HandleSubTaskSQLs), zap.String("owner", owner), zap.String("lock ID", lockID))
		s.sqlOperatorHolder.Remove(lock.Task, key) // remove SQL operator after sent to owner
	}

	// If send ExecuteDDL request failed, we will generate more tracer group id,
	// this is acceptable if each ExecuteDDL request successes at last.
	// TODO: we need a better way to combine brain split tracing events into one
	// single group.
	traceGID := s.idGen.NextID("resolveDDLLock", 0)
	log.L().Info("requesting to execute DDL", zap.String("owner", owner), zap.String("lock ID", lockID))
	ownerReq := &workerrpc.Request{
		Type: workerrpc.CmdExecDDL,
		ExecDDL: &pb.ExecDDLRequest{
			Task:     lock.Task,
			LockID:   lockID,
			Exec:     true,
			TraceGID: traceGID,
			DDLs:     lock.ddls,
		},
	}
	// use a longer timeout for executing DDL in DM-worker.
	// now, we ignore `invalid connection` for `ADD INDEX`, use a longer timout to ensure the DDL lock removed.
	ownerTimeout := time.Duration(syncer.MaxDDLConnectionTimeoutMinute)*time.Minute + 30*time.Second
	resp, err := cli.SendRequest(ctx, ownerReq, ownerTimeout)
	ownerResp := &pb.CommonWorkerResponse{}
	if err != nil {
		ownerResp = errorCommonWorkerResponse(errors.ErrorStack(err), "")
	} else {
		ownerResp = resp.ExecDDL
	}
	ownerResp.Worker = owner
	if !ownerResp.Result {
		// owner execute DDL fail, do not continue
		return []*pb.CommonWorkerResponse{
			ownerResp,
		}, terror.ErrMasterOwnerExecDDL.Generate(owner)
	}

	// request other dm-workers to ignore DDL
	workers := make([]string, 0, len(ready))
	if len(prefWorkers) > 0 {
		workers = prefWorkers
	} else {
		for worker := range ready {
			workers = append(workers, worker)
		}
	}

	request := &workerrpc.Request{
		Type: workerrpc.CmdExecDDL,
		ExecDDL: &pb.ExecDDLRequest{
			Task:     lock.Task,
			LockID:   lockID,
			Exec:     false, // ignore and skip DDL
			TraceGID: traceGID,
			DDLs:     lock.ddls,
		},
	}
	workerRespCh := make(chan *pb.CommonWorkerResponse, len(workers))
	var wg sync.WaitGroup
	for _, worker := range workers {
		if worker == owner {
			continue // owner has executed DDL
		}

		wg.Add(1)
		go func(worker string) {
			defer wg.Done()
			cli, ok := s.workerClients[worker]
			if !ok {
				workerRespCh <- errorCommonWorkerResponse(fmt.Sprintf("worker %s relevant worker-client not found", worker), worker)
				return
			}
			if _, ok := ready[worker]; !ok {
				workerRespCh <- errorCommonWorkerResponse(fmt.Sprintf("worker %s not waiting for DDL lock %s", owner, lockID), worker)
				return
			}

			log.L().Info("request to skip DDL", zap.String("not owner worker", worker), zap.String("lock ID", lockID))
			resp, err2 := cli.SendRequest(ctx, request, s.cfg.RPCTimeout)
			var workerResp *pb.CommonWorkerResponse
			if err2 != nil {
				workerResp = errorCommonWorkerResponse(errors.ErrorStack(err2), "")
			} else {
				workerResp = resp.ExecDDL
			}
			workerResp.Worker = worker
			workerRespCh <- workerResp
		}(worker)
	}
	wg.Wait()

	workerRespMap := make(map[string]*pb.CommonWorkerResponse, len(workers))
	var success = true
	for len(workerRespCh) > 0 {
		workerResp := <-workerRespCh
		workerRespMap[workerResp.Worker] = workerResp
		if !workerResp.Result {
			success = false
		}
	}

	sort.Strings(workers)
	workerResps := make([]*pb.CommonWorkerResponse, 0, len(workers)+1)
	workerResps = append(workerResps, ownerResp)
	for _, worker := range workers {
		workerResp, ok := workerRespMap[worker]
		if ok {
			workerResps = append(workerResps, workerResp)
		}
	}

	// owner has ExecuteDDL successfully, we remove the Lock
	// if some dm-workers ExecuteDDL occurred error, we should use dmctl to handle dm-worker directly
	s.lockKeeper.RemoveLock(lockID)

	if !success {
		err = terror.ErrMasterPartWorkerExecDDLFail.Generate(lockID)
	}
	return workerResps, err
}

// UpdateMasterConfig implements MasterServer.UpdateConfig
func (s *Server) UpdateMasterConfig(ctx context.Context, req *pb.UpdateMasterConfigRequest) (*pb.UpdateMasterConfigResponse, error) {
	log.L().Info("receive request and save it to etcd", zap.Stringer("payload", req), zap.String("request", "UpdateMasterConfig"))

	responseErr := func(err error) *pb.UpdateMasterConfigResponse {
		return &pb.UpdateMasterConfigResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}
	}

	reqBytes, err := req.Marshal()
	if err != nil {
		return responseErr(err), nil
	}
	responseBytes, err := s.saveRequestAndWaitResponse(ctx, pb.OperateType_UpdateMasterConfig, reqBytes)
	if err != nil {
		return responseErr(err), nil
	}

	response := &pb.UpdateMasterConfigResponse{}
	err = response.Unmarshal(responseBytes)
	if err != nil {
		return responseErr(err), nil
	}

	return response, nil
}

func (s *Server) updateMasterConfig(ctx context.Context, req *pb.UpdateMasterConfigRequest) (*pb.UpdateMasterConfigResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "UpdateMasterConfig"))
	s.Lock()

	err := s.cfg.UpdateConfigFile(req.Config)
	if err != nil {
		s.Unlock()
		return &pb.UpdateMasterConfigResponse{
			Result: false,
			Msg:    "Failed to write config to local file. detail: " + errors.ErrorStack(err),
		}, nil
	}
	log.L().Info("saved dm-master config file", zap.String("config file", s.cfg.ConfigFile), zap.String("request", "UpdateMasterConfig"))

	cfg := NewConfig()
	cfg.ConfigFile = s.cfg.ConfigFile
	err = cfg.Reload()
	if err != nil {
		s.Unlock()
		return &pb.UpdateMasterConfigResponse{
			Result: false,
			Msg:    fmt.Sprintf("Failed to parse configure from file %s, detail: ", cfg.ConfigFile) + errors.ErrorStack(err),
		}, nil
	}
	log.L().Info("update dm-master config", zap.Stringer("config", cfg), zap.String("request", "UpdateMasterConfig"))

	// delete worker
	wokerList := make([]string, 0, len(s.cfg.DeployMap))
	for k, workerAddr := range s.cfg.DeployMap {
		if _, ok := cfg.DeployMap[k]; !ok {
			wokerList = append(wokerList, workerAddr)
			DDLreq := &pb.ShowDDLLocksRequest{
				Task:    "",
				Workers: wokerList,
			}
			resp, err2 := s.ShowDDLLocks(ctx, DDLreq)
			if err2 != nil {
				s.Unlock()
				return &pb.UpdateMasterConfigResponse{
					Result: false,
					Msg:    fmt.Sprintf("Failed to get DDL lock Info from %s, detail: ", workerAddr) + errors.ErrorStack(err2),
				}, nil
			}
			if len(resp.Locks) != 0 {
				err = terror.ErrMasterWorkerExistDDLLock.Generate(workerAddr)
				s.Unlock()
				return &pb.UpdateMasterConfigResponse{
					Result: false,
					Msg:    errors.ErrorStack(err),
				}, nil
			}
		}
	}
	for i := 0; i < len(wokerList); i++ {
		delete(s.workerClients, wokerList[i])
	}

	// add new worker
	for _, workerAddr := range cfg.DeployMap {
		if _, ok := s.workerClients[workerAddr]; !ok {
			cli, err2 := workerrpc.NewGRPCClient(workerAddr)
			if err2 != nil {
				s.Unlock()
				return &pb.UpdateMasterConfigResponse{
					Result: false,
					Msg:    fmt.Sprintf("Failed to add woker %s, detail: ", workerAddr) + errors.ErrorStack(err2),
				}, nil
			}
			s.workerClients[workerAddr] = cli
		}
	}

	// update log configure. not supported now
	/*log.SetLevelByString(strings.ToLower(cfg.LogLevel))
	if len(cfg.LogFile) > 0 {
		log.SetOutputByName(cfg.LogFile)
	}*/

	s.cfg = cfg
	log.L().Info("update dm-master config file success", zap.String("request", "UpdateMasterConfig"))
	s.Unlock()

	workers := make([]string, 0, len(s.workerClients))
	for worker := range s.workerClients {
		workers = append(workers, worker)
	}

	workerRespCh := s.getStatusFromWorkers(ctx, workers, "")
	log.L().Info("checking every dm-worker status...", zap.String("request", "UpdateMasterConfig"))

	workerRespMap := make(map[string]*pb.QueryStatusResponse, len(workers))
	for len(workerRespCh) > 0 {
		workerResp := <-workerRespCh
		workerRespMap[workerResp.Worker] = workerResp
	}

	sort.Strings(workers)
	workerResps := make([]*pb.QueryStatusResponse, 0, len(workers))
	for _, worker := range workers {
		workerResps = append(workerResps, workerRespMap[worker])
	}

	return &pb.UpdateMasterConfigResponse{
		Result:  true,
		Msg:     "",
		Workers: workerResps,
	}, nil
}

// UpdateWorkerRelayConfig updates config for relay and (dm-worker)
func (s *Server) UpdateWorkerRelayConfig(ctx context.Context, req *pb.UpdateWorkerRelayConfigRequest) (*pb.CommonWorkerResponse, error) {
	log.L().Info("receive request and will save it to etcd", zap.Stringer("payload", req), zap.String("request", "UpdateWorkerRelayConfig"))

	responseErr := func(err error) *pb.CommonWorkerResponse {
		return &pb.CommonWorkerResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}
	}

	reqBytes, err := req.Marshal()
	if err != nil {
		return responseErr(err), nil
	}
	responseBytes, err := s.saveRequestAndWaitResponse(ctx, pb.OperateType_UpdateWorkerRelayConfig, reqBytes)
	if err != nil {
		return responseErr(err), nil
	}

	response := &pb.CommonWorkerResponse{}
	err = response.Unmarshal(responseBytes)
	if err != nil {
		return responseErr(err), nil
	}

	return response, nil
}

func (s *Server) updateWorkerRelayConfig(ctx context.Context, req *pb.UpdateWorkerRelayConfigRequest) (*pb.CommonWorkerResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "UpdateWorkerRelayConfig"))
	worker := req.Worker
	content := req.Config
	cli, ok := s.workerClients[worker]
	if !ok {
		return errorCommonWorkerResponse(fmt.Sprintf("worker %s relevant worker-client not found", worker), worker), nil
	}

	log.L().Info("update relay config", zap.String("worker", worker), zap.String("request", "UpdateWorkerRelayConfig"))
	request := &workerrpc.Request{
		Type:        workerrpc.CmdUpdateRelay,
		UpdateRelay: &pb.UpdateRelayRequest{Content: content},
	}
	resp, err := cli.SendRequest(ctx, request, s.cfg.RPCTimeout)
	if err != nil {
		return errorCommonWorkerResponse(errors.ErrorStack(err), worker), nil
	}
	return resp.UpdateRelay, nil
}

// TODO: refine the call stack of this API, query worker configs that we needed only
func (s *Server) getWorkerConfigs(ctx context.Context, workerIDs []string) (map[string]config.DBConfig, error) {
	var (
		wg          sync.WaitGroup
		workerMutex sync.Mutex
		workerCfgs  = make(map[string]config.DBConfig)
		errCh       = make(chan error, len(s.workerClients))
		err         error
	)
	handleErr := func(err2 error) bool {
		if err2 != nil {
			log.L().Error("response error", zap.Error(err2))
		}
		errCh <- err2
		return false
	}

	argsExtractor := func(args ...interface{}) (string, workerrpc.Client, bool) {
		if len(args) != 2 {
			return "", nil, handleErr(terror.ErrMasterGetWorkerCfgExtractor.Generatef("fail to call emit to fetch worker config, miss some arguments %v", args))
		}

		worker, ok := args[0].(string)
		if !ok {
			return "", nil, handleErr(terror.ErrMasterGetWorkerCfgExtractor.Generatef("fail to call emit to fetch worker config, can't get id from args[0], arguments %v", args))
		}

		client, ok := args[1].(*workerrpc.GRPCClient)
		if !ok {
			return "", nil, handleErr(terror.ErrMasterGetWorkerCfgExtractor.Generatef("fail to call emit to fetch config of worker %s, can't get worker client from args[1], arguments %v", worker, args))
		}

		return worker, client, true
	}

	request := &workerrpc.Request{
		Type:              workerrpc.CmdQueryWorkerConfig,
		QueryWorkerConfig: &pb.QueryWorkerConfigRequest{},
	}
	for _, worker := range workerIDs {
		client, ok := s.workerClients[worker]
		if !ok {
			continue // outer caller can handle the lack of the config
		}
		wg.Add(1)
		go s.ap.Emit(ctx, 0, func(args ...interface{}) {
			defer wg.Done()

			worker1, client1, ok := argsExtractor(args...)
			if !ok {
				return
			}

			response, err1 := client1.SendRequest(ctx, request, s.cfg.RPCTimeout)
			if err1 != nil {
				handleErr(terror.Annotatef(err1, "fetch config of worker %s", worker1))
				return
			}

			resp := response.QueryWorkerConfig
			if !resp.Result {
				handleErr(terror.ErrMasterQueryWorkerConfig.Generatef("fail to query config from worker %s, message %s", worker1, resp.Msg))
				return
			}

			if len(resp.Content) == 0 {
				handleErr(terror.ErrMasterQueryWorkerConfig.Generatef("fail to query config from worker %s, config is empty", worker1))
				return
			}

			if len(resp.SourceID) == 0 {
				handleErr(terror.ErrMasterQueryWorkerConfig.Generatef("fail to query config from worker %s, source ID is empty, it should be set in worker config", worker1))
				return
			}

			dbCfg := &config.DBConfig{}
			err2 := dbCfg.Decode(resp.Content)
			if err2 != nil {
				handleErr(terror.WithClass(terror.Annotatef(err2, "unmarshal worker %s config, resp: %s", worker1, resp.Content), terror.ClassDMMaster))
				return
			}

			workerMutex.Lock()
			workerCfgs[resp.SourceID] = *dbCfg
			workerMutex.Unlock()

		}, func(args ...interface{}) {
			defer wg.Done()

			worker1, _, ok := argsExtractor(args...)
			if !ok {
				return
			}
			handleErr(terror.ErrMasterNoEmitToken.Generate(worker1))
		}, worker, client)
	}

	wg.Wait()

	if len(errCh) > 0 {
		err = <-errCh
	}

	return workerCfgs, err
}

// MigrateWorkerRelay migrates dm-woker relay unit
func (s *Server) MigrateWorkerRelay(ctx context.Context, req *pb.MigrateWorkerRelayRequest) (*pb.CommonWorkerResponse, error) {
	log.L().Info("receive request and will save it to etcd", zap.Stringer("payload", req), zap.String("request", "MigrateWorkerRelay"))

	responseErr := func(err error) *pb.CommonWorkerResponse {
		return &pb.CommonWorkerResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}
	}

	reqBytes, err := req.Marshal()
	if err != nil {
		return responseErr(err), nil
	}
	responseBytes, err := s.saveRequestAndWaitResponse(ctx, pb.OperateType_MigrateWorkerRelay, reqBytes)
	if err != nil {
		return responseErr(err), nil
	}

	response := &pb.CommonWorkerResponse{}
	err = response.Unmarshal(responseBytes)
	if err != nil {
		return responseErr(err), nil
	}

	return response, nil
}

func (s *Server) migrateWorkerRelay(ctx context.Context, req *pb.MigrateWorkerRelayRequest) (*pb.CommonWorkerResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "MigrateWorkerRelay"))
	worker := req.Worker
	binlogPos := req.BinlogPos
	binlogName := req.BinlogName
	cli, ok := s.workerClients[worker]
	if !ok {
		return errorCommonWorkerResponse(fmt.Sprintf("worker %s relevant worker-client not found", worker), worker), nil
	}
	log.L().Info("try to migrate relay", zap.String("worker", worker), zap.String("request", "MigrateWorkerRelay"))
	request := &workerrpc.Request{
		Type:         workerrpc.CmdMigrateRelay,
		MigrateRelay: &pb.MigrateRelayRequest{BinlogName: binlogName, BinlogPos: binlogPos},
	}
	resp, err := cli.SendRequest(ctx, request, s.cfg.RPCTimeout)
	if err != nil {
		return errorCommonWorkerResponse(errors.ErrorStack(err), worker), nil
	}
	return resp.MigrateRelay, nil
}

// CheckTask checks legality of task configuration
func (s *Server) CheckTask(ctx context.Context, req *pb.CheckTaskRequest) (*pb.CheckTaskResponse, error) {
	log.L().Info("receive request and will save it to etcd", zap.Stringer("payload", req), zap.String("request", "CheckTask"))

	responseErr := func(err error) *pb.CheckTaskResponse {
		return &pb.CheckTaskResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}
	}

	reqBytes, err := req.Marshal()
	if err != nil {
		return responseErr(err), nil
	}
	responseBytes, err := s.saveRequestAndWaitResponse(ctx, pb.OperateType_CheckTask, reqBytes)
	if err != nil {
		return responseErr(err), nil
	}

	response := &pb.CheckTaskResponse{}
	err = response.Unmarshal(responseBytes)
	if err != nil {
		return responseErr(err), nil
	}

	return response, nil
}

func (s *Server) checkTask(ctx context.Context, req *pb.CheckTaskRequest) (*pb.CheckTaskResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "CheckTask"))

	_, _, err := s.generateSubTask(ctx, req.Task)
	if err != nil {
		return &pb.CheckTaskResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}, nil
	}

	return &pb.CheckTaskResponse{
		Result: true,
		Msg:    "check pass!!!",
	}, nil
}

func (s *Server) generateSubTask(ctx context.Context, task string) (*config.TaskConfig, []*config.SubTaskConfig, error) {
	cfg := config.NewTaskConfig()
	err := cfg.Decode(task)
	if err != nil {
		return nil, nil, terror.WithClass(err, terror.ClassDMMaster)
	}

	// get workerID from deploy map by sourceID, refactor this when dynamic add/remove worker supported.
	workerIDs := make([]string, 0, len(cfg.MySQLInstances))
	for _, inst := range cfg.MySQLInstances {
		workerID, ok := s.cfg.DeployMap[inst.SourceID]
		if !ok {
			return nil, nil, terror.ErrMasterTaskConfigExtractor.Generatef("%s relevant worker not found", inst.SourceID)
		}
		workerIDs = append(workerIDs, workerID)
	}

	sourceCfgs, err := s.getWorkerConfigs(ctx, workerIDs)
	if err != nil {
		return nil, nil, err
	}

	stCfgs, err := cfg.SubTaskConfigs(sourceCfgs)
	if err != nil {
		return nil, nil, terror.WithClass(err, terror.ClassDMMaster)
	}

	err = checker.CheckSyncConfigFunc(ctx, stCfgs)
	if err != nil {
		return nil, nil, terror.WithClass(err, terror.ClassDMMaster)
	}

	return cfg, stCfgs, nil
}

var (
	maxRetryNum   = 30
	retryInterval = time.Second
)

func (s *Server) waitOperationOk(ctx context.Context, cli workerrpc.Client, taskName, workerID string, opLogID int64) error {
	req := &workerrpc.Request{
		Type: workerrpc.CmdQueryTaskOperation,
		QueryTaskOperation: &pb.QueryTaskOperationRequest{
			Name:  taskName,
			LogID: opLogID,
		},
	}

	for num := 0; num < maxRetryNum; num++ {
		resp, err := cli.SendRequest(ctx, req, s.cfg.RPCTimeout)
		var queryResp *pb.QueryTaskOperationResponse
		if err != nil {
			log.L().Error("fail to query task operation", zap.String("task", taskName), zap.String("worker", workerID), zap.Int64("operation log ID", opLogID), log.ShortError(err))
		} else {
			queryResp = resp.QueryTaskOperation
			respLog := queryResp.Log
			if respLog == nil {
				return terror.ErrMasterOperNotFound.Generate(opLogID, taskName, workerID)
			} else if respLog.Success {
				return nil
			} else if len(respLog.Message) != 0 {
				return terror.ErrMasterOperRespNotSuccess.Generate(opLogID, taskName, workerID, respLog.Message)
			}
			log.L().Info("wait op log result", zap.String("task", taskName), zap.String("worker", workerID), zap.Int64("operation log ID", opLogID), zap.Stringer("result", resp.QueryTaskOperation))
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryInterval):
		}
	}

	return terror.ErrMasterOperRequestTimeout.Generate(workerID)
}

func (s *Server) handleOperationResult(ctx context.Context, cli workerrpc.Client, taskName, workerID string, err error, resp *workerrpc.Response) *pb.OperateSubTaskResponse {
	if err != nil {
		return &pb.OperateSubTaskResponse{
			Meta: errorCommonWorkerResponse(errors.ErrorStack(err), ""),
		}
	}
	response := &pb.OperateSubTaskResponse{}
	switch resp.Type {
	case workerrpc.CmdStartSubTask:
		response = resp.StartSubTask
	case workerrpc.CmdOperateSubTask:
		response = resp.OperateSubTask
	case workerrpc.CmdUpdateSubTask:
		response = resp.UpdateSubTask
	default:
		// this should not happen
		response.Meta = errorCommonWorkerResponse(fmt.Sprintf("invalid operate task type %v", resp.Type), "")
		return response
	}

	err = s.waitOperationOk(ctx, cli, taskName, workerID, response.LogID)
	if err != nil {
		response.Meta = errorCommonWorkerResponse(errors.ErrorStack(err), "")
	}

	return response
}

// taskConfigArgsExtractor extracts SubTaskConfig from args and returns its relevant
// grpc client, worker id (host:port), subtask config in toml, task name and error
func (s *Server) taskConfigArgsExtractor(args ...interface{}) (workerrpc.Client, string, string, string, error) {
	handleErr := func(err error) error {
		log.L().Error("response", zap.Error(err))
		return err
	}

	if len(args) != 1 {
		return nil, "", "", "", handleErr(terror.ErrMasterTaskConfigExtractor.Generatef("miss task config %v", args))
	}

	cfg, ok := args[0].(*config.SubTaskConfig)
	if !ok {
		return nil, "", "", "", handleErr(terror.ErrMasterTaskConfigExtractor.Generatef("args[0] is not SubTaskConfig: %v", args[0]))
	}

	worker, ok1 := s.cfg.DeployMap[cfg.SourceID]
	cli, ok2 := s.workerClients[worker]
	if !ok1 || !ok2 {
		return nil, "", "", "", handleErr(terror.ErrMasterTaskConfigExtractor.Generatef("%s relevant worker-client not found", worker))
	}

	cfgToml, err := cfg.Toml()
	if err != nil {
		return nil, "", "", "", handleErr(err)
	}

	return cli, worker, cfgToml, cfg.Name, nil
}

// workerArgsExtractor extracts worker from args and returns its relevant
// grpc client, worker id (host:port) and error
func (s *Server) workerArgsExtractor(args ...interface{}) (workerrpc.Client, string, error) {
	if len(args) != 1 {
		return nil, "", terror.ErrMasterWorkerArgsExtractor.Generatef("miss worker id %v", args)
	}
	worker, ok := args[0].(string)
	if !ok {
		return nil, "", terror.ErrMasterWorkerArgsExtractor.Generatef("invalid argument, args[0] is not valid worker id: %v", args[0])
	}
	cli, ok := s.workerClients[worker]
	if !ok {
		return nil, worker, terror.ErrMasterWorkerArgsExtractor.Generatef("%s relevant worker-client not found", worker)
	}

	return cli, worker, nil
}

// WatchRequest watches requests in etcd, and handle these request, and write response to etcd.
func (s *Server) WatchRequest(ctx context.Context) {
	watchCh := s.etcdClient.Watch(context.Background(), defaultOperatePath, -1)

	for {
		select {
		case <-ctx.Done():
			return
		case wresp := <-watchCh:
			if wresp.Err() != nil {
				log.L().Warn("watch etcd failed", zap.Error(wresp.Err()))
				continue
			}

			for _, ev := range wresp.Events {
				// only need handle put event
				// FIXME: if don't use int32, will get error invalid operation: `ev.Type != "go.etcd.io/etcd/mvcc/mvccpb".PUT (mismatched types "github.com/coreos/etcd/mvcc/mvccpb".Event_EventType and "go.etcd.io/etcd/mvcc/mvccpb".Event_EventType)` when build, don't know why now, maybe fix it later.
				if int32(ev.Type) != int32(mvccpb.PUT) {
					continue
				}

				operate := &pb.Operate{}
				err := operate.Unmarshal(ev.Kv.Value)
				if err != nil {
					log.L().Error("unmarshal operate failed", zap.Error(err))
					continue
				}

				if len(operate.Response) != 0 || len(operate.Err) != 0 {
					// this request already had response, ignore it
					continue
				}

				// FIXME: only master leader need handle request
				go s.handleRequest(string(ev.Kv.Key), operate)
			}
		}
	}
}

func (s *Server) handleRequest(path string, operate *pb.Operate) {
	var err error
	var responseBytes []byte

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	switch operate.Tp {
	case pb.OperateType_MigrateWorkerRelay:
		request := &pb.MigrateWorkerRelayRequest{}
		if err = request.Unmarshal(operate.Request); err != nil {
			operate.Err = err.Error()
			break
		}
		response, err := s.migrateWorkerRelay(ctx, request)
		if err != nil {
			operate.Err = err.Error()
			break
		}
		responseBytes, err = response.Marshal()
	case pb.OperateType_UpdateWorkerRelayConfig:
		request := &pb.UpdateWorkerRelayConfigRequest{}
		if err = request.Unmarshal(operate.Request); err != nil {
			operate.Err = err.Error()
			break
		}
		response, err := s.updateWorkerRelayConfig(ctx, request)
		if err != nil {
			operate.Err = err.Error()
			break
		}
		responseBytes, err = response.Marshal()
	case pb.OperateType_StartTask:
		request := &pb.StartTaskRequest{}
		if err = request.Unmarshal(operate.Request); err != nil {
			operate.Err = err.Error()
			break
		}
		response, err := s.startTask(ctx, request)
		if err != nil {
			operate.Err = err.Error()
			break
		}
		responseBytes, err = response.Marshal()
	case pb.OperateType_UpdateMasterConfig:
		request := &pb.UpdateMasterConfigRequest{}
		if err = request.Unmarshal(operate.Request); err != nil {
			operate.Err = err.Error()
			break
		}
		response, err := s.updateMasterConfig(ctx, request)
		if err != nil {
			operate.Err = err.Error()
			break
		}
		responseBytes, err = response.Marshal()
	case pb.OperateType_OperateTask:
		request := &pb.OperateTaskRequest{}
		if err = request.Unmarshal(operate.Request); err != nil {
			operate.Err = err.Error()
			break
		}
		response, err := s.operateTask(ctx, request)
		if err != nil {
			operate.Err = err.Error()
			break
		}
		responseBytes, err = response.Marshal()
	case pb.OperateType_UpdateTask:
		request := &pb.UpdateTaskRequest{}
		if err = request.Unmarshal(operate.Request); err != nil {
			operate.Err = err.Error()
			break
		}
		response, err := s.updateTask(ctx, request)
		if err != nil {
			operate.Err = err.Error()
			break
		}
		responseBytes, err = response.Marshal()
	case pb.OperateType_QueryStatus:
		request := &pb.QueryStatusListRequest{}
		if err = request.Unmarshal(operate.Request); err != nil {
			operate.Err = err.Error()
			break
		}
		response, err := s.queryStatus(ctx, request)
		if err != nil {
			operate.Err = err.Error()
			break
		}
		responseBytes, err = response.Marshal()
	case pb.OperateType_QueryError:
		request := &pb.QueryErrorListRequest{}
		if err = request.Unmarshal(operate.Request); err != nil {
			operate.Err = err.Error()
			break
		}
		response, err := s.queryError(ctx, request)
		if err != nil {
			operate.Err = err.Error()
			break
		}
		responseBytes, err = response.Marshal()
	case pb.OperateType_ShowDDLLocks:
		request := &pb.ShowDDLLocksRequest{}
		if err = request.Unmarshal(operate.Request); err != nil {
			operate.Err = err.Error()
			break
		}
		response, err := s.showDDLLocks(ctx, request)
		if err != nil {
			operate.Err = err.Error()
			break
		}
		responseBytes, err = response.Marshal()
	case pb.OperateType_UnlockDDLLock:
		request := &pb.UnlockDDLLockRequest{}
		if err = request.Unmarshal(operate.Request); err != nil {
			operate.Err = err.Error()
			break
		}
		response, err := s.unlockDDLLock(ctx, request)
		if err != nil {
			operate.Err = err.Error()
			break
		}
		responseBytes, err = response.Marshal()
	case pb.OperateType_BreakWorkerDDLLock:
		request := &pb.BreakWorkerDDLLockRequest{}
		if err = request.Unmarshal(operate.Request); err != nil {
			operate.Err = err.Error()
			break
		}
		response, err := s.breakWorkerDDLLock(ctx, request)
		if err != nil {
			operate.Err = err.Error()
			break
		}
		responseBytes, err = response.Marshal()
	case pb.OperateType_SwitchWorkerRelayMaster:
		request := &pb.SwitchWorkerRelayMasterRequest{}
		if err = request.Unmarshal(operate.Request); err != nil {
			operate.Err = err.Error()
			break
		}
		response, err := s.switchWorkerRelayMaster(ctx, request)
		if err != nil {
			operate.Err = err.Error()
			break
		}
		responseBytes, err = response.Marshal()
	case pb.OperateType_OperateWorkerRelay:
		request := &pb.OperateWorkerRelayRequest{}
		if err = request.Unmarshal(operate.Request); err != nil {
			operate.Err = err.Error()
			break
		}
		response, err := s.operateWorkerRelayTask(ctx, request)
		if err != nil {
			operate.Err = err.Error()
			break
		}
		responseBytes, err = response.Marshal()
	case pb.OperateType_RefreshWorkerTasks:
		request := &pb.RefreshWorkerTasksRequest{}
		if err = request.Unmarshal(operate.Request); err != nil {
			operate.Err = err.Error()
			break
		}
		response, err := s.refreshWorkerTasks(ctx, request)
		if err != nil {
			operate.Err = err.Error()
			break
		}
		responseBytes, err = response.Marshal()
	case pb.OperateType_HandleSQLs:
		request := &pb.HandleSQLsRequest{}
		if err = request.Unmarshal(operate.Request); err != nil {
			operate.Err = err.Error()
			break
		}
		response, err := s.handleSQLs(ctx, request)
		if err != nil {
			operate.Err = err.Error()
			break
		}
		responseBytes, err = response.Marshal()
	case pb.OperateType_PurgeWorkerRelay:
		request := &pb.PurgeWorkerRelayRequest{}
		if err = request.Unmarshal(operate.Request); err != nil {
			operate.Err = err.Error()
			break
		}
		response, err := s.purgeWorkerRelay(ctx, request)
		if err != nil {
			operate.Err = err.Error()
			break
		}
		responseBytes, err = response.Marshal()
	case pb.OperateType_CheckTask:
		request := &pb.CheckTaskRequest{}
		if err = request.Unmarshal(operate.Request); err != nil {
			operate.Err = err.Error()
			break
		}
		response, err := s.checkTask(ctx, request)
		if err != nil {
			operate.Err = err.Error()
			break
		}
		responseBytes, err = response.Marshal()
	default:

	}

	if err != nil {
		operate.Err = err.Error()
	} else {
		operate.Response = responseBytes
	}

	operateBytes, err := operate.Marshal()
	if err != nil {
		log.L().Error("marshal operate failed", zap.Error(err))
		return
	}

	err = s.etcdClient.Update(ctx, path, string(operateBytes), 0)
	if err != nil {
		log.L().Error("update operate failed", zap.Error(err))
	}
}

// saveRequestAndWaitResponse saves request to etcd, and wait for the response
func (s *Server) saveRequestAndWaitResponse(ctx context.Context, tp pb.OperateType, request []byte) ([]byte, error) {
	operateID := getOperateID()

	operateIDStr := strconv.FormatInt(operateID, 10)

	operate := pb.Operate{
		Tp:      tp,
		Request: request,
	}
	opBytes, err := operate.Marshal()
	if err != nil {
		return nil, errors.Trace(err)
	}
	revision, err := s.etcdClient.Create(ctx, operateIDStr, string(opBytes), nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	fmt.Println(revision)

	watchCh := s.etcdClient.Watch(ctx, operateIDStr, revision+1)

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case wresp := <-watchCh:
			if wresp.Err() != nil {
				return nil, errors.Trace(wresp.Err())
			}

			// should only have one event
			if len(wresp.Events) > 1 {
				log.L().Warn("have more than one event on key in etcd", zap.String("key", operateIDStr))
			}

			for _, ev := range wresp.Events {
				operate := &pb.Operate{}
				err := operate.Unmarshal(ev.Kv.Value)
				if err != nil {
					return nil, errors.Trace(err)
				}

				if len(operate.Err) != 0 {
					return nil, errors.New(operate.Err)
				}

				return operate.Response, nil
			}
		}
	}
}

// getOperateID returns a unique operate id
// TODO: Operate ID should be monotone increasing, just like OpLog.ID in dm-worker
func getOperateID() int64 {
	rand.Seed(time.Now().UnixNano())
	randomValue := uint32(rand.Intn(1000))
	return time.Now().UnixNano()/1000*1000 + int64(randomValue)
}
