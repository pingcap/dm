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
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/siddontang/go/sync2"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/pingcap/dm/checker"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/master/scheduler"
	"github.com/pingcap/dm/dm/master/shardddl"
	operator "github.com/pingcap/dm/dm/master/sql-operator"
	"github.com/pingcap/dm/dm/master/workerrpc"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/conn"
	"github.com/pingcap/dm/pkg/election"
	"github.com/pingcap/dm/pkg/etcdutil"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/tracing"
	"github.com/pingcap/dm/pkg/utils"
)

const (
	// the session's TTL in seconds for leader election.
	// NOTE: select this value carefully when adding a mechanism relying on leader election.
	electionTTL = 60
	// the DM-master leader election key prefix
	// DM-master cluster : etcd cluster = 1 : 1 now.
	electionKey = "/dm-master/leader"
)

var (
	// the retry times for dm-master to confirm the dm-workers status is expected
	maxRetryNum = 30
	// the retry interval for dm-master to confirm the dm-workers status is expected
	retryInterval = time.Second
)

// Server handles RPC requests for dm-master
type Server struct {
	sync.RWMutex

	cfg *Config

	// the embed etcd server, and the gRPC/HTTP API server also attached to it.
	etcd *embed.Etcd

	etcdClient *clientv3.Client
	election   *election.Election

	leader         string
	leaderClient   pb.MasterClient
	leaderGrpcConn *grpc.ClientConn

	// WaitGroup for background functions.
	bgFunWg sync.WaitGroup

	// dm-worker-ID(host:ip) -> dm-worker client management
	scheduler *scheduler.Scheduler

	// shard DDL pessimist
	pessimist *shardddl.Pessimist

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
	logger := log.L()
	server := Server{
		cfg:               cfg,
		scheduler:         scheduler.NewScheduler(&logger),
		sqlOperatorHolder: operator.NewHolder(),
		idGen:             tracing.NewIDGen(),
		ap:                NewAgentPool(&RateLimitConfig{rate: cfg.RPCRateLimit, burst: cfg.RPCRateBurst}),
	}
	server.pessimist = shardddl.NewPessimist(&logger, server.getTaskResources)
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

	// create an etcd client used in the whole server instance.
	// NOTE: we only use the local member's address now, but we can use all endpoints of the cluster if needed.
	s.etcdClient, err = etcdutil.CreateClient([]string{s.cfg.MasterAddr})
	if err != nil {
		return
	}

	// start leader election
	// TODO: s.cfg.Name -> address
	s.election, err = election.NewElection(ctx, s.etcdClient, electionTTL, electionKey, s.cfg.Name, s.cfg.AdvertiseAddr)
	if err != nil {
		return
	}

	// start the shard DDL pessimist.
	err = s.pessimist.Start(ctx, s.etcdClient)
	if err != nil {
		return
	}

	s.closed.Set(false) // the server started now.

	s.bgFunWg.Add(1)
	go func() {
		defer s.bgFunWg.Done()
		s.ap.Start(ctx)
	}()

	s.bgFunWg.Add(1)
	go func() {
		defer s.bgFunWg.Done()
		s.electionNotify(ctx)
	}()

	s.bgFunWg.Add(1)
	go func() {
		defer s.bgFunWg.Done()
		select {
		case <-ctx.Done():
			return
		}
	}()

	log.L().Info("listening gRPC API and status request", zap.String("address", s.cfg.MasterAddr))
	return
}

// Close close the RPC server, this function can be called multiple times
func (s *Server) Close() {
	if s.closed.Get() {
		return
	}
	log.L().Info("closing server")

	// wait for background functions returned
	s.bgFunWg.Wait()

	s.Lock()
	defer s.Unlock()

	s.pessimist.Close()

	if s.election != nil {
		s.election.Close()
	}

	if s.etcdClient != nil {
		s.etcdClient.Close()
	}

	// close the etcd and other attached servers
	if s.etcd != nil {
		s.etcd.Close()
	}
	s.closed.Set(true)

}

func errorCommonWorkerResponse(msg string, source, worker string) *pb.CommonWorkerResponse {
	return &pb.CommonWorkerResponse{
		Result: false,
		Msg:    msg,
		Source: source,
		Worker: worker,
	}
}

// RegisterWorker registers the worker to the master, and all the worker will be store in the path:
// key:   /dm-worker/r/address
// value: name
func (s *Server) RegisterWorker(ctx context.Context, req *pb.RegisterWorkerRequest) (*pb.RegisterWorkerResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "RegisterWorker"))
	isLeader, needForward := s.isLeaderAndNeedForward()
	if !isLeader {
		if needForward {
			return s.leaderClient.RegisterWorker(ctx, req)
		}
		return nil, terror.ErrMasterRequestIsNotForwardToLeader
	}

	err := s.scheduler.AddWorker(req.Name, req.Address)
	if err != nil {
		return &pb.RegisterWorkerResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}, nil
	}
	log.L().Info("register worker successfully", zap.String("name", req.Name), zap.String("address", req.Address))
	return &pb.RegisterWorkerResponse{
		Result: true,
	}, nil
}

// OfflineWorker removes info of the worker which has been Closed, and all the worker are store in the path:
// key:   /dm-worker/r/address
// value: name
func (s *Server) OfflineWorker(ctx context.Context, req *pb.OfflineWorkerRequest) (*pb.OfflineWorkerResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "OfflineWorker"))
	isLeader, needForward := s.isLeaderAndNeedForward()
	if !isLeader {
		if needForward {
			return s.leaderClient.OfflineWorker(ctx, req)
		}
		return nil, terror.ErrMasterRequestIsNotForwardToLeader
	}

	err := s.scheduler.RemoveWorker(req.Name)
	if err != nil {
		return &pb.OfflineWorkerResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}, nil
	}
	log.L().Info("offline worker successfully", zap.String("name", req.Name), zap.String("address", req.Address))
	return &pb.OfflineWorkerResponse{
		Result: true,
	}, nil
}

func subtaskCfgPointersToInstances(stCfgPointers ...*config.SubTaskConfig) []config.SubTaskConfig {
	stCfgs := make([]config.SubTaskConfig, 0, len(stCfgPointers))
	for _, stCfg := range stCfgPointers {
		stCfgs = append(stCfgs, *stCfg)
	}
	return stCfgs
}

// StartTask implements MasterServer.StartTask
func (s *Server) StartTask(ctx context.Context, req *pb.StartTaskRequest) (*pb.StartTaskResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "StartTask"))

	isLeader, needForward := s.isLeaderAndNeedForward()
	if !isLeader {
		if needForward {
			return s.leaderClient.StartTask(ctx, req)
		}
		return nil, terror.ErrMasterRequestIsNotForwardToLeader
	}

	resp := &pb.StartTaskResponse{}
	cfg, stCfgs, err := s.generateSubTask(ctx, req.Task)
	if err != nil {
		resp.Msg = errors.ErrorStack(err)
		return resp, nil
	}
	log.L().Info("", zap.String("task name", cfg.Name), zap.Stringer("task", cfg), zap.String("request", "StartTask"))

	sourceRespCh := make(chan *pb.CommonWorkerResponse, len(stCfgs))
	if len(req.Sources) > 0 {
		// specify only start task on partial sources
		sourceCfg := make(map[string]*config.SubTaskConfig, len(stCfgs))
		for _, stCfg := range stCfgs {
			sourceCfg[stCfg.SourceID] = stCfg
		}
		stCfgs = make([]*config.SubTaskConfig, 0, len(req.Sources))
		for _, source := range req.Sources {
			if stCfg, ok := sourceCfg[source]; ok {
				stCfgs = append(stCfgs, stCfg)
			} else {
				sourceRespCh <- errorCommonWorkerResponse("source not found in task's config", source, "")
			}
		}
	}

	var sourceResps []*pb.CommonWorkerResponse
	// there are invalid sourceCfgs
	if len(sourceRespCh) > 0 {
		sourceResps = sortCommonWorkerResults(sourceRespCh)
	} else {
		err = s.scheduler.AddSubTasks(subtaskCfgPointersToInstances(stCfgs...)...)
		if err != nil {
			resp.Msg = errors.ErrorStack(err)
			return resp, nil
		}
		sources := make([]string, 0, len(stCfgs))
		for _, stCfg := range stCfgs {
			sources = append(sources, stCfg.SourceID)
		}
		resp.Result = true
		sourceResps = s.getSourceRespsAfterOperation(ctx, cfg.Name, sources, []string{}, req)
	}

	resp.Sources = sourceResps
	return resp, nil
}

// OperateTask implements MasterServer.OperateTask
func (s *Server) OperateTask(ctx context.Context, req *pb.OperateTaskRequest) (*pb.OperateTaskResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "OperateTask"))

	isLeader, needForward := s.isLeaderAndNeedForward()
	if !isLeader {
		if needForward {
			return s.leaderClient.OperateTask(ctx, req)
		}
		return nil, terror.ErrMasterRequestIsNotForwardToLeader
	}

	resp := &pb.OperateTaskResponse{
		Op:     req.Op,
		Result: false,
	}

	sources := req.Sources
	if len(req.Sources) == 0 {
		sources = s.getTaskResources(req.Name)
	}
	if len(sources) == 0 {
		resp.Msg = fmt.Sprintf("task %s has no source or not exist, please check the task name and status", req.Name)
		return resp, nil
	}
	var expect pb.Stage
	switch req.Op {
	case pb.TaskOp_Pause:
		expect = pb.Stage_Paused
	case pb.TaskOp_Resume:
		expect = pb.Stage_Running
	case pb.TaskOp_Stop:
		expect = pb.Stage_Stopped
	default:
		resp.Msg = terror.ErrMasterInvalidOperateOp.Generate(req.Op.String(), "task").Error()
		return resp, nil
	}
	var err error
	if expect == pb.Stage_Stopped {
		err = s.scheduler.RemoveSubTasks(req.Name, sources...)
	} else {
		err = s.scheduler.UpdateExpectSubTaskStage(expect, req.Name, sources...)
	}
	if err != nil {
		resp.Msg = errors.ErrorStack(err)
		return resp, nil
	}

	resp.Result = true
	resp.Sources = s.getSourceRespsAfterOperation(ctx, req.Name, sources, []string{}, req)
	return resp, nil
}

// UpdateTask implements MasterServer.UpdateTask
// TODO: support update task later
func (s *Server) UpdateTask(ctx context.Context, req *pb.UpdateTaskRequest) (*pb.UpdateTaskResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "UpdateTask"))

	isLeader, needForward := s.isLeaderAndNeedForward()
	if !isLeader {
		if needForward {
			return s.leaderClient.UpdateTask(ctx, req)
		}
		return nil, terror.ErrMasterRequestIsNotForwardToLeader
	}

	cfg, stCfgs, err := s.generateSubTask(ctx, req.Task)
	if err != nil {
		return &pb.UpdateTaskResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}, nil
	}
	log.L().Info("update task", zap.String("task name", cfg.Name), zap.Stringer("task", cfg))

	workerRespCh := make(chan *pb.CommonWorkerResponse, len(stCfgs)+len(req.Sources))
	if len(req.Sources) > 0 {
		// specify only update task on partial dm-workers
		// filter sub-task-configs through user specified workers
		// if worker not exist, an error message will return
		workerCfg := make(map[string]*config.SubTaskConfig)
		for _, stCfg := range stCfgs {
			workerCfg[stCfg.SourceID] = stCfg
		}
		stCfgs = make([]*config.SubTaskConfig, 0, len(req.Sources))
		for _, source := range req.Sources {
			if sourceCfg, ok := workerCfg[source]; ok {
				stCfgs = append(stCfgs, sourceCfg)
			} else {
				workerRespCh <- errorCommonWorkerResponse("source not found in task's config or deployment config", source, "")
			}
		}
	}

	//var wg sync.WaitGroup
	//for _, stCfg := range stCfgs {
	//	wg.Add(1)
	//	go s.ap.Emit(ctx, 0, func(args ...interface{}) {
	//		defer wg.Done()
	//		cfg, _ := args[0].(*config.SubTaskConfig)
	//		worker, stCfgToml, _, err := s.taskConfigArgsExtractor(cfg)
	//		if err != nil {
	//			workerRespCh <- errorCommonWorkerResponse(err.Error(), cfg.SourceID)
	//			return
	//		}
	//		request := &workerrpc.Request{
	//			Type:          workerrpc.CmdUpdateSubTask,
	//			UpdateSubTask: &pb.UpdateSubTaskRequest{Task: stCfgToml},
	//		}
	//		resp, err := worker.SendRequest(ctx, request, s.cfg.RPCTimeout)
	//		if err != nil {
	//			resp = &workerrpc.Response{
	//				Type:          workerrpc.CmdUpdateSubTask,
	//				UpdateSubTask: errorCommonWorkerResponse(err.Error(), cfg.SourceID),
	//			}
	//		}
	//		resp.UpdateSubTask.Source = cfg.SourceID
	//		workerRespCh <- resp.UpdateSubTask
	//	}, func(args ...interface{}) {
	//		defer wg.Done()
	//		cfg, _ := args[0].(*config.SubTaskConfig)
	//		worker, _, _, err := s.taskConfigArgsExtractor(cfg)
	//		if err != nil {
	//			workerRespCh <- errorCommonWorkerResponse(err.Error(), cfg.SourceID)
	//			return
	//		}
	//		workerRespCh <- errorCommonWorkerResponse(terror.ErrMasterNoEmitToken.Generate(worker.Address()).Error(), cfg.SourceID)
	//	}, stCfg)
	//}
	//wg.Wait()

	workerRespMap := make(map[string]*pb.CommonWorkerResponse, len(stCfgs))
	workers := make([]string, 0, len(stCfgs))
	for len(workerRespCh) > 0 {
		workerResp := <-workerRespCh
		workerRespMap[workerResp.Source] = workerResp
		workers = append(workers, workerResp.Source)
	}

	sort.Strings(workers)
	workerResps := make([]*pb.CommonWorkerResponse, 0, len(workers))
	for _, worker := range workers {
		workerResps = append(workerResps, workerRespMap[worker])
	}

	return &pb.UpdateTaskResponse{
		Result:  true,
		Sources: workerResps,
	}, nil
}

type hasWokers interface {
	GetSources() []string
	GetName() string
}

func extractSources(s *Server, req hasWokers) ([]string, error) {
	var sources []string

	if len(req.GetSources()) > 0 {
		// query specified dm-workers
		invalidWorkers := make([]string, 0, len(req.GetSources()))
		for _, source := range req.GetSources() {
			w := s.scheduler.GetWorkerBySource(source)
			if w == nil || w.Stage() == scheduler.WorkerOffline {
				invalidWorkers = append(invalidWorkers, source)
			}
		}
		if len(invalidWorkers) > 0 {
			return nil, errors.Errorf("%s relevant worker-client not found", strings.Join(invalidWorkers, ", "))
		}
		sources = req.GetSources()
	} else if len(req.GetName()) > 0 {
		// query specified task's sources
		sources = s.getTaskResources(req.GetName())
		if len(sources) == 0 {
			return nil, errors.Errorf("task %s has no source or not exist, can try `refresh-worker-tasks` cmd first", req.GetName())
		}
	} else {
		// query all sources
		log.L().Info("get sources")
		sources = s.scheduler.BoundSources()
	}
	return sources, nil
}

// QueryStatus implements MasterServer.QueryStatus
func (s *Server) QueryStatus(ctx context.Context, req *pb.QueryStatusListRequest) (*pb.QueryStatusListResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "QueryStatus"))
	isLeader, needForward := s.isLeaderAndNeedForward()
	if !isLeader {
		if needForward {
			return s.leaderClient.QueryStatus(ctx, req)
		}
		return nil, terror.ErrMasterRequestIsNotForwardToLeader
	}

	sources, err := extractSources(s, req)
	if err != nil {
		return &pb.QueryStatusListResponse{
			Result: false,
			Msg:    err.Error(),
		}, nil
	}
	workerRespCh := s.getStatusFromWorkers(ctx, sources, req.Name)

	workerRespMap := make(map[string]*pb.QueryStatusResponse, len(sources))
	for len(workerRespCh) > 0 {
		workerResp := <-workerRespCh
		workerRespMap[workerResp.SourceStatus.Source] = workerResp
	}

	sort.Strings(sources)
	workerResps := make([]*pb.QueryStatusResponse, 0, len(sources))
	for _, worker := range sources {
		workerResps = append(workerResps, workerRespMap[worker])
	}
	resp := &pb.QueryStatusListResponse{
		Result:  true,
		Sources: workerResps,
	}
	return resp, nil
}

// QueryError implements MasterServer.QueryError
func (s *Server) QueryError(ctx context.Context, req *pb.QueryErrorListRequest) (*pb.QueryErrorListResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "QueryError"))
	isLeader, needForward := s.isLeaderAndNeedForward()
	if !isLeader {
		if needForward {
			return s.leaderClient.QueryError(ctx, req)
		}
		return nil, terror.ErrMasterRequestIsNotForwardToLeader
	}

	sources, err := extractSources(s, req)
	if err != nil {
		return &pb.QueryErrorListResponse{
			Result: false,
			Msg:    err.Error(),
		}, nil
	}

	workerRespCh := s.getErrorFromWorkers(ctx, sources, req.Name)

	workerRespMap := make(map[string]*pb.QueryErrorResponse, len(sources))
	for len(workerRespCh) > 0 {
		workerResp := <-workerRespCh
		workerRespMap[workerResp.SourceError.Source] = workerResp
	}

	sort.Strings(sources)
	workerResps := make([]*pb.QueryErrorResponse, 0, len(sources))
	for _, worker := range sources {
		workerResps = append(workerResps, workerRespMap[worker])
	}
	resp := &pb.QueryErrorListResponse{
		Result:  true,
		Sources: workerResps,
	}
	return resp, nil
}

// ShowDDLLocks implements MasterServer.ShowDDLLocks
func (s *Server) ShowDDLLocks(ctx context.Context, req *pb.ShowDDLLocksRequest) (*pb.ShowDDLLocksResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "ShowDDLLocks"))

	isLeader, needForward := s.isLeaderAndNeedForward()
	if !isLeader {
		if needForward {
			return s.leaderClient.ShowDDLLocks(ctx, req)
		}
		return nil, terror.ErrMasterRequestIsNotForwardToLeader
	}

	resp := &pb.ShowDDLLocksResponse{
		Result: true,
	}

	locks := s.pessimist.Locks()
	resp.Locks = make([]*pb.DDLLock, 0, len(locks))
	for _, lock := range locks {
		if len(req.Task) > 0 && req.Task != lock.Task {
			continue // specify task and mismatch
		}
		ready := lock.Ready()
		if len(req.Sources) > 0 {
			for _, worker := range req.Sources {
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
			DDLs:     lock.DDLs,
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
// TODO(csuzhangxc): implement this later.
func (s *Server) UnlockDDLLock(ctx context.Context, req *pb.UnlockDDLLockRequest) (*pb.UnlockDDLLockResponse, error) {
	log.L().Info("", zap.String("lock ID", req.ID), zap.Stringer("payload", req), zap.String("request", "UnlockDDLLock"))
	return &pb.UnlockDDLLockResponse{
		Result: false,
		Msg:    "not implement",
	}, nil
}

// BreakWorkerDDLLock implements MasterServer.BreakWorkerDDLLock
// TODO(csuzhangxc): implement this later.
func (s *Server) BreakWorkerDDLLock(ctx context.Context, req *pb.BreakWorkerDDLLockRequest) (*pb.BreakWorkerDDLLockResponse, error) {
	log.L().Info("", zap.String("lock ID", req.RemoveLockID), zap.Stringer("payload", req), zap.String("request", "BreakWorkerDDLLock"))
	return &pb.BreakWorkerDDLLockResponse{
		Result: false,
		Msg:    "not implement",
	}, nil
}

// HandleSQLs implements MasterServer.HandleSQLs
func (s *Server) HandleSQLs(ctx context.Context, req *pb.HandleSQLsRequest) (*pb.HandleSQLsResponse, error) {
	log.L().Info("", zap.String("task name", req.Name), zap.Stringer("payload", req), zap.String("request", "HandleSQLs"))

	isLeader, needForward := s.isLeaderAndNeedForward()
	if !isLeader {
		if needForward {
			return s.leaderClient.HandleSQLs(ctx, req)
		}
		return nil, terror.ErrMasterRequestIsNotForwardToLeader
	}

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

	if !s.checkTaskAndWorkerMatch(req.Name, req.Source) {
		resp.Msg = fmt.Sprintf("task %s and worker %s not match, can try `refresh-worker-tasks` cmd first", req.Name, req.Source)
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
	worker := s.scheduler.GetWorkerBySource(req.Source)
	if worker == nil {
		resp.Msg = fmt.Sprintf("source %s not found in bound sources %v", req.Source, s.scheduler.BoundSources())
		return resp, nil
	}
	response, err := worker.SendRequest(ctx, subReq, s.cfg.RPCTimeout)
	workerResp := &pb.CommonWorkerResponse{}
	if err != nil {
		workerResp = errorCommonWorkerResponse(errors.ErrorStack(err), req.Source, worker.BaseInfo().Name)
	} else {
		workerResp = response.HandleSubTaskSQLs
	}
	resp.Sources = []*pb.CommonWorkerResponse{workerResp}
	resp.Result = true
	return resp, nil
}

// PurgeWorkerRelay implements MasterServer.PurgeWorkerRelay
func (s *Server) PurgeWorkerRelay(ctx context.Context, req *pb.PurgeWorkerRelayRequest) (*pb.PurgeWorkerRelayResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "PurgeWorkerRelay"))

	isLeader, needForward := s.isLeaderAndNeedForward()
	if !isLeader {
		if needForward {
			return s.leaderClient.PurgeWorkerRelay(ctx, req)
		}
		return nil, terror.ErrMasterRequestIsNotForwardToLeader
	}

	workerReq := &workerrpc.Request{
		Type: workerrpc.CmdPurgeRelay,
		PurgeRelay: &pb.PurgeRelayRequest{
			Inactive: req.Inactive,
			Time:     req.Time,
			Filename: req.Filename,
			SubDir:   req.SubDir,
		},
	}

	workerRespCh := make(chan *pb.CommonWorkerResponse, len(req.Sources))
	var wg sync.WaitGroup
	for _, source := range req.Sources {
		wg.Add(1)
		go func(source string) {
			defer wg.Done()
			worker := s.scheduler.GetWorkerBySource(source)
			if worker == nil {
				workerRespCh <- errorCommonWorkerResponse(fmt.Sprintf("worker %s relevant worker-client not found", source), source, "")
				return
			}
			resp, err := worker.SendRequest(ctx, workerReq, s.cfg.RPCTimeout)
			workerResp := &pb.CommonWorkerResponse{}
			if err != nil {
				workerResp = errorCommonWorkerResponse(errors.ErrorStack(err), source, worker.BaseInfo().Name)
			} else {
				workerResp = resp.PurgeRelay
			}
			workerResp.Source = source
			workerRespCh <- workerResp
		}(source)
	}
	wg.Wait()

	workerRespMap := make(map[string]*pb.CommonWorkerResponse, len(req.Sources))
	for len(workerRespCh) > 0 {
		workerResp := <-workerRespCh
		workerRespMap[workerResp.Source] = workerResp
	}

	sort.Strings(req.Sources)
	workerResps := make([]*pb.CommonWorkerResponse, 0, len(req.Sources))
	for _, worker := range req.Sources {
		workerResps = append(workerResps, workerRespMap[worker])
	}

	return &pb.PurgeWorkerRelayResponse{
		Result:  true,
		Sources: workerResps,
	}, nil
}

// SwitchWorkerRelayMaster implements MasterServer.SwitchWorkerRelayMaster
func (s *Server) SwitchWorkerRelayMaster(ctx context.Context, req *pb.SwitchWorkerRelayMasterRequest) (*pb.SwitchWorkerRelayMasterResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "SwitchWorkerRelayMaster"))

	isLeader, needForward := s.isLeaderAndNeedForward()
	if !isLeader {
		if needForward {
			return s.leaderClient.SwitchWorkerRelayMaster(ctx, req)
		}
		return nil, terror.ErrMasterRequestIsNotForwardToLeader
	}

	workerRespCh := make(chan *pb.CommonWorkerResponse, len(req.Sources))

	handleErr := func(err error, source string) {
		log.L().Error("response error", zap.Error(err))
		resp := errorCommonWorkerResponse(errors.ErrorStack(err), source, "")
		workerRespCh <- resp
	}

	var wg sync.WaitGroup
	for _, source := range req.Sources {
		wg.Add(1)
		go s.ap.Emit(ctx, 0, func(args ...interface{}) {
			defer wg.Done()
			sourceID, _ := args[0].(string)
			worker := s.scheduler.GetWorkerBySource(sourceID)
			if worker == nil {
				err := terror.ErrMasterWorkerArgsExtractor.Generatef("%s relevant worker-client not found", sourceID)
				handleErr(err, sourceID)
				return
			}
			request := &workerrpc.Request{
				Type:              workerrpc.CmdSwitchRelayMaster,
				SwitchRelayMaster: &pb.SwitchRelayMasterRequest{},
			}
			resp, err := worker.SendRequest(ctx, request, s.cfg.RPCTimeout)
			workerResp := &pb.CommonWorkerResponse{}
			if err != nil {
				workerResp = errorCommonWorkerResponse(errors.ErrorStack(err), sourceID, worker.BaseInfo().Name)
			} else {
				workerResp = resp.SwitchRelayMaster
			}
			workerResp.Source = sourceID
			workerRespCh <- workerResp
		}, func(args ...interface{}) {
			defer wg.Done()
			sourceID, _ := args[0].(string)
			workerRespCh <- errorCommonWorkerResponse(terror.ErrMasterNoEmitToken.Generate(sourceID).Error(), sourceID, "")
		}, source)
	}
	wg.Wait()

	workerRespMap := make(map[string]*pb.CommonWorkerResponse, len(req.Sources))
	for len(workerRespCh) > 0 {
		workerResp := <-workerRespCh
		workerRespMap[workerResp.Source] = workerResp
	}

	sort.Strings(req.Sources)
	workerResps := make([]*pb.CommonWorkerResponse, 0, len(req.Sources))
	for _, worker := range req.Sources {
		workerResps = append(workerResps, workerRespMap[worker])
	}

	return &pb.SwitchWorkerRelayMasterResponse{
		Result:  true,
		Sources: workerResps,
	}, nil
}

// OperateWorkerRelayTask implements MasterServer.OperateWorkerRelayTask
func (s *Server) OperateWorkerRelayTask(ctx context.Context, req *pb.OperateWorkerRelayRequest) (*pb.OperateWorkerRelayResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "OperateWorkerRelayTask"))
	isLeader, needForward := s.isLeaderAndNeedForward()
	if !isLeader {
		if needForward {
			return s.leaderClient.OperateWorkerRelayTask(ctx, req)
		}
		return nil, terror.ErrMasterRequestIsNotForwardToLeader
	}

	resp := &pb.OperateWorkerRelayResponse{
		Op:     req.Op,
		Result: false,
	}
	var expect pb.Stage
	switch req.Op {
	case pb.RelayOp_ResumeRelay:
		expect = pb.Stage_Running
	case pb.RelayOp_PauseRelay:
		expect = pb.Stage_Paused
	default:
		resp.Msg = terror.ErrMasterInvalidOperateOp.Generate(req.Op.String(), "relay").Error()
		return resp, nil
	}
	err := s.scheduler.UpdateExpectRelayStage(expect, req.Sources...)
	if err != nil {
		resp.Msg = errors.ErrorStack(err)
		return resp, nil
	}
	resp.Result = true
	resp.Sources = s.getSourceRespsAfterOperation(ctx, "", req.Sources, []string{}, req)
	return resp, nil
}

// getTaskResources gets workers relevant to specified task
func (s *Server) getTaskResources(task string) []string {
	s.Lock()
	defer s.Unlock()
	cfgM := s.scheduler.GetSubTaskCfgsByTask(task)
	// do a copy
	ret := make([]string, 0, len(cfgM))
	for source := range cfgM {
		ret = append(ret, source)
	}
	return ret
}

// getStatusFromWorkers does RPC request to get status from dm-workers
func (s *Server) getStatusFromWorkers(ctx context.Context, sources []string, taskName string) chan *pb.QueryStatusResponse {
	workerReq := &workerrpc.Request{
		Type:        workerrpc.CmdQueryStatus,
		QueryStatus: &pb.QueryStatusRequest{Name: taskName},
	}
	workerRespCh := make(chan *pb.QueryStatusResponse, len(sources))

	handleErr := func(err error, source string) bool {
		log.L().Error("response error", zap.Error(err))
		resp := &pb.QueryStatusResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
			SourceStatus: &pb.SourceStatus{
				Source: source,
			},
		}
		workerRespCh <- resp
		return false
	}

	var wg sync.WaitGroup
	for _, source := range sources {
		wg.Add(1)
		go s.ap.Emit(ctx, 0, func(args ...interface{}) {
			defer wg.Done()
			sourceID, _ := args[0].(string)
			worker := s.scheduler.GetWorkerBySource(sourceID)
			if worker == nil {
				err := terror.ErrMasterWorkerArgsExtractor.Generatef("%s relevant worker-client not found", sourceID)
				handleErr(err, sourceID)
				return
			}
			resp, err := worker.SendRequest(ctx, workerReq, s.cfg.RPCTimeout)
			workerStatus := &pb.QueryStatusResponse{}
			if err != nil {
				workerStatus = &pb.QueryStatusResponse{
					Result:       false,
					Msg:          errors.ErrorStack(err),
					SourceStatus: &pb.SourceStatus{},
				}
			} else {
				workerStatus = resp.QueryStatus
			}
			workerStatus.SourceStatus.Source = sourceID
			workerRespCh <- workerStatus
		}, func(args ...interface{}) {
			defer wg.Done()
			sourceID, _ := args[0].(string)
			handleErr(terror.ErrMasterNoEmitToken.Generate(sourceID), sourceID)
		}, source)
	}
	wg.Wait()
	return workerRespCh
}

// getErrorFromWorkers does RPC request to get error information from dm-workers
func (s *Server) getErrorFromWorkers(ctx context.Context, sources []string, taskName string) chan *pb.QueryErrorResponse {
	workerReq := &workerrpc.Request{
		Type:       workerrpc.CmdQueryError,
		QueryError: &pb.QueryErrorRequest{Name: taskName},
	}
	workerRespCh := make(chan *pb.QueryErrorResponse, len(sources))

	handleErr := func(err error, source string) bool {
		log.L().Error("response error", zap.Error(err))
		resp := &pb.QueryErrorResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
			SourceError: &pb.SourceError{
				Source: source,
			},
		}
		workerRespCh <- resp
		return false
	}

	var wg sync.WaitGroup
	for _, source := range sources {
		wg.Add(1)
		go s.ap.Emit(ctx, 0, func(args ...interface{}) {
			defer wg.Done()
			sourceID, _ := args[0].(string)
			worker := s.scheduler.GetWorkerBySource(sourceID)
			if worker == nil {
				err := terror.ErrMasterWorkerArgsExtractor.Generatef("%s relevant worker-client not found", sourceID)
				handleErr(err, sourceID)
				return
			}

			resp, err := worker.SendRequest(ctx, workerReq, s.cfg.RPCTimeout)
			workerError := &pb.QueryErrorResponse{}
			if err != nil {
				workerError = &pb.QueryErrorResponse{
					Result:      false,
					Msg:         errors.ErrorStack(err),
					SourceError: &pb.SourceError{},
				}
			} else {
				workerError = resp.QueryError
			}
			workerError.SourceError.Source = sourceID
			workerRespCh <- workerError
		}, func(args ...interface{}) {
			defer wg.Done()
			sourceID, _ := args[0].(string)
			handleErr(terror.ErrMasterNoEmitToken.Generate(sourceID), sourceID)
		}, source)
	}
	wg.Wait()
	return workerRespCh
}

// return true means match, false means mismatch.
func (s *Server) checkTaskAndWorkerMatch(taskname string, targetWorker string) bool {
	// find worker
	workers := s.getTaskResources(taskname)
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

// UpdateMasterConfig implements MasterServer.UpdateConfig
func (s *Server) UpdateMasterConfig(ctx context.Context, req *pb.UpdateMasterConfigRequest) (*pb.UpdateMasterConfigResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "UpdateMasterConfig"))

	isLeader, needForward := s.isLeaderAndNeedForward()
	if !isLeader {
		if needForward {
			return s.leaderClient.UpdateMasterConfig(ctx, req)
		}
		return nil, terror.ErrMasterRequestIsNotForwardToLeader
	}

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
	s.cfg = cfg
	log.L().Info("update dm-master config file success", zap.String("request", "UpdateMasterConfig"))
	s.Unlock()
	return &pb.UpdateMasterConfigResponse{
		Result: true,
		Msg:    "",
	}, nil
}

// UpdateWorkerRelayConfig updates config for relay and (dm-worker)
func (s *Server) UpdateWorkerRelayConfig(ctx context.Context, req *pb.UpdateWorkerRelayConfigRequest) (*pb.CommonWorkerResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "UpdateWorkerRelayConfig"))

	isLeader, needForward := s.isLeaderAndNeedForward()
	if !isLeader {
		if needForward {
			return s.leaderClient.UpdateWorkerRelayConfig(ctx, req)
		}
		return nil, terror.ErrMasterRequestIsNotForwardToLeader
	}

	source := req.Source
	content := req.Config
	worker := s.scheduler.GetWorkerBySource(source)
	if worker == nil {
		return errorCommonWorkerResponse(fmt.Sprintf("source %s relevant source-client not found", source), source, ""), nil
	}

	log.L().Info("update relay config", zap.String("source", source), zap.String("request", "UpdateWorkerRelayConfig"))
	request := &workerrpc.Request{
		Type:        workerrpc.CmdUpdateRelay,
		UpdateRelay: &pb.UpdateRelayRequest{Content: content},
	}
	resp, err := worker.SendRequest(ctx, request, s.cfg.RPCTimeout)
	if err != nil {
		return errorCommonWorkerResponse(errors.ErrorStack(err), source, worker.BaseInfo().Name), nil
	}
	return resp.UpdateRelay, nil
}

// TODO: refine the call stack of this API, query worker configs that we needed only
func (s *Server) getSourceConfigs(sources []*config.MySQLInstance) (map[string]config.DBConfig, error) {
	cfgs := make(map[string]config.DBConfig)
	for _, source := range sources {
		if cfg := s.scheduler.GetSourceCfgByID(source.SourceID); cfg != nil {
			// check the password
			_, err := cfg.DecryptPassword()
			if err != nil {
				return nil, err
			}
			cfgs[source.SourceID] = cfg.From
		}
	}
	return cfgs, nil
}

// MigrateWorkerRelay migrates dm-woker relay unit
func (s *Server) MigrateWorkerRelay(ctx context.Context, req *pb.MigrateWorkerRelayRequest) (*pb.CommonWorkerResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "MigrateWorkerRelay"))

	isLeader, needForward := s.isLeaderAndNeedForward()
	if !isLeader {
		if needForward {
			return s.leaderClient.MigrateWorkerRelay(ctx, req)
		}
		return nil, terror.ErrMasterRequestIsNotForwardToLeader
	}

	source := req.Source
	binlogPos := req.BinlogPos
	binlogName := req.BinlogName
	worker := s.scheduler.GetWorkerBySource(source)
	if worker == nil {
		return errorCommonWorkerResponse(fmt.Sprintf("source %s relevant source-client not found", source), source, ""), nil
	}
	log.L().Info("try to migrate relay", zap.String("source", source), zap.String("request", "MigrateWorkerRelay"))
	request := &workerrpc.Request{
		Type:         workerrpc.CmdMigrateRelay,
		MigrateRelay: &pb.MigrateRelayRequest{BinlogName: binlogName, BinlogPos: binlogPos},
	}
	resp, err := worker.SendRequest(ctx, request, s.cfg.RPCTimeout)
	if err != nil {
		return errorCommonWorkerResponse(errors.ErrorStack(err), source, worker.BaseInfo().Name), nil
	}
	return resp.MigrateRelay, nil
}

// CheckTask checks legality of task configuration
func (s *Server) CheckTask(ctx context.Context, req *pb.CheckTaskRequest) (*pb.CheckTaskResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "CheckTask"))

	isLeader, needForward := s.isLeaderAndNeedForward()
	if !isLeader {
		if needForward {
			return s.leaderClient.CheckTask(ctx, req)
		}
		return nil, terror.ErrMasterRequestIsNotForwardToLeader
	}

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

func parseAndAdjustSourceConfig(cfg *config.SourceConfig, content string) error {
	if err := cfg.Parse(content); err != nil {
		return err
	}

	dbConfig, err := cfg.GenerateDBConfig()
	if err != nil {
		return err
	}

	fromDB, err := conn.DefaultDBProvider.Apply(*dbConfig)
	if err != nil {
		return err
	}
	if err = cfg.Adjust(fromDB.DB); err != nil {
		return err
	}
	if _, err = cfg.Toml(); err != nil {
		return err
	}
	return nil
}

// OperateSource will create or update an upstream source.
func (s *Server) OperateSource(ctx context.Context, req *pb.OperateSourceRequest) (*pb.OperateSourceResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "OperateSource"))
	isLeader, needForward := s.isLeaderAndNeedForward()
	if !isLeader {
		if needForward {
			return s.leaderClient.OperateSource(ctx, req)
		}
		return nil, terror.ErrMasterRequestIsNotForwardToLeader
	}

	cfg := config.NewSourceConfig()
	err := parseAndAdjustSourceConfig(cfg, req.Config)
	resp := &pb.OperateSourceResponse{
		Result: false,
	}
	if err != nil {
		resp.Msg = errors.ErrorStack(err)
		return resp, nil
	}
	var w *scheduler.Worker
	switch req.Op {
	case pb.SourceOp_StartSource:
		err := s.scheduler.AddSourceCfg(*cfg)
		if err != nil {
			resp.Msg = errors.ErrorStack(err)
			return resp, nil
		}
		// for start source, we should get worker after start source
		w = s.scheduler.GetWorkerBySource(cfg.SourceID)
	case pb.SourceOp_UpdateSource:
		// TODO: support SourceOp_UpdateSource later
		resp.Msg = "Update worker config is not supported by dm-ha now"
		return resp, nil
	case pb.SourceOp_StopSource:
		w = s.scheduler.GetWorkerBySource(cfg.SourceID)
		err := s.scheduler.RemoveSourceCfg(cfg.SourceID)
		if err != nil {
			resp.Msg = errors.ErrorStack(err)
			return resp, nil
		}
	default:
		resp.Msg = terror.ErrMasterInvalidOperateOp.Generate(req.Op.String(), "source").Error()
		return resp, nil
	}

	resp.Result = true
	// source is added but not bounded
	if w == nil {
		var msg string
		switch req.Op {
		case pb.SourceOp_StartSource:
			msg = "source is added but there is no free worker to bound"
		case pb.SourceOp_StopSource:
			msg = "source is stopped and hasn't bound to worker before being stopped"
		}
		resp.Sources = []*pb.CommonWorkerResponse{{
			Result: true,
			Msg:    msg,
			Source: cfg.SourceID,
		}}
	} else {
		resp.Sources = s.getSourceRespsAfterOperation(ctx, "", []string{cfg.SourceID}, []string{w.BaseInfo().Name}, req)
	}
	return resp, nil
}

func (s *Server) generateSubTask(ctx context.Context, task string) (*config.TaskConfig, []*config.SubTaskConfig, error) {
	cfg := config.NewTaskConfig()
	err := cfg.Decode(task)
	if err != nil {
		return nil, nil, terror.WithClass(err, terror.ClassDMMaster)
	}

	sourceCfgs, err := s.getSourceConfigs(cfg.MySQLInstances)
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

func extractWorkerError(result *pb.ProcessResult) error {
	if result != nil && len(result.Errors) > 0 {
		return terror.ErrMasterOperRespNotSuccess.Generate(utils.JoinProcessErrors(result.Errors))
	}
	return nil
}

/*
QueryStatus for worker response
Source:
	OperateSource:
		* StartSource, UpdateSource: sourceID = Source
		* StopSource: return resp.Result = false && resp.Msg = “worker has not started”.
		  But if len(resp.SourceStatus.Result) > 0, it means dm-worker has some error in
		  StopWorker and it should be pushed to users.
Task:
	StartTask, UpdateTask: query status and related subTask stage is running
	OperateTask:
		* pause: related task status is paused
		* resume: related task status is running
		* stop: related task can't be found in worker's result
Relay:
	OperateRelay:
		* pause: related relay status is paused
		* resume: related relay status is running
In the above situations, once we find an error in response we should return the error
*/
func (s *Server) waitOperationOk(ctx context.Context, cli *scheduler.Worker, taskName, sourceID string, masterReq interface{}) (*pb.QueryStatusResponse, error) {
	var expect pb.Stage
	switch masterReq.(type) {
	case *pb.OperateSourceRequest:
		req := masterReq.(*pb.OperateSourceRequest)
		switch req.Op {
		case pb.SourceOp_StartSource, pb.SourceOp_UpdateSource:
			expect = pb.Stage_Running
		case pb.SourceOp_StopSource:
			expect = pb.Stage_Stopped
		}
	case *pb.StartTaskRequest, *pb.UpdateTaskRequest:
		expect = pb.Stage_Running
	case *pb.OperateTaskRequest:
		req := masterReq.(*pb.OperateTaskRequest)
		switch req.Op {
		case pb.TaskOp_Resume:
			expect = pb.Stage_Running
		case pb.TaskOp_Pause:
			expect = pb.Stage_Paused
		case pb.TaskOp_Stop:
			expect = pb.Stage_Stopped
		}
	case *pb.OperateWorkerRelayRequest:
		req := masterReq.(*pb.OperateWorkerRelayRequest)
		switch req.Op {
		case pb.RelayOp_ResumeRelay:
			expect = pb.Stage_Running
		case pb.RelayOp_PauseRelay:
			expect = pb.Stage_Paused
		case pb.RelayOp_StopRelay:
			expect = pb.Stage_Stopped
		}
	default:
		return nil, terror.ErrMasterIsNotAsyncRequest.Generate(masterReq)
	}
	req := &workerrpc.Request{
		Type: workerrpc.CmdQueryStatus,
		QueryStatus: &pb.QueryStatusRequest{
			Name: taskName,
		},
	}

	for num := 0; num < maxRetryNum; num++ {
		// check whether source relative worker has been removed by scheduler
		if _, ok := masterReq.(*pb.OperateSourceRequest); ok {
			if expect == pb.Stage_Stopped {
				resp := &pb.QueryStatusResponse{
					Result:       true,
					SourceStatus: &pb.SourceStatus{Source: sourceID, Worker: cli.BaseInfo().Name},
				}
				if w := s.scheduler.GetWorkerByName(cli.BaseInfo().Name); w == nil {
					return resp, nil
				} else if cli.Stage() == scheduler.WorkerOffline {
					return resp, nil
				}
			}
		}
		resp, err := cli.SendRequest(ctx, req, s.cfg.RPCTimeout)
		var queryResp *pb.QueryStatusResponse
		if err != nil {
			log.L().Error("fail to query operation", zap.Int("retryNum", num), zap.String("task", taskName),
				zap.String("source", sourceID), zap.Stringer("expect", expect), log.ShortError(err))
		} else {
			queryResp = resp.QueryStatus

			switch masterReq.(type) {
			case *pb.OperateSourceRequest:
				switch expect {
				case pb.Stage_Running:
					if queryResp.SourceStatus.Source == sourceID {
						if err := extractWorkerError(queryResp.SourceStatus.Result); err != nil {
							return queryResp, err
						}
						return queryResp, nil
					}
				case pb.Stage_Stopped:
					// we don't use queryResp.SourceStatus.Source == "" because worker might be re-arranged after being stopped
					if queryResp.SourceStatus.Source != sourceID {
						if err := extractWorkerError(queryResp.SourceStatus.Result); err != nil {
							return queryResp, err
						}
						return queryResp, nil
					}
				}
			case *pb.StartTaskRequest, *pb.UpdateTaskRequest, *pb.OperateTaskRequest:
				if expect == pb.Stage_Stopped && len(queryResp.SubTaskStatus) == 0 {
					return queryResp, nil
				} else if len(queryResp.SubTaskStatus) == 1 {
					if subtaskStatus := queryResp.SubTaskStatus[0]; subtaskStatus != nil {
						if err := extractWorkerError(subtaskStatus.Result); err != nil {
							return queryResp, err
						}
						// If expect stage is running, finished should also be okay
						var finished pb.Stage = -1
						if expect == pb.Stage_Running {
							finished = pb.Stage_Finished
						}
						if expect == pb.Stage_Stopped {
							if st, ok := subtaskStatus.Status.(*pb.SubTaskStatus_Msg); ok && st.Msg == fmt.Sprintf("no sub task with name %s has started", taskName) {
								return queryResp, nil
							}
						} else if subtaskStatus.Name == taskName && (subtaskStatus.Stage == expect || subtaskStatus.Stage == finished) {
							return queryResp, nil
						}
					}
				}
			case *pb.OperateWorkerRelayRequest:
				if queryResp.SourceStatus != nil {
					if relayStatus := queryResp.SourceStatus.RelayStatus; relayStatus != nil {
						if err := extractWorkerError(relayStatus.Result); err != nil {
							return queryResp, err
						}
						if relayStatus.Stage == expect {
							return queryResp, nil
						}
					} else {
						return queryResp, terror.ErrMasterOperRespNotSuccess.Generate("relay is disabled for this source")
					}
				}
			}
			log.L().Info("fail to get expect operation result", zap.Int("retryNum", num), zap.String("task", taskName),
				zap.String("source", sourceID), zap.Stringer("expect", expect), zap.Stringer("resp", queryResp))
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(retryInterval):
		}
	}

	return nil, terror.ErrMasterFailToGetExpectResult
}

func (s *Server) handleOperationResult(ctx context.Context, cli *scheduler.Worker, taskName, sourceID string, req interface{}) *pb.CommonWorkerResponse {
	if cli == nil {
		return errorCommonWorkerResponse(sourceID+" relevant worker-client not found", sourceID, "")
	}
	var response *pb.CommonWorkerResponse
	queryResp, err := s.waitOperationOk(ctx, cli, taskName, sourceID, req)
	if err != nil {
		response = errorCommonWorkerResponse(errors.ErrorStack(err), sourceID, cli.BaseInfo().Name)
	} else {
		response = &pb.CommonWorkerResponse{
			Result: true,
			Source: queryResp.SourceStatus.Source,
			Worker: queryResp.SourceStatus.Worker,
		}
	}
	return response
}

func sortCommonWorkerResults(sourceRespCh chan *pb.CommonWorkerResponse) []*pb.CommonWorkerResponse {
	sourceRespMap := make(map[string]*pb.CommonWorkerResponse, cap(sourceRespCh))
	sources := make([]string, 0, cap(sourceRespCh))
	for len(sourceRespCh) > 0 {
		sourceResp := <-sourceRespCh
		sourceRespMap[sourceResp.Source] = sourceResp
		sources = append(sources, sourceResp.Source)
	}
	// TODO: simplify logic of response sort
	sort.Strings(sources)
	sourceResps := make([]*pb.CommonWorkerResponse, 0, len(sources))
	for _, source := range sources {
		sourceResps = append(sourceResps, sourceRespMap[source])
	}
	return sourceResps
}

func (s *Server) getSourceRespsAfterOperation(ctx context.Context, taskName string, sources, workers []string, req interface{}) []*pb.CommonWorkerResponse {
	sourceRespCh := make(chan *pb.CommonWorkerResponse, len(sources))
	var wg sync.WaitGroup
	for i, source := range sources {
		wg.Add(1)
		var worker string
		if i < len(workers) {
			worker = workers[i]
		}
		go s.ap.Emit(ctx, 0, func(args ...interface{}) {
			defer wg.Done()
			source1, _ := args[0].(string)
			worker1, _ := args[1].(string)
			workerCli := s.scheduler.GetWorkerBySource(source1)
			if workerCli == nil && worker1 != "" {
				workerCli = s.scheduler.GetWorkerByName(worker1)
			}
			sourceResp := s.handleOperationResult(ctx, workerCli, taskName, source1, req)
			sourceResp.Source = source1 // may return other source's ID during stop worker
			sourceRespCh <- sourceResp
		}, func(args ...interface{}) {
			defer wg.Done()
			source1, _ := args[0].(string)
			worker1, _ := args[1].(string)
			sourceRespCh <- errorCommonWorkerResponse(terror.ErrMasterNoEmitToken.Generate(source1).Error(), source1, worker1)
		}, source, worker)
	}
	wg.Wait()
	return sortCommonWorkerResults(sourceRespCh)
}
