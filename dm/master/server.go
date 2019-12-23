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
	"github.com/pingcap/dm/pkg/conn"
	"io"
	"net/http"
	"path"
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
	"github.com/pingcap/dm/dm/master/coordinator"
	operator "github.com/pingcap/dm/dm/master/sql-operator"
	"github.com/pingcap/dm/dm/master/workerrpc"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/election"
	"github.com/pingcap/dm/pkg/etcdutil"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/tracing"
	"github.com/pingcap/dm/syncer"
)

const (
	// the session's TTL in seconds for leader election.
	// NOTE: select this value carefully when adding a mechanism relying on leader election.
	electionTTL = 60
	// the DM-master leader election key prefix
	// DM-master cluster : etcd cluster = 1 : 1 now.
	electionKey        = "/dm-master/leader"
	workerRegisterPath = "/dm-worker/r/"
)

var (
	fetchDDLInfoRetryTimeout = 5 * time.Second
	etcdTimeouit             = 10 * time.Second
)

// Server handles RPC requests for dm-master
type Server struct {
	sync.Mutex

	cfg *Config

	// the embed etcd server, and the gRPC/HTTP API server also attached to it.
	etcd *embed.Etcd

	etcdClient *clientv3.Client
	election   *election.Election

	// WaitGroup for background functions.
	bgFunWg sync.WaitGroup

	// dm-worker-ID(host:ip) -> dm-worker client management
	coordinator   *coordinator.Coordinator
	workerClients map[string]workerrpc.Client

	// task-name -> worker-list
	taskSources map[string][]string

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
		coordinator:       coordinator.NewCoordinator(),
		workerClients:     make(map[string]workerrpc.Client),
		taskSources:       make(map[string][]string),
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
	s.election, err = election.NewElection(ctx, s.etcdClient, electionTTL, electionKey, s.cfg.Name)
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
	s.coordinator.Init(s.etcdClient)
	go func() {
		defer s.bgFunWg.Done()
		s.coordinator.ObserveWorkers(ctx, s.etcdClient)
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

func errorCommonWorkerResponse(msg string, worker string) *pb.CommonWorkerResponse {
	return &pb.CommonWorkerResponse{
		Result: false,
		Worker: worker,
		Msg:    msg,
	}
}

// RegisterWorker registers the worker to the master, and all the worker will be store in the path:
// key:   /dm-worker/address
// value: name
func (s *Server) RegisterWorker(ctx context.Context, req *pb.RegisterWorkerRequest) (*pb.RegisterWorkerResponse, error) {
	k := path.Join(workerRegisterPath, req.Address)
	v := req.Name
	ectx, cancel := context.WithTimeout(ctx, etcdTimeouit)
	defer cancel()
	resp, err := s.etcdClient.Txn(ectx).
		If(clientv3.Compare(clientv3.CreateRevision(k), "=", 0)).
		Then(clientv3.OpPut(k, v)).
		Else(clientv3.OpGet(k)).
		Commit()
	if err != nil {
		return nil, err
	}
	if !resp.Succeeded {
		if len(resp.Responses) == 0 {
			return nil, errors.Errorf("the response kv is invalid length, request key: %s", k)
		}
		kv := resp.Responses[0].GetResponseRange().GetKvs()[0]
		address, name := strings.TrimPrefix(string(kv.Key), workerRegisterPath), string(kv.Value)
		if name != req.Name {
			msg := fmt.Sprintf("the address %s already registered with name %s", address, name)
			respWorker := &pb.RegisterWorkerResponse{
				Result: false,
				Msg:    msg,
			}
			log.L().Error(msg)
			return respWorker, nil
		}
	}
	fmt.Println("=======Register a worker")
	s.coordinator.AddWorker(req.Name, req.Address)
	log.L().Info("register worker successfully", zap.String("name", req.Name), zap.String("address", req.Address))
	respWorker := &pb.RegisterWorkerResponse{
		Result: true,
	}
	return respWorker, nil
}

// StartTask implements MasterServer.StartTask
func (s *Server) StartTask(ctx context.Context, req *pb.StartTaskRequest) (*pb.StartTaskResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "StartTask"))

	cfg, stCfgs, err := s.generateSubTask(ctx, req.Task)
	if err != nil {
		return &pb.StartTaskResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}, nil
	}
	log.L().Info("", zap.String("task name", cfg.Name), zap.Stringer("task", cfg), zap.String("request", "StartTask"))

	workerRespCh := make(chan *pb.CommonWorkerResponse, len(stCfgs))
	var wg sync.WaitGroup
	subSourceIDs := make([]string, 0, len(stCfgs))
	for _, stCfg := range stCfgs {
		subSourceIDs = append(subSourceIDs, stCfg.SourceID)
		wg.Add(1)
		go s.ap.Emit(ctx, 0, func(args ...interface{}) {
			defer wg.Done()
			cfg, _ := args[0].(*config.SubTaskConfig)
			worker, stCfgToml, _, err := s.taskConfigArgsExtractor(cfg)
			if err != nil {
				workerRespCh <- errorCommonWorkerResponse(err.Error(), cfg.SourceID)
				return
			}
			request := &workerrpc.Request{
				Type:         workerrpc.CmdStartSubTask,
				StartSubTask: &pb.StartSubTaskRequest{Task: stCfgToml},
			}
			resp, err := worker.SendRequest(ctx, request, s.cfg.RPCTimeout)
			if err != nil {
				resp = &workerrpc.Response{
					Type:         workerrpc.CmdStartSubTask,
					StartSubTask: errorCommonWorkerResponse(err.Error(), cfg.SourceID),
				}
			}
			resp.StartSubTask.Worker = cfg.SourceID
			workerRespCh <- resp.StartSubTask
		}, func(args ...interface{}) {
			defer wg.Done()
			cfg, _ := args[0].(*config.SubTaskConfig)
			worker, _, _, err := s.taskConfigArgsExtractor(cfg)
			if err != nil {
				workerRespCh <- errorCommonWorkerResponse(err.Error(), cfg.SourceID)
				return
			}
			workerRespCh <- errorCommonWorkerResponse(terror.ErrMasterNoEmitToken.Generate(worker.Address()).Error(), cfg.SourceID)
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
	s.taskSources[cfg.Name] = subSourceIDs

	return &pb.StartTaskResponse{
		Result:  true,
		Workers: workerResps,
	}, nil
}

// OperateTask implements MasterServer.OperateTask
func (s *Server) OperateTask(ctx context.Context, req *pb.OperateTaskRequest) (*pb.OperateTaskResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "OperateTask"))

	resp := &pb.OperateTaskResponse{
		Op:     req.Op,
		Result: false,
	}

	sources := s.getTaskResources(req.Name)
	workerRespCh := make(chan *pb.OperateSubTaskResponse, len(sources))

	handleErr := func(err error, worker string) {
		log.L().Error("response error", zap.Error(err))
		workerResp := &pb.OperateSubTaskResponse{
			Op:     req.Op,
			Result: false,
			Worker: worker,
			Msg:    err.Error(),
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
	for _, source := range sources {
		wg.Add(1)
		go s.ap.Emit(ctx, 0, func(args ...interface{}) {
			defer wg.Done()
			sourceID, _ := args[0].(string)
			worker1 := s.coordinator.GetWorkerBySourceID(sourceID)
			if worker1 == nil {
				err := terror.ErrMasterWorkerArgsExtractor.Generatef("%s relevant worker-client not found", sourceID)
				handleErr(err, sourceID)
				return
			}
			resp, err := worker1.SendRequest(ctx, subReq, s.cfg.RPCTimeout)
			if err != nil {
				resp = &workerrpc.Response{
					Type: workerrpc.CmdOperateSubTask,
					OperateSubTask: &pb.OperateSubTaskResponse{
						Op:     req.Op,
						Result: false,
						Msg:    err.Error(),
					},
				}
			}
			resp.OperateSubTask.Worker = sourceID
			workerRespCh <- resp.OperateSubTask
		}, func(args ...interface{}) {
			defer wg.Done()
			sourceID, _ := args[0].(string)
			handleErr(terror.ErrMasterNoEmitToken.Generate(sourceID), sourceID)
		}, source)
	}
	wg.Wait()

	workerRespMap := make(map[string]*pb.OperateSubTaskResponse, len(sources))
	for len(workerRespCh) > 0 {
		workerResp := <-workerRespCh
		workerRespMap[workerResp.Worker] = workerResp
	}

	workerResps := make([]*pb.OperateSubTaskResponse, 0, len(sources))
	for _, worker := range sources {
		workerResps = append(workerResps, workerRespMap[worker])
	}

	if req.Op == pb.TaskOp_Stop {
		// remove (partial / all) workers for a task
		s.removeTaskWorkers(req.Name, sources)
	}

	resp.Result = true
	resp.Workers = workerResps

	return resp, nil
}

// UpdateTask implements MasterServer.UpdateTask
func (s *Server) UpdateTask(ctx context.Context, req *pb.UpdateTaskRequest) (*pb.UpdateTaskResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "UpdateTask"))

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
				workerRespCh <- errorCommonWorkerResponse("worker not found in task's config or deployment config", source)
			}
		}
	}

	var wg sync.WaitGroup
	for _, stCfg := range stCfgs {
		wg.Add(1)
		go s.ap.Emit(ctx, 0, func(args ...interface{}) {
			defer wg.Done()
			cfg, _ := args[0].(*config.SubTaskConfig)
			worker, stCfgToml, _, err := s.taskConfigArgsExtractor(cfg)
			if err != nil {
				workerRespCh <- errorCommonWorkerResponse(err.Error(), cfg.SourceID)
				return
			}
			request := &workerrpc.Request{
				Type:          workerrpc.CmdUpdateSubTask,
				UpdateSubTask: &pb.UpdateSubTaskRequest{Task: stCfgToml},
			}
			resp, err := worker.SendRequest(ctx, request, s.cfg.RPCTimeout)
			if err != nil {
				resp = &workerrpc.Response{
					Type:          workerrpc.CmdUpdateSubTask,
					UpdateSubTask: errorCommonWorkerResponse(err.Error(), cfg.SourceID),
				}
			}
			resp.UpdateSubTask.Worker = cfg.SourceID
			workerRespCh <- resp.UpdateSubTask
		}, func(args ...interface{}) {
			defer wg.Done()
			cfg, _ := args[0].(*config.SubTaskConfig)
			worker, _, _, err := s.taskConfigArgsExtractor(cfg)
			if err != nil {
				workerRespCh <- errorCommonWorkerResponse(err.Error(), cfg.SourceID)
				return
			}
			workerRespCh <- errorCommonWorkerResponse(terror.ErrMasterNoEmitToken.Generate(worker.Address()).Error(), cfg.SourceID)
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

type hasWokers interface {
	GetSources() []string
	GetName() string
}

func extractWorkers(s *Server, req hasWokers) ([]string, error) {
	workers := make([]string, 0, len(s.coordinator.GetAllWorkers()))

	if len(req.GetSources()) > 0 {
		// query specified dm-workers
		invalidWorkers := make([]string, 0, len(req.GetSources()))
		for _, source := range req.GetSources() {
			w := s.coordinator.GetWorkerBySourceID(source)
			if w == nil || w.State() == coordinator.WorkerClosed {
				invalidWorkers = append(invalidWorkers, source)
			}
		}
		if len(invalidWorkers) > 0 {
			return nil, errors.Errorf("%s relevant worker-client not found", strings.Join(invalidWorkers, ", "))
		}
		workers = req.GetSources()
	} else if len(req.GetName()) > 0 {
		// query specified task's workers
		workers = s.getTaskResources(req.GetName())
		if len(workers) == 0 {
			return nil, errors.Errorf("task %s has no workers or not exist, can try `refresh-worker-tasks` cmd first", req.GetName())
		}
	} else {
		// query all workers
		log.L().Info("get workers")
		for source := range s.coordinator.GetRunningMysqlSource() {
			workers = append(workers, source)
		}
	}
	return workers, nil
}

// QueryStatus implements MasterServer.QueryStatus
func (s *Server) QueryStatus(ctx context.Context, req *pb.QueryStatusListRequest) (*pb.QueryStatusListResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "QueryStatus"))
	sources, err := extractWorkers(s, req)
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
		workerRespMap[workerResp.Worker] = workerResp
	}

	sort.Strings(sources)
	workerResps := make([]*pb.QueryStatusResponse, 0, len(sources))
	for _, worker := range sources {
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
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "QueryError"))

	sources, err := extractWorkers(s, req)
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
		workerRespMap[workerResp.Worker] = workerResp
	}

	sort.Strings(sources)
	workerResps := make([]*pb.QueryErrorResponse, 0, len(sources))
	for _, worker := range sources {
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
	log.L().Info("", zap.String("lock ID", req.ID), zap.Stringer("payload", req), zap.String("request", "UnlockDDLLock"))

	workerResps, err := s.resolveDDLLock(ctx, req.ID, req.ReplaceOwner, req.Sources)
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

	request := &workerrpc.Request{
		Type: workerrpc.CmdBreakDDLLock,
		BreakDDLLock: &pb.BreakDDLLockRequest{
			Task:         req.Task,
			RemoveLockID: req.RemoveLockID,
			ExecDDL:      req.ExecDDL,
			SkipDDL:      req.SkipDDL,
		},
	}

	workerRespCh := make(chan *pb.CommonWorkerResponse, len(req.Sources))
	var wg sync.WaitGroup
	for _, source := range req.Sources {
		wg.Add(1)
		go func(sourceID string) {
			defer wg.Done()
			worker := s.coordinator.GetWorkerBySourceID(sourceID)
			if worker == nil || worker.State() == coordinator.WorkerClosed {
				workerRespCh <- errorCommonWorkerResponse(fmt.Sprintf("worker %s relevant worker-client not found", sourceID), sourceID)
				return
			}
			resp, err := worker.SendRequest(ctx, request, s.cfg.RPCTimeout)
			workerResp := &pb.CommonWorkerResponse{}
			if err != nil {
				workerResp = errorCommonWorkerResponse(errors.ErrorStack(err), sourceID)
			} else {
				workerResp = resp.BreakDDLLock
			}
			workerResp.Worker = sourceID
			workerRespCh <- workerResp
		}(source)
	}
	wg.Wait()

	workerRespMap := make(map[string]*pb.CommonWorkerResponse, len(req.Sources))
	for len(workerRespCh) > 0 {
		workerResp := <-workerRespCh
		workerRespMap[workerResp.Worker] = workerResp
	}

	sort.Strings(req.Sources)
	workerResps := make([]*pb.CommonWorkerResponse, 0, len(req.Sources))
	for _, worker := range req.Sources {
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
	cli := s.coordinator.GetWorkerBySourceID(req.Worker)
	if cli == nil {
		resp.Msg = fmt.Sprintf("worker %s client not found in %v", req.Worker, s.coordinator.GetAllWorkers())
		return resp, nil
	}
	response, err := cli.SendRequest(ctx, subReq, s.cfg.RPCTimeout)
	workerResp := &pb.CommonWorkerResponse{}
	if err != nil {
		workerResp = errorCommonWorkerResponse(errors.ErrorStack(err), req.Worker)
	} else {
		workerResp = response.HandleSubTaskSQLs
	}
	resp.Workers = []*pb.CommonWorkerResponse{workerResp}
	resp.Result = true
	return resp, nil
}

// PurgeWorkerRelay implements MasterServer.PurgeWorkerRelay
func (s *Server) PurgeWorkerRelay(ctx context.Context, req *pb.PurgeWorkerRelayRequest) (*pb.PurgeWorkerRelayResponse, error) {
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

	workerRespCh := make(chan *pb.CommonWorkerResponse, len(req.Sources))
	var wg sync.WaitGroup
	for _, source := range req.Sources {
		wg.Add(1)
		go func(source string) {
			defer wg.Done()
			cli := s.coordinator.GetWorkerBySourceID(source)
			if cli == nil {
				workerRespCh <- errorCommonWorkerResponse(fmt.Sprintf("worker %s relevant worker-client not found", source), source)
				return
			}
			resp, err := cli.SendRequest(ctx, workerReq, s.cfg.RPCTimeout)
			workerResp := &pb.CommonWorkerResponse{}
			if err != nil {
				workerResp = errorCommonWorkerResponse(errors.ErrorStack(err), source)
			} else {
				workerResp = resp.PurgeRelay
			}
			workerResp.Worker = source
			workerRespCh <- workerResp
		}(source)
	}
	wg.Wait()

	workerRespMap := make(map[string]*pb.CommonWorkerResponse, len(req.Sources))
	for len(workerRespCh) > 0 {
		workerResp := <-workerRespCh
		workerRespMap[workerResp.Worker] = workerResp
	}

	sort.Strings(req.Sources)
	workerResps := make([]*pb.CommonWorkerResponse, 0, len(req.Sources))
	for _, worker := range req.Sources {
		workerResps = append(workerResps, workerRespMap[worker])
	}

	return &pb.PurgeWorkerRelayResponse{
		Result:  true,
		Workers: workerResps,
	}, nil
}

// SwitchWorkerRelayMaster implements MasterServer.SwitchWorkerRelayMaster
func (s *Server) SwitchWorkerRelayMaster(ctx context.Context, req *pb.SwitchWorkerRelayMasterRequest) (*pb.SwitchWorkerRelayMasterResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "SwitchWorkerRelayMaster"))

	workerRespCh := make(chan *pb.CommonWorkerResponse, len(req.Sources))

	handleErr := func(err error, worker string) {
		log.L().Error("response error", zap.Error(err))
		resp := errorCommonWorkerResponse(errors.ErrorStack(err), worker)
		workerRespCh <- resp
	}

	var wg sync.WaitGroup
	for _, source := range req.Sources {
		wg.Add(1)
		go s.ap.Emit(ctx, 0, func(args ...interface{}) {
			defer wg.Done()
			sourceID, _ := args[0].(string)
			cli := s.coordinator.GetWorkerBySourceID(sourceID)
			if cli == nil {
				err := terror.ErrMasterWorkerArgsExtractor.Generatef("%s relevant worker-client not found", sourceID)
				handleErr(err, sourceID)
				return
			}
			request := &workerrpc.Request{
				Type:              workerrpc.CmdSwitchRelayMaster,
				SwitchRelayMaster: &pb.SwitchRelayMasterRequest{},
			}
			resp, err := cli.SendRequest(ctx, request, s.cfg.RPCTimeout)
			workerResp := &pb.CommonWorkerResponse{}
			if err != nil {
				workerResp = errorCommonWorkerResponse(errors.ErrorStack(err), sourceID)
			} else {
				workerResp = resp.SwitchRelayMaster
			}
			workerResp.Worker = sourceID
			workerRespCh <- workerResp
		}, func(args ...interface{}) {
			defer wg.Done()
			sourceID, _ := args[0].(string)
			workerRespCh <- errorCommonWorkerResponse(terror.ErrMasterNoEmitToken.Generate(sourceID).Error(), sourceID)
		}, source)
	}
	wg.Wait()

	workerRespMap := make(map[string]*pb.CommonWorkerResponse, len(req.Sources))
	for len(workerRespCh) > 0 {
		workerResp := <-workerRespCh
		workerRespMap[workerResp.Worker] = workerResp
	}

	sort.Strings(req.Sources)
	workerResps := make([]*pb.CommonWorkerResponse, 0, len(req.Sources))
	for _, worker := range req.Sources {
		workerResps = append(workerResps, workerRespMap[worker])
	}

	return &pb.SwitchWorkerRelayMasterResponse{
		Result:  true,
		Workers: workerResps,
	}, nil
}

// OperateWorkerRelayTask implements MasterServer.OperateWorkerRelayTask
func (s *Server) OperateWorkerRelayTask(ctx context.Context, req *pb.OperateWorkerRelayRequest) (*pb.OperateWorkerRelayResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "OperateWorkerRelayTask"))

	request := &workerrpc.Request{
		Type:         workerrpc.CmdOperateRelay,
		OperateRelay: &pb.OperateRelayRequest{Op: req.Op},
	}
	workerRespCh := make(chan *pb.OperateRelayResponse, len(req.Sources))
	var wg sync.WaitGroup
	for _, source := range req.Sources {
		wg.Add(1)
		go func(source string) {
			defer wg.Done()
			cli := s.coordinator.GetWorkerBySourceID(source)
			if cli == nil {
				workerResp := &pb.OperateRelayResponse{
					Op:     req.Op,
					Result: false,
					Worker: source,
					Msg:    fmt.Sprintf("%s relevant worker-client not found", source),
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
			workerResp.Worker = source
			workerRespCh <- workerResp
		}(source)
	}
	wg.Wait()

	workerRespMap := make(map[string]*pb.OperateRelayResponse, len(req.Sources))
	for len(workerRespCh) > 0 {
		workerResp := <-workerRespCh
		workerRespMap[workerResp.Worker] = workerResp
	}

	sort.Strings(req.Sources)
	workerResps := make([]*pb.OperateRelayResponse, 0, len(req.Sources))
	for _, worker := range req.Sources {
		workerResps = append(workerResps, workerRespMap[worker])
	}

	return &pb.OperateWorkerRelayResponse{
		Result:  true,
		Workers: workerResps,
	}, nil
}

// replaceTaskWorkers replaces the whole task-workers mapper
func (s *Server) replaceTaskWorkers(taskWorkers map[string][]string) {
	for task := range taskWorkers {
		sort.Strings(taskWorkers[task])
	}
	s.Lock()
	defer s.Unlock()
	s.taskSources = taskWorkers
}

// removeTaskWorkers remove (partial / all) workers for a task
func (s *Server) removeTaskWorkers(task string, workers []string) {
	toRemove := make(map[string]struct{})
	for _, w := range workers {
		toRemove[w] = struct{}{}
	}

	s.Lock()
	defer s.Unlock()
	if _, ok := s.taskSources[task]; !ok {
		log.L().Warn("not found workers", zap.String("task", task))
		return
	}
	remain := make([]string, 0, len(s.taskSources[task]))
	for _, worker := range s.taskSources[task] {
		if _, ok := toRemove[worker]; !ok {
			remain = append(remain, worker)
		}
	}
	if len(remain) == 0 {
		delete(s.taskSources, task)
		log.L().Info("remove task from taskWorker", zap.String("task", task))
	} else {
		s.taskSources[task] = remain
		log.L().Info("update workers of task", zap.String("task", task), zap.Strings("reamin workers", remain))
	}
}

// getTaskResources gets workers relevant to specified task
func (s *Server) getTaskResources(task string) []string {
	s.Lock()
	defer s.Unlock()
	workers, ok := s.taskSources[task]
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
			sourceID, _ := args[0].(string)
			cli := s.coordinator.GetWorkerBySourceID(sourceID)
			if cli == nil {
				err := terror.ErrMasterWorkerArgsExtractor.Generatef("%s relevant worker-client not found", sourceID)
				handleErr(err, sourceID)
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
			workerStatus.Worker = sourceID
			workerRespCh <- workerStatus
		}, func(args ...interface{}) {
			defer wg.Done()
			sourceID, _ := args[0].(string)
			handleErr(terror.ErrMasterNoEmitToken.Generate(sourceID), sourceID)
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
			sourceID, _ := args[0].(string)
			cli := s.coordinator.GetWorkerBySourceID(sourceID)
			if cli == nil {
				err := terror.ErrMasterWorkerArgsExtractor.Generatef("%s relevant worker-client not found", sourceID)
				handleErr(err, sourceID)
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
			workerError.Worker = sourceID
			workerRespCh <- workerError
		}, func(args ...interface{}) {
			defer wg.Done()
			sourceID, _ := args[0].(string)
			handleErr(terror.ErrMasterNoEmitToken.Generate(sourceID), sourceID)
		}, worker)
	}
	wg.Wait()
	return workerRespCh
}

// updateTaskWorkers fetches task-workers mapper from dm-workers and update s.taskSources
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
	workers := make([]string, 0, len(s.coordinator.GetAllWorkers()))
	for worker := range s.coordinator.GetAllWorkers() {
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

// fetchWorkerDDLInfo fetches DDL info from all dm-workers
// and sends DDL lock info back to dm-workers
func (s *Server) fetchWorkerDDLInfo(ctx context.Context) {
	var wg sync.WaitGroup

	request := &workerrpc.Request{Type: workerrpc.CmdFetchDDLInfo}
	for source, w := range s.coordinator.GetRunningMysqlSource() {
		wg.Add(1)
		go func(source string, cli *coordinator.Worker) {
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
						log.L().Error("create FetchDDLInfo stream", zap.String("worker", source), log.ShortError(err))
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
							log.L().Error("receive ddl info", zap.String("worker", source), log.ShortError(err))
							doRetry = true
							break
						}
						log.L().Info("receive ddl info", zap.Stringer("ddl info", in), zap.String("worker", source))

						workers := s.getTaskResources(in.Task)
						if len(workers) == 0 {
							// should happen only when starting and before updateTaskWorkers return
							log.L().Error("try to sync shard DDL, but with no workers", zap.String("task", in.Task))
							doRetry = true
							break
						}
						if !s.containWorker(workers, source) {
							// should not happen
							log.L().Error("try to sync shard DDL, but worker is not in workers", zap.String("task", in.Task), zap.String("worker", source), zap.Strings("workers", workers))
							doRetry = true
							break
						}

						lockID, synced, remain, err := s.lockKeeper.TrySync(in.Task, in.Schema, in.Table, source, in.DDLs, workers)
						if err != nil {
							log.L().Error("fail to sync lock", zap.String("worker", source), log.ShortError(err))
							doRetry = true
							break
						}

						out := &pb.DDLLockInfo{
							Task: in.Task,
							ID:   lockID,
						}
						err = stream.Send(out)
						if err != nil {
							log.L().Error("fail to send ddl lock info", zap.Stringer("ddl lock info", out), zap.String("worker", source), log.ShortError(err))
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

		}(source, w)
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
	cli := s.coordinator.GetWorkerBySourceID(owner)
	if cli == nil {
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
		ownerResp = errorCommonWorkerResponse(errors.ErrorStack(err), owner)
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
			cli := s.coordinator.GetWorkerBySourceID(worker)
			if cli == nil {
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
	worker := req.Worker
	content := req.Config
	cli := s.coordinator.GetWorkerBySourceID(worker)
	if cli == nil {
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
func (s *Server) getWorkerConfigs(ctx context.Context, workers []*config.MySQLInstance) map[string]config.DBConfig {
	cfgs := make(map[string]config.DBConfig)
	for _, w := range workers {
		if cfg := s.coordinator.GetConfigBySourceID(w.SourceID); cfg != nil {
			cfgs[w.SourceID] = cfg.From
		}
	}
	return cfgs
}

// MigrateWorkerRelay migrates dm-woker relay unit
func (s *Server) MigrateWorkerRelay(ctx context.Context, req *pb.MigrateWorkerRelayRequest) (*pb.CommonWorkerResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "MigrateWorkerRelay"))
	worker := req.Worker
	binlogPos := req.BinlogPos
	binlogName := req.BinlogName
	cli := s.coordinator.GetWorkerBySourceID(worker)
	if cli == nil {
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

func makeMysqlTaskResponse(err error) (*pb.MysqlTaskResponse, error) {
	return &pb.MysqlTaskResponse{
		Result: false,
		Msg:    errors.ErrorStack(err),
	}, nil
}

// OperateMysqlWorker will create or update a Worker
func (s *Server) OperateMysqlWorker(ctx context.Context, req *pb.MysqlTaskRequest) (*pb.MysqlTaskResponse, error) {
	cfg := config.NewWorkerConfig()
	if err := cfg.Parse(req.Config); err != nil {
		return makeMysqlTaskResponse(err)
	}

	dbConfig, err := cfg.GenerateDBConfig()
	if err != nil {
		return makeMysqlTaskResponse(err)
	}
	fromDB, err := conn.DefaultDBProvider.Apply(*dbConfig)
	if err != nil {
		return makeMysqlTaskResponse(err)
	}
	if err := cfg.Adjust(fromDB.DB); err != nil {
		return makeMysqlTaskResponse(err)
	}
	var resp *pb.MysqlTaskResponse
	if req.Op == pb.WorkerOp_StartWorker {
		w := s.coordinator.GetWorkerBySourceID(cfg.SourceID)
		if w != nil {
			return &pb.MysqlTaskResponse{
				Result: false,
				Msg:    "Create worker failed. worker has been started",
			}, nil
		}
		w, addr := s.coordinator.GetFreeWorkerForSource(cfg.SourceID)
		if w == nil {
			if addr != "" {
				return &pb.MysqlTaskResponse{
					Result: false,
					Msg:    fmt.Sprintf("Create worker failed. the same source has been started in worker: %v", addr),
				}, nil
			}
			return &pb.MysqlTaskResponse{
				Result: false,
				Msg:    "Create worker failed. no free worker could start mysql task",
			}, nil
		}
		resp, err = w.OperateMysqlTask(ctx, req, s.cfg.RPCTimeout)
		if err != nil {
			s.coordinator.HandleStartedWorker(w, cfg, false)
			return &pb.MysqlTaskResponse{
				Result: false,
				Msg:    errors.ErrorStack(err),
			}, nil
		}
		s.coordinator.HandleStartedWorker(w, cfg, true)
	} else if req.Op == pb.WorkerOp_UpdateConfig {
		w := s.coordinator.GetWorkerBySourceID(cfg.SourceID)
		if w == nil {
			return &pb.MysqlTaskResponse{
				Result: false,
				Msg:    "Update worker config failed. worker has not been started",
			}, nil
		}
		if resp, err = w.OperateMysqlTask(ctx, req, s.cfg.RPCTimeout); err != nil {
			return &pb.MysqlTaskResponse{
				Result: false,
				Msg:    errors.ErrorStack(err),
			}, nil
		}
	} else {
		w := s.coordinator.GetWorkerBySourceID(cfg.SourceID)
		if w == nil {
			return &pb.MysqlTaskResponse{
				Result: false,
				Msg:    "Stop worker failed. worker has not been started",
			}, nil
		}
		if resp, err = w.OperateMysqlTask(ctx, req, s.cfg.RPCTimeout); err != nil {
			return &pb.MysqlTaskResponse{
				Result: false,
				Msg:    errors.ErrorStack(err),
			}, nil
		}
		s.coordinator.HandleStoppedWorker(w, cfg)
	}
	return &pb.MysqlTaskResponse{
		Result: resp.Result,
		Msg:    resp.Msg,
	}, nil
}

func (s *Server) generateSubTask(ctx context.Context, task string) (*config.TaskConfig, []*config.SubTaskConfig, error) {
	cfg := config.NewTaskConfig()
	err := cfg.Decode(task)
	if err != nil {
		return nil, nil, terror.WithClass(err, terror.ClassDMMaster)
	}

	sourceCfgs := s.getWorkerConfigs(ctx, cfg.MySQLInstances)
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
	maxRetryNum = 30
)

// taskConfigArgsExtractor extracts SubTaskConfig from args and returns its relevant
// grpc client, worker id (host:port), subtask config in toml, task name and error
func (s *Server) taskConfigArgsExtractor(cfg *config.SubTaskConfig) (*coordinator.Worker, string, string, error) {
	handleErr := func(err error) error {
		log.L().Error("response", zap.Error(err))
		return err
	}

	worker := s.coordinator.GetWorkerBySourceID(cfg.SourceID)
	if worker == nil {
		return nil, "", "", handleErr(terror.ErrMasterTaskConfigExtractor.Generatef("%s relevant worker-client not found", worker))
	}

	cfgToml, err := cfg.Toml()
	if err != nil {
		return nil, "", "", handleErr(err)
	}

	return worker, cfgToml, cfg.Name, nil
}

// workerArgsExtractor extracts worker from args and returns its relevant
// grpc client, worker id (host:port) and error
func (s *Server) workerArgsExtractor(source string) (*coordinator.Worker, error) {
	log.L().Info("Debug get worker", zap.String("source-id", source))
	cli := s.coordinator.GetWorkerBySourceID(source)
	if cli == nil {
		return nil, terror.ErrMasterWorkerArgsExtractor.Generatef("%s relevant worker-client not found", source)
	}
	return cli, nil
}
