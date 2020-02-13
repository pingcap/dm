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
	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/master/coordinator"
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
	fetchDDLInfoRetryTimeout = 5 * time.Second
	etcdTimeouit             = 10 * time.Second
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
	coordinator   *coordinator.Coordinator
	workerClients map[string]workerrpc.Client

	// task-name -> source-list
	taskSources map[string][]string

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
	server := Server{
		cfg:               cfg,
		coordinator:       coordinator.NewCoordinator(),
		workerClients:     make(map[string]workerrpc.Client),
		taskSources:       make(map[string][]string),
		sqlOperatorHolder: operator.NewHolder(),
		idGen:             tracing.NewIDGen(),
		ap:                NewAgentPool(&RateLimitConfig{rate: cfg.RPCRateLimit, burst: cfg.RPCRateBurst}),
	}
	logger := log.L()
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

func (s *Server) recoverSubTask() error {
	ectx, cancel := context.WithTimeout(s.etcdClient.Ctx(), etcdTimeouit)
	defer cancel()
	resp, err := s.etcdClient.Get(ectx, common.UpstreamSubTaskKeyAdapter.Path(), clientv3.WithPrefix())
	if err != nil {
		return err
	}
	for _, kv := range resp.Kvs {
		infos, err := common.UpstreamSubTaskKeyAdapter.Decode(string(kv.Key))
		if err != nil {
			return terror.Annotate(err, "decode upstream subtask key from etcd failed")
		}
		sourceID := infos[0]
		taskName := infos[1]
		if sources, ok := s.taskSources[taskName]; ok {
			s.taskSources[taskName] = append(sources, sourceID)
		} else {
			srcs := make([]string, 1)
			srcs = append(srcs, sourceID)
			s.taskSources[taskName] = srcs
		}
	}
	return nil
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

func errorCommonWorkerResponse(msg string, source string) *pb.CommonWorkerResponse {
	return &pb.CommonWorkerResponse{
		Result: false,
		Source: source,
		Msg:    msg,
	}
}

// RegisterWorker registers the worker to the master, and all the worker will be store in the path:
// key:   /dm-worker/r/address
// value: name
func (s *Server) RegisterWorker(ctx context.Context, req *pb.RegisterWorkerRequest) (*pb.RegisterWorkerResponse, error) {
	isLeader, needForward := s.isLeaderAndNeedForward()
	if !isLeader {
		if needForward {
			return s.leaderClient.RegisterWorker(ctx, req)
		}
		return nil, terror.ErrMasterRequestIsNotForwardToLeader
	}

	if !s.coordinator.IsStarted() {
		respWorker := &pb.RegisterWorkerResponse{
			Result: false,
			Msg:    "coordinator not started, may not leader",
		}
		return respWorker, nil
	}
	k := common.WorkerRegisterKeyAdapter.Encode(req.Address)
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
		kvs, err := common.WorkerRegisterKeyAdapter.Decode(string(kv.Key))
		if err != nil {
			log.L().Error("decode worker register key from etcd failed", zap.Error(err))
			return &pb.RegisterWorkerResponse{
				Result: false,
				Msg:    errors.ErrorStack(err),
			}, nil
		}
		address := kvs[0]
		name := string(kv.Value)
		if name != req.Name {
			msg := fmt.Sprintf("the address %s already registered with name %s", address, name)
			respSource := &pb.RegisterWorkerResponse{
				Result: false,
				Msg:    msg,
			}
			log.L().Error(msg)
			return respSource, nil
		}
	}
	s.coordinator.AddWorker(req.Name, req.Address, nil)
	log.L().Info("register worker successfully", zap.String("name", req.Name), zap.String("address", req.Address))
	respWorker := &pb.RegisterWorkerResponse{
		Result: true,
	}
	return respWorker, nil
}

// OfflineWorker removes info of the worker which has been Closed, and all the worker are store in the path:
// key:   /dm-worker/r/address
// value: name
func (s *Server) OfflineWorker(ctx context.Context, req *pb.OfflineWorkerRequest) (*pb.OfflineWorkerResponse, error) {
	isLeader, needForward := s.isLeaderAndNeedForward()
	if !isLeader {
		if needForward {
			return s.leaderClient.OfflineWorker(ctx, req)
		}
		return nil, terror.ErrMasterRequestIsNotForwardToLeader
	}

	if !s.coordinator.IsStarted() {
		respWorker := &pb.OfflineWorkerResponse{
			Result: false,
			Msg:    "coordinator not started, may not leader",
		}
		return respWorker, nil
	}
	w := s.coordinator.GetWorkerByAddress(req.Address)
	if w == nil || w.State() != coordinator.WorkerClosed {
		respWorker := &pb.OfflineWorkerResponse{
			Result: false,
			Msg:    "worker which has not been closed is not allowed to offline",
		}
		return respWorker, nil
	}
	k := common.WorkerRegisterKeyAdapter.Encode(req.Address)
	v := req.Name
	ectx, cancel := context.WithTimeout(ctx, etcdTimeouit)
	defer cancel()
	resp, err := s.etcdClient.Txn(ectx).
		If(clientv3.Compare(clientv3.Value(k), "=", v)).
		Then(clientv3.OpDelete(k)).
		Commit()
	if err != nil {
		return nil, err
	}
	if !resp.Succeeded {
		respWorker := &pb.OfflineWorkerResponse{
			Result: false,
			Msg:    "delete from etcd failed, please check whether the name and address of worker match.",
		}
		return respWorker, nil
	}
	s.coordinator.RemoveWorker(req.Name)
	log.L().Info("offline worker successfully", zap.String("name", req.Name), zap.String("address", req.Address))
	respWorker := &pb.OfflineWorkerResponse{
		Result: true,
	}
	return respWorker, nil
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

	cfg, stCfgs, err := s.generateSubTask(ctx, req.Task)
	if err != nil {
		return &pb.StartTaskResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}, nil
	}
	log.L().Info("", zap.String("task name", cfg.Name), zap.Stringer("task", cfg), zap.String("request", "StartTask"))

	sourceRespCh := make(chan *pb.CommonWorkerResponse, len(stCfgs))
	var wg sync.WaitGroup
	subSourceIDs := make([]string, 0, len(stCfgs))
	if len(req.Sources) > 0 {
		// specify only start task on partial sources
		sourceCfg := make(map[string]*config.SubTaskConfig)
		for _, stCfg := range stCfgs {
			sourceCfg[stCfg.SourceID] = stCfg
		}
		stCfgs = make([]*config.SubTaskConfig, 0, len(req.Sources))
		for _, source := range req.Sources {
			if stCfg, ok := sourceCfg[source]; ok {
				stCfgs = append(stCfgs, stCfg)
			} else {
				sourceRespCh <- errorCommonWorkerResponse("source not found in task's config", source)
			}
		}
	}

	for _, stCfg := range stCfgs {
		subSourceIDs = append(subSourceIDs, stCfg.SourceID)
		wg.Add(1)
		go s.ap.Emit(ctx, 0, func(args ...interface{}) {
			defer wg.Done()
			cfg, _ := args[0].(*config.SubTaskConfig)
			worker, stCfgToml, _, err := s.taskConfigArgsExtractor(cfg)
			if err != nil {
				sourceRespCh <- errorCommonWorkerResponse(err.Error(), cfg.SourceID)
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
			resp.StartSubTask.Source = cfg.SourceID
			sourceRespCh <- resp.StartSubTask
		}, func(args ...interface{}) {
			defer wg.Done()
			cfg, _ := args[0].(*config.SubTaskConfig)
			worker, _, _, err := s.taskConfigArgsExtractor(cfg)
			if err != nil {
				sourceRespCh <- errorCommonWorkerResponse(err.Error(), cfg.SourceID)
				return
			}
			sourceRespCh <- errorCommonWorkerResponse(terror.ErrMasterNoEmitToken.Generate(worker.Address()).Error(), cfg.SourceID)
		}, stCfg)
	}
	wg.Wait()

	sourceRespMap := make(map[string]*pb.CommonWorkerResponse, len(stCfgs))
	sources := make([]string, 0, len(stCfgs))
	for len(sourceRespCh) > 0 {
		sourceResp := <-sourceRespCh
		sourceRespMap[sourceResp.Source] = sourceResp
		sources = append(sources, sourceResp.Source)
	}

	// TODO: simplify logic of response sort
	sort.Strings(sources)
	sourceResps := make([]*pb.CommonWorkerResponse, 0, len(sources))
	for _, worker := range sources {
		sourceResps = append(sourceResps, sourceRespMap[worker])
	}

	// record task -> sources map
	s.taskSources[cfg.Name] = subSourceIDs

	return &pb.StartTaskResponse{
		Result:  true,
		Sources: sourceResps,
	}, nil
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
	workerRespCh := make(chan *pb.OperateSubTaskResponse, len(sources))

	handleErr := func(err error, source string) {
		log.L().Error("response error", zap.Error(err))
		workerResp := &pb.OperateSubTaskResponse{
			Op:     req.Op,
			Result: false,
			Source: source,
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
			resp.OperateSubTask.Source = sourceID
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
		workerRespMap[workerResp.Source] = workerResp
	}

	workerResps := make([]*pb.OperateSubTaskResponse, 0, len(sources))
	for _, source := range sources {
		workerResps = append(workerResps, workerRespMap[source])
	}

	if req.Op == pb.TaskOp_Stop {
		// remove (partial / all) workers for a task
		s.removeTaskWorkers(req.Name, sources)
	}

	resp.Result = true
	resp.Sources = workerResps

	return resp, nil
}

// UpdateTask implements MasterServer.UpdateTask
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
				workerRespCh <- errorCommonWorkerResponse("source not found in task's config or deployment config", source)
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
			resp.UpdateSubTask.Source = cfg.SourceID
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

	isLeader, needForward := s.isLeaderAndNeedForward()
	if !isLeader {
		if needForward {
			return s.leaderClient.QueryStatus(ctx, req)
		}
		return nil, terror.ErrMasterRequestIsNotForwardToLeader
	}

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
		workerRespMap[workerResp.Source] = workerResp
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
		workerRespMap[workerResp.Source] = workerResp
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
	worker := s.coordinator.GetWorkerBySourceID(req.Source)
	if worker == nil {
		resp.Msg = fmt.Sprintf("worker %s client not found in %v", req.Source, s.coordinator.GetAllWorkers())
		return resp, nil
	}
	response, err := worker.SendRequest(ctx, subReq, s.cfg.RPCTimeout)
	workerResp := &pb.CommonWorkerResponse{}
	if err != nil {
		workerResp = errorCommonWorkerResponse(errors.ErrorStack(err), req.Source)
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
			worker := s.coordinator.GetWorkerBySourceID(source)
			if worker == nil {
				workerRespCh <- errorCommonWorkerResponse(fmt.Sprintf("worker %s relevant worker-client not found", source), source)
				return
			}
			resp, err := worker.SendRequest(ctx, workerReq, s.cfg.RPCTimeout)
			workerResp := &pb.CommonWorkerResponse{}
			if err != nil {
				workerResp = errorCommonWorkerResponse(errors.ErrorStack(err), source)
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
			worker := s.coordinator.GetWorkerBySourceID(sourceID)
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
				workerResp = errorCommonWorkerResponse(errors.ErrorStack(err), sourceID)
			} else {
				workerResp = resp.SwitchRelayMaster
			}
			workerResp.Source = sourceID
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
			worker := s.coordinator.GetWorkerBySourceID(source)
			if worker == nil {
				workerResp := &pb.OperateRelayResponse{
					Op:     req.Op,
					Result: false,
					Source: source,
					Msg:    fmt.Sprintf("%s relevant worker-client not found", source),
				}
				workerRespCh <- workerResp
				return
			}
			resp, err := worker.SendRequest(ctx, request, s.cfg.RPCTimeout)
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
			workerResp.Source = source
			workerRespCh <- workerResp
		}(source)
	}
	wg.Wait()

	workerRespMap := make(map[string]*pb.OperateRelayResponse, len(req.Sources))
	for len(workerRespCh) > 0 {
		workerResp := <-workerRespCh
		workerRespMap[workerResp.Source] = workerResp
	}

	sort.Strings(req.Sources)
	workerResps := make([]*pb.OperateRelayResponse, 0, len(req.Sources))
	for _, worker := range req.Sources {
		workerResps = append(workerResps, workerRespMap[worker])
	}

	return &pb.OperateWorkerRelayResponse{
		Result:  true,
		Sources: workerResps,
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
func (s *Server) getStatusFromWorkers(ctx context.Context, sources []string, taskName string) chan *pb.QueryStatusResponse {
	workerReq := &workerrpc.Request{
		Type:        workerrpc.CmdQueryStatus,
		QueryStatus: &pb.QueryStatusRequest{Name: taskName},
	}
	workerRespCh := make(chan *pb.QueryStatusResponse, len(sources))

	handleErr := func(err error, worker string) bool {
		log.L().Error("response error", zap.Error(err))
		resp := &pb.QueryStatusResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
			Source: worker,
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
			worker := s.coordinator.GetWorkerBySourceID(sourceID)
			if worker == nil {
				err := terror.ErrMasterWorkerArgsExtractor.Generatef("%s relevant worker-client not found", sourceID)
				handleErr(err, sourceID)
				return
			}
			resp, err := worker.SendRequest(ctx, workerReq, s.cfg.RPCTimeout)
			workerStatus := &pb.QueryStatusResponse{}
			if err != nil {
				workerStatus = &pb.QueryStatusResponse{
					Result: false,
					Msg:    errors.ErrorStack(err),
				}
			} else {
				workerStatus = resp.QueryStatus
			}
			workerStatus.Source = sourceID
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
			Source: source,
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
			worker := s.coordinator.GetWorkerBySourceID(sourceID)
			if worker == nil {
				err := terror.ErrMasterWorkerArgsExtractor.Generatef("%s relevant worker-client not found", sourceID)
				handleErr(err, sourceID)
				return
			}

			resp, err := worker.SendRequest(ctx, workerReq, s.cfg.RPCTimeout)
			workerError := &pb.QueryErrorResponse{}
			if err != nil {
				workerError = &pb.QueryErrorResponse{
					Result: false,
					Msg:    errors.ErrorStack(err),
				}
			} else {
				workerError = resp.QueryError
			}
			workerError.Source = sourceID
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
	s.Lock()

	isLeader, needForward := s.isLeaderAndNeedForward()
	if !isLeader {
		if needForward {
			return s.leaderClient.UpdateMasterConfig(ctx, req)
		}
		return nil, terror.ErrMasterRequestIsNotForwardToLeader
	}

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
	worker := s.coordinator.GetWorkerBySourceID(source)
	if worker == nil {
		return errorCommonWorkerResponse(fmt.Sprintf("source %s relevant source-client not found", source), source), nil
	}

	log.L().Info("update relay config", zap.String("source", source), zap.String("request", "UpdateWorkerRelayConfig"))
	request := &workerrpc.Request{
		Type:        workerrpc.CmdUpdateRelay,
		UpdateRelay: &pb.UpdateRelayRequest{Content: content},
	}
	resp, err := worker.SendRequest(ctx, request, s.cfg.RPCTimeout)
	if err != nil {
		return errorCommonWorkerResponse(errors.ErrorStack(err), source), nil
	}
	return resp.UpdateRelay, nil
}

// TODO: refine the call stack of this API, query worker configs that we needed only
func (s *Server) getWorkerConfigs(ctx context.Context, workers []*config.MySQLInstance) (map[string]config.DBConfig, error) {
	cfgs := make(map[string]config.DBConfig)
	for _, w := range workers {
		if cfg := s.coordinator.GetConfigBySourceID(w.SourceID); cfg != nil {
			// check the password
			_, err := cfg.DecryptPassword()
			if err != nil {
				return nil, err
			}
			cfgs[w.SourceID] = cfg.From
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
	worker := s.coordinator.GetWorkerBySourceID(source)
	if worker == nil {
		return errorCommonWorkerResponse(fmt.Sprintf("source %s relevant source-client not found", source), source), nil
	}
	log.L().Info("try to migrate relay", zap.String("source", source), zap.String("request", "MigrateWorkerRelay"))
	request := &workerrpc.Request{
		Type:         workerrpc.CmdMigrateRelay,
		MigrateRelay: &pb.MigrateRelayRequest{BinlogName: binlogName, BinlogPos: binlogPos},
	}
	resp, err := worker.SendRequest(ctx, request, s.cfg.RPCTimeout)
	if err != nil {
		return errorCommonWorkerResponse(errors.ErrorStack(err), source), nil
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

func makeMysqlWorkerResponse(err error) (*pb.MysqlWorkerResponse, error) {
	return &pb.MysqlWorkerResponse{
		Result: false,
		Msg:    errors.ErrorStack(err),
	}, nil
}

// OperateMysqlWorker will create or update a Worker
func (s *Server) OperateMysqlWorker(ctx context.Context, req *pb.MysqlWorkerRequest) (*pb.MysqlWorkerResponse, error) {
	isLeader, needForward := s.isLeaderAndNeedForward()
	if !isLeader {
		if needForward {
			return s.leaderClient.OperateMysqlWorker(ctx, req)
		}
		return nil, terror.ErrMasterRequestIsNotForwardToLeader
	}

	cfg := config.NewMysqlConfig()
	if err := cfg.Parse(req.Config); err != nil {
		return makeMysqlWorkerResponse(err)
	}

	dbConfig, err := cfg.GenerateDBConfig()
	if err != nil {
		return makeMysqlWorkerResponse(err)
	}
	fromDB, err := conn.DefaultDBProvider.Apply(*dbConfig)
	if err != nil {
		return makeMysqlWorkerResponse(err)
	}
	if err = cfg.Adjust(fromDB.DB); err != nil {
		return makeMysqlWorkerResponse(err)
	}
	if req.Config, err = cfg.Toml(); err != nil {
		return makeMysqlWorkerResponse(err)
	}
	var resp *pb.MysqlWorkerResponse
	switch req.Op {
	case pb.WorkerOp_StartWorker:
		w := s.coordinator.GetWorkerBySourceID(cfg.SourceID)
		if w != nil {
			return &pb.MysqlWorkerResponse{
				Result: false,
				Msg:    "Create worker failed. worker has been started",
			}, nil
		}
		w, err = s.coordinator.AcquireWorkerForSource(cfg.SourceID)
		if err != nil {
			return makeMysqlWorkerResponse(err)
		}

		resp, err = w.OperateMysqlWorker(ctx, req, s.cfg.RPCTimeout)
		if err != nil {
			// TODO: handle error or backoff
			s.coordinator.HandleStartedWorker(w, cfg, false)
			return makeMysqlWorkerResponse(err)
		}
		// TODO: handle error or backoff
		s.coordinator.HandleStartedWorker(w, cfg, true)
	case pb.WorkerOp_UpdateConfig:
		w := s.coordinator.GetWorkerBySourceID(cfg.SourceID)
		if w == nil {
			return &pb.MysqlWorkerResponse{
				Result: false,
				Msg:    "Update worker config failed. worker has not been started",
			}, nil
		}
		if resp, err = w.OperateMysqlWorker(ctx, req, s.cfg.RPCTimeout); err != nil {
			return makeMysqlWorkerResponse(err)
		}
	case pb.WorkerOp_StopWorker:
		w := s.coordinator.GetWorkerBySourceID(cfg.SourceID)
		if w == nil {
			return &pb.MysqlWorkerResponse{
				Result: false,
				Msg:    "Stop Mysql-worker failed. worker has not been started",
			}, nil
		}
		if resp, err = w.OperateMysqlWorker(ctx, req, s.cfg.RPCTimeout); err != nil {
			return &pb.MysqlWorkerResponse{
				Result: false,
				Msg:    errors.ErrorStack(err),
			}, nil
		}
		if resp.Result {
			s.coordinator.HandleStoppedWorker(w, cfg)
		}
	default:
		return &pb.MysqlWorkerResponse{
			Result: false,
			Msg:    "invalid operate on worker",
		}, nil
	}

	return resp, nil
}

func (s *Server) generateSubTask(ctx context.Context, task string) (*config.TaskConfig, []*config.SubTaskConfig, error) {
	cfg := config.NewTaskConfig()
	err := cfg.Decode(task)
	if err != nil {
		return nil, nil, terror.WithClass(err, terror.ClassDMMaster)
	}

	sourceCfgs, err := s.getWorkerConfigs(ctx, cfg.MySQLInstances)
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
		return nil, "", "", handleErr(terror.ErrMasterTaskConfigExtractor.Generatef("%s relevant worker-client not found", cfg.SourceID))
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
