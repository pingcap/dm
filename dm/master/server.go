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
	"net"
	"net/http"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	toolutils "github.com/pingcap/tidb-tools/pkg/utils"
	"github.com/siddontang/go/sync2"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/pingcap/dm/checker"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/master/metrics"
	"github.com/pingcap/dm/dm/master/scheduler"
	"github.com/pingcap/dm/dm/master/shardddl"
	"github.com/pingcap/dm/dm/master/workerrpc"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/conn"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/cputil"
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

	// getLeaderBlockTime is the max block time for get leader information from election
	getLeaderBlockTime = 10 * time.Minute
)

var (
	// the retry times for dm-master to confirm the dm-workers status is expected
	maxRetryNum = 30
	// the retry interval for dm-master to confirm the dm-workers status is expected
	retryInterval = time.Second

	// 0 means not use tls
	// 1 means use tls
	useTLS = int32(0)

	// typically there's only one server running in one process, but testMaster.TestOfflineMember starts 3 servers,
	// so we need sync.Once to prevent data race
	registerOnce      sync.Once
	runBackgroundOnce sync.Once
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

	// removeMetaLock locks start task when removing meta
	removeMetaLock sync.RWMutex

	// WaitGroup for background functions.
	bgFunWg sync.WaitGroup

	// dm-worker-ID(host:ip) -> dm-worker client management
	scheduler *scheduler.Scheduler

	// shard DDL pessimist
	pessimist *shardddl.Pessimist
	// shard DDL optimist
	optimist *shardddl.Optimist

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
		cfg:       cfg,
		scheduler: scheduler.NewScheduler(&logger, cfg.Security),
		idGen:     tracing.NewIDGen(),
		ap:        NewAgentPool(&RateLimitConfig{rate: cfg.RPCRateLimit, burst: cfg.RPCRateBurst}),
	}
	server.pessimist = shardddl.NewPessimist(&logger, server.getTaskResources)
	server.optimist = shardddl.NewOptimist(&logger)
	server.closed.Set(true)

	setUseTLS(&cfg.Security)

	return &server
}

// Start starts to serving
func (s *Server) Start(ctx context.Context) (err error) {
	etcdCfg := genEmbedEtcdConfigWithLogger(s.cfg.LogLevel)
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
	etcdCfg, err = s.cfg.genEmbedEtcdConfig(etcdCfg)
	if err != nil {
		return
	}

	tls, err := toolutils.NewTLS(s.cfg.SSLCA, s.cfg.SSLCert, s.cfg.SSLKey, s.cfg.AdvertiseAddr, s.cfg.CertAllowedCN)
	if err != nil {
		return terror.ErrMasterTLSConfigNotValid.Delegate(err)
	}

	// tls2 is used for grpc client in grpc gateway
	tls2, err := toolutils.NewTLS(s.cfg.SSLCA, s.cfg.SSLCert, s.cfg.SSLKey, s.cfg.AdvertiseAddr, s.cfg.CertAllowedCN)
	if err != nil {
		return terror.ErrMasterTLSConfigNotValid.Delegate(err)
	}
	if tls2 != nil && tls2.TLSConfig() != nil {
		tls2.TLSConfig().InsecureSkipVerify = true
	}

	apiHandler, err := getHTTPAPIHandler(ctx, s.cfg.MasterAddr, tls2.ToGRPCDialOption())
	if err != nil {
		return
	}

	registerOnce.Do(metrics.RegistryMetrics)

	// HTTP handlers on etcd's client IP:port
	// NOTE: after received any HTTP request from chrome browser,
	// the server may be blocked when closing sometime.
	// And any request to etcd's builtin handler has the same problem.
	// And curl or safari browser does trigger this problem.
	// But I haven't figured it out.
	// (maybe more requests are sent from chrome or its extensions).
	userHandles := map[string]http.Handler{
		"/apis/":   apiHandler,
		"/status":  getStatusHandle(),
		"/debug/":  getDebugHandler(),
		"/metrics": metrics.GetMetricsHandler(),
	}

	// gRPC API server
	gRPCSvr := func(gs *grpc.Server) { pb.RegisterMasterServer(gs, s) }

	// start embed etcd server, gRPC API server and HTTP (API, status and debug) server.
	s.etcd, err = startEtcd(etcdCfg, gRPCSvr, userHandles, etcdStartTimeout)
	if err != nil {
		return
	}

	// create an etcd client used in the whole server instance.
	// NOTE: we only use the local member's address now, but we can use all endpoints of the cluster if needed.
	s.etcdClient, err = etcdutil.CreateClient([]string{withHost(s.cfg.MasterAddr)}, tls.TLSConfig())
	if err != nil {
		return
	}

	// start leader election
	// TODO: s.cfg.Name -> address
	s.election, err = election.NewElection(ctx, s.etcdClient, electionTTL, electionKey, s.cfg.Name, s.cfg.AdvertiseAddr, getLeaderBlockTime)
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

	runBackgroundOnce.Do(func() {
		s.bgFunWg.Add(1)
		go func() {
			defer s.bgFunWg.Done()
			metrics.RunBackgroundJob(ctx)
		}()
	})

	s.bgFunWg.Add(1)
	go func() {
		defer s.bgFunWg.Done()
		select {
		case <-ctx.Done():
			return
		}
	}()

	failpoint.Inject("FailToElect", func(val failpoint.Value) {
		masterStrings := val.(string)
		if strings.Contains(masterStrings, s.cfg.Name) {
			log.L().Info("master election failed", zap.String("failpoint", "FailToElect"))
			s.election.Close()
		}
	})

	log.L().Info("listening gRPC API and status request", zap.String("address", s.cfg.MasterAddr))
	return
}

// Close close the RPC server, this function can be called multiple times
func (s *Server) Close() {
	if s.closed.Get() {
		return
	}
	log.L().Info("closing server")
	defer func() {
		log.L().Info("server closed")
	}()

	// wait for background functions returned
	s.bgFunWg.Wait()

	s.Lock()
	defer s.Unlock()

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
// key:   /dm-worker/r/name
// value: workerInfo
func (s *Server) RegisterWorker(ctx context.Context, req *pb.RegisterWorkerRequest) (*pb.RegisterWorkerResponse, error) {
	var (
		resp2 *pb.RegisterWorkerResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	err := s.scheduler.AddWorker(req.Name, req.Address)
	if err != nil {
		return &pb.RegisterWorkerResponse{
			Result: false,
			Msg:    err.Error(),
		}, nil
	}
	log.L().Info("register worker successfully", zap.String("name", req.Name), zap.String("address", req.Address))
	return &pb.RegisterWorkerResponse{
		Result: true,
	}, nil
}

// OfflineMember removes info of the master/worker which has been Closed
// all the masters are store in etcd member list
// all the workers are store in the path:
// key:   /dm-worker/r
// value: WorkerInfo
func (s *Server) OfflineMember(ctx context.Context, req *pb.OfflineMemberRequest) (*pb.OfflineMemberResponse, error) {
	var (
		resp2 *pb.OfflineMemberResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	if req.Type == common.Worker {
		err := s.scheduler.RemoveWorker(req.Name)
		if err != nil {
			return &pb.OfflineMemberResponse{
				Result: false,
				Msg:    err.Error(),
			}, nil
		}
	} else if req.Type == common.Master {
		err := s.deleteMasterByName(ctx, req.Name)
		if err != nil {
			return &pb.OfflineMemberResponse{
				Result: false,
				Msg:    err.Error(),
			}, nil
		}
	} else {
		return &pb.OfflineMemberResponse{
			Result: false,
			Msg:    terror.ErrMasterInvalidOfflineType.Generate(req.Type).Error(),
		}, nil
	}
	log.L().Info("offline member successfully", zap.String("type", req.Type), zap.String("name", req.Name))
	return &pb.OfflineMemberResponse{
		Result: true,
	}, nil
}

func (s *Server) deleteMasterByName(ctx context.Context, name string) error {
	cli := s.etcdClient
	// Get etcd ID by name.
	var id uint64
	listResp, err := etcdutil.ListMembers(cli)
	if err != nil {
		return err
	}
	for _, m := range listResp.Members {
		if name == m.Name {
			id = m.ID
			break
		}
	}
	if id == 0 {
		return terror.ErrMasterMasterNameNotExist.Generate(name)
	}

	_, err = s.election.ClearSessionIfNeeded(ctx, name)
	if err != nil {
		return err
	}

	_, err = etcdutil.RemoveMember(cli, id)
	return err
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
	var (
		resp2 *pb.StartTaskResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	resp := &pb.StartTaskResponse{}
	cfg, stCfgs, err := s.generateSubTask(ctx, req.Task)
	if err != nil {
		resp.Msg = err.Error()
		return resp, nil
	}
	log.L().Info("", zap.String("task name", cfg.Name), zap.String("task", cfg.JSON()), zap.String("request", "StartTask"))

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
		sources := make([]string, 0, len(stCfgs))
		for _, stCfg := range stCfgs {
			sources = append(sources, stCfg.SourceID)
		}
		s.removeMetaLock.Lock()
		if req.RemoveMeta {
			if scm := s.scheduler.GetSubTaskCfgsByTask(cfg.Name); len(scm) > 0 {
				resp.Msg = terror.Annotate(terror.ErrSchedulerSubTaskExist.Generate(cfg.Name, sources),
					"while remove-meta is true").Error()
				s.removeMetaLock.Unlock()
				return resp, nil
			}
			err = s.removeMetaData(ctx, cfg)
			if err != nil {
				resp.Msg = terror.Annotate(err, "while removing metadata").Error()
				s.removeMetaLock.Unlock()
				return resp, nil
			}
		}
		err = s.scheduler.AddSubTasks(subtaskCfgPointersToInstances(stCfgs...)...)
		s.removeMetaLock.Unlock()
		if err != nil {
			resp.Msg = err.Error()
			return resp, nil
		}

		err = s.scheduler.AddTaskCfg(*cfg)
		if err != nil {
			resp.Msg = err.Error()
			return resp, nil
		}

		resp.Result = true
		if cfg.RemoveMeta {
			resp.Msg = "`remove-meta` in task config is deprecated, please use `start-task ... --remove-meta` instead"
		}
		sourceResps = s.getSourceRespsAfterOperation(ctx, cfg.Name, sources, []string{}, req)
	}

	resp.Sources = sourceResps
	return resp, nil
}

// OperateTask implements MasterServer.OperateTask
func (s *Server) OperateTask(ctx context.Context, req *pb.OperateTaskRequest) (*pb.OperateTaskResponse, error) {
	var (
		resp2 *pb.OperateTaskResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
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
		if err != nil {
			resp.Msg = err.Error()
			return resp, nil
		}
		err = s.scheduler.RemoveTaskCfg(req.Name)
	} else {
		err = s.scheduler.UpdateExpectSubTaskStage(expect, req.Name, sources...)
	}
	if err != nil {
		resp.Msg = err.Error()
		return resp, nil
	}

	resp.Result = true
	resp.Sources = s.getSourceRespsAfterOperation(ctx, req.Name, sources, []string{}, req)
	return resp, nil
}

// GetSubTaskCfg implements MasterServer.GetSubTaskCfg
func (s *Server) GetSubTaskCfg(ctx context.Context, req *pb.GetSubTaskCfgRequest) (*pb.GetSubTaskCfgResponse, error) {
	var (
		resp2 *pb.GetSubTaskCfgResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	subCfgs := s.scheduler.GetSubTaskCfgsByTask(req.Name)
	if len(subCfgs) == 0 {
		return &pb.GetSubTaskCfgResponse{
			Result: false,
			Msg:    "task not found",
		}, nil
	}

	cfgs := make([]string, 0, len(subCfgs))

	for _, cfg := range subCfgs {
		cfgBytes, err := cfg.Toml()
		if err != nil {
			return &pb.GetSubTaskCfgResponse{
				Result: false,
				Msg:    err.Error(),
			}, nil
		}
		cfgs = append(cfgs, string(cfgBytes))
	}

	return &pb.GetSubTaskCfgResponse{
		Result: true,
		Cfgs:   cfgs,
	}, nil
}

// UpdateTask implements MasterServer.UpdateTask
// TODO: support update task later
func (s *Server) UpdateTask(ctx context.Context, req *pb.UpdateTaskRequest) (*pb.UpdateTaskResponse, error) {
	var (
		resp2 *pb.UpdateTaskResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	cfg, stCfgs, err := s.generateSubTask(ctx, req.Task)
	if err != nil {
		return &pb.UpdateTaskResponse{
			Result: false,
			Msg:    err.Error(),
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

	// TODO: update task config
	// s.scheduler.UpdateTaskCfg(*cfg)

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
	var (
		resp2 *pb.QueryStatusListResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
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
	var (
		resp2 *pb.QueryErrorListResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
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
	var (
		resp2 *pb.ShowDDLLocksResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	resp := &pb.ShowDDLLocksResponse{
		Result: true,
	}

	// show pessimistic locks.
	resp.Locks = append(resp.Locks, s.pessimist.ShowLocks(req.Task, req.Sources)...)
	// show optimistic locks.
	resp.Locks = append(resp.Locks, s.optimist.ShowLocks(req.Task, req.Sources)...)

	if len(resp.Locks) == 0 {
		resp.Msg = "no DDL lock exists"
	}
	return resp, nil
}

// UnlockDDLLock implements MasterServer.UnlockDDLLock
// TODO(csuzhangxc): implement this later.
func (s *Server) UnlockDDLLock(ctx context.Context, req *pb.UnlockDDLLockRequest) (*pb.UnlockDDLLockResponse, error) {
	var (
		resp2 *pb.UnlockDDLLockResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	resp := &pb.UnlockDDLLockResponse{}

	task := utils.ExtractTaskFromLockID(req.ID)
	if task == "" {
		resp.Msg = "can't find task name from lock-ID"
		return resp, nil
	}
	cfgStr := s.scheduler.GetTaskCfg(task)
	if cfgStr == "" {
		resp.Msg = "task (" + task + ") which extracted from lock-ID is not found in DM"
		return resp, nil
	}
	cfg := config.NewTaskConfig()
	if err := cfg.Decode(cfgStr); err != nil {
		resp.Msg = err.Error()
		return resp, nil
	}

	if cfg.ShardMode != config.ShardPessimistic {
		resp.Msg = "`unlock-ddl-lock` is only supported in pessimistic shard mode currently"
		return resp, nil
	}

	// TODO: add `unlock-ddl-lock` support for Optimist later.
	err := s.pessimist.UnlockLock(ctx, req.ID, req.ReplaceOwner, req.ForceRemove)
	if err != nil {
		resp.Msg = err.Error()
	} else {
		resp.Result = true
	}

	return resp, nil
}

// PurgeWorkerRelay implements MasterServer.PurgeWorkerRelay
func (s *Server) PurgeWorkerRelay(ctx context.Context, req *pb.PurgeWorkerRelayRequest) (*pb.PurgeWorkerRelayResponse, error) {
	var (
		resp2 *pb.PurgeWorkerRelayResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
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
				workerRespCh <- errorCommonWorkerResponse(fmt.Sprintf("source %s relevant worker-client not found", source), source, "")
				return
			}
			resp, err := worker.SendRequest(ctx, workerReq, s.cfg.RPCTimeout)
			workerResp := &pb.CommonWorkerResponse{}
			if err != nil {
				workerResp = errorCommonWorkerResponse(err.Error(), source, worker.BaseInfo().Name)
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
	var (
		resp2 *pb.SwitchWorkerRelayMasterResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	workerRespCh := make(chan *pb.CommonWorkerResponse, len(req.Sources))

	handleErr := func(err error, source string) {
		log.L().Error("response error", zap.Error(err))
		resp := errorCommonWorkerResponse(err.Error(), source, "")
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
				workerResp = errorCommonWorkerResponse(err.Error(), sourceID, worker.BaseInfo().Name)
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
	var (
		resp2 *pb.OperateWorkerRelayResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
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
		resp.Msg = err.Error()
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
			Msg:    err.Error(),
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
					Msg:          err.Error(),
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
			Msg:    err.Error(),
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
					Msg:         err.Error(),
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
	var (
		resp2 *pb.UpdateMasterConfigResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	s.Lock()
	err := s.cfg.UpdateConfigFile(req.Config)
	if err != nil {
		s.Unlock()
		return &pb.UpdateMasterConfigResponse{
			Result: false,
			Msg:    "Failed to write config to local file. detail: " + err.Error(),
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
			Msg:    fmt.Sprintf("Failed to parse configure from file %s, detail: ", cfg.ConfigFile) + err.Error(),
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
	var (
		resp2 *pb.CommonWorkerResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
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
		return errorCommonWorkerResponse(err.Error(), source, worker.BaseInfo().Name), nil
	}
	return resp.UpdateRelay, nil
}

// TODO: refine the call stack of this API, query worker configs that we needed only
func (s *Server) getSourceConfigs(sources []*config.MySQLInstance) (map[string]config.DBConfig, error) {
	cfgs := make(map[string]config.DBConfig)
	for _, source := range sources {
		if cfg := s.scheduler.GetSourceCfgByID(source.SourceID); cfg != nil {
			// check the password
			cfg.DecryptPassword()
			cfgs[source.SourceID] = cfg.From
		}
	}
	return cfgs, nil
}

// MigrateWorkerRelay migrates dm-woker relay unit
func (s *Server) MigrateWorkerRelay(ctx context.Context, req *pb.MigrateWorkerRelayRequest) (*pb.CommonWorkerResponse, error) {
	var (
		resp2 *pb.CommonWorkerResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
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
		return errorCommonWorkerResponse(err.Error(), source, worker.BaseInfo().Name), nil
	}
	return resp.MigrateRelay, nil
}

// CheckTask checks legality of task configuration
func (s *Server) CheckTask(ctx context.Context, req *pb.CheckTaskRequest) (*pb.CheckTaskResponse, error) {
	var (
		resp2 *pb.CheckTaskResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	_, _, err := s.generateSubTask(ctx, req.Task)
	if err != nil {
		return &pb.CheckTaskResponse{
			Result: false,
			Msg:    err.Error(),
		}, nil
	}

	return &pb.CheckTaskResponse{
		Result: true,
		Msg:    "check pass!!!",
	}, nil
}

func parseAndAdjustSourceConfig(contents []string) ([]*config.SourceConfig, error) {
	cfgs := make([]*config.SourceConfig, len(contents))
	for i, content := range contents {
		cfg := config.NewSourceConfig()
		if err := cfg.ParseYaml(content); err != nil {
			return cfgs, err
		}

		dbConfig := cfg.GenerateDBConfig()

		fromDB, err := conn.DefaultDBProvider.Apply(*dbConfig)
		if err != nil {
			return cfgs, err
		}
		if err = cfg.Adjust(fromDB.DB); err != nil {
			return cfgs, err
		}
		if _, err = cfg.Yaml(); err != nil {
			return cfgs, err
		}

		cfgs[i] = cfg
	}
	return cfgs, nil
}

// OperateSource will create or update an upstream source.
func (s *Server) OperateSource(ctx context.Context, req *pb.OperateSourceRequest) (*pb.OperateSourceResponse, error) {
	var (
		resp2 *pb.OperateSourceResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	cfgs, err := parseAndAdjustSourceConfig(req.Config)
	resp := &pb.OperateSourceResponse{
		Result: false,
	}
	if err != nil {
		resp.Msg = err.Error()
		return resp, nil
	}

	// boundM: sourceID -> worker are used to query status from worker, to return a more real status
	boundM := map[string]*scheduler.Worker{}

	switch req.Op {
	case pb.SourceOp_StartSource:
		var (
			started  []string
			hasError = false
			err      error
		)
		for _, cfg := range cfgs {
			err = s.scheduler.AddSourceCfg(*cfg)
			// return first error and try to revert, so user could copy-paste same start command after error
			if err != nil {
				resp.Msg = err.Error()
				hasError = true
				break
			}
			started = append(started, cfg.SourceID)
		}

		if hasError {
			log.L().Info("reverting start source", zap.String("error", err.Error()))
			for _, sid := range started {
				err := s.scheduler.RemoveSourceCfg(sid)
				if err != nil {
					log.L().Error("while reverting started source, another error happens",
						zap.String("source id", sid),
						zap.String("error", err.Error()))
				}
			}
			return resp, nil
		}
		// for start source, we should get worker after start source
		for _, sid := range started {
			boundM[sid] = s.scheduler.GetWorkerBySource(sid)
		}
	case pb.SourceOp_UpdateSource:
		// TODO: support SourceOp_UpdateSource later
		resp.Msg = "Update worker config is not supported by dm-ha now"
		return resp, nil
	case pb.SourceOp_StopSource:
		toRemove := make([]string, 0, len(cfgs)+len(req.SourceID))
		for _, sid := range req.SourceID {
			toRemove = append(toRemove, sid)
		}
		for _, cfg := range cfgs {
			toRemove = append(toRemove, cfg.SourceID)
		}

		for _, sid := range toRemove {
			boundM[sid] = s.scheduler.GetWorkerBySource(sid)
			err := s.scheduler.RemoveSourceCfg(sid)
			// TODO(lance6716):
			// user could not copy-paste same command if encounter error halfway:
			// `operate-source stop  correct-id-1     wrong-id-2`
			//                       remove success   some error
			// `operate-source stop  correct-id-1     correct-id-2`
			//                       not exist, error
			// find a way to distinguish this scenario and wrong source id
			// or give a command to show existing source id
			if err != nil {
				resp.Msg = err.Error()
				return resp, nil
			}
		}
	case pb.SourceOp_ShowSource:
		for _, id := range s.scheduler.GetSourceCfgIDs() {
			boundM[id] = s.scheduler.GetWorkerBySource(id)
		}
	default:
		resp.Msg = terror.ErrMasterInvalidOperateOp.Generate(req.Op.String(), "source").Error()
		return resp, nil
	}

	resp.Result = true
	var noWorkerMsg string
	switch req.Op {
	case pb.SourceOp_StartSource, pb.SourceOp_ShowSource:
		noWorkerMsg = "source is added but there is no free worker to bound"
	case pb.SourceOp_StopSource:
		noWorkerMsg = "source is stopped and hasn't bound to worker before being stopped"
	}

	var (
		sourceToCheck []string
		workerToCheck []string
	)

	for id, w := range boundM {
		if w == nil {
			resp.Sources = append(resp.Sources, &pb.CommonWorkerResponse{
				Result: true,
				Msg:    noWorkerMsg,
				Source: id,
			})
		} else {
			sourceToCheck = append(sourceToCheck, id)
			workerToCheck = append(workerToCheck, w.BaseInfo().Name)
		}
	}
	if len(sourceToCheck) > 0 {
		resp.Sources = append(resp.Sources, s.getSourceRespsAfterOperation(ctx, "", sourceToCheck, workerToCheck, req)...)
	}
	return resp, nil
}

// OperateLeader implements MasterServer.OperateLeader
// Note: this request doesn't need to forward to leader
func (s *Server) OperateLeader(ctx context.Context, req *pb.OperateLeaderRequest) (*pb.OperateLeaderResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "OperateLeader"))

	switch req.Op {
	case pb.LeaderOp_EvictLeaderOp:
		s.election.EvictLeader()
	case pb.LeaderOp_CancelEvictLeaderOp:
		s.election.CancelEvictLeader()
	default:
		return &pb.OperateLeaderResponse{
			Result: false,
			Msg:    fmt.Sprintf("operate %s is not supported", req.Op),
		}, nil
	}

	return &pb.OperateLeaderResponse{
		Result: true,
	}, nil
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

func setUseTLS(tlsCfg *config.Security) {
	if enableTLS(tlsCfg) {
		atomic.StoreInt32(&useTLS, 1)
	} else {
		atomic.StoreInt32(&useTLS, 0)
	}

}

func enableTLS(tlsCfg *config.Security) bool {
	if tlsCfg == nil {
		return false
	}

	if len(tlsCfg.SSLCA) == 0 || len(tlsCfg.SSLCert) == 0 || len(tlsCfg.SSLKey) == 0 {
		return false
	}

	return true
}

func withHost(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		// do nothing
		return addr
	}
	if len(host) == 0 {
		return fmt.Sprintf("127.0.0.1:%s", port)
	}

	return addr
}

func (s *Server) removeMetaData(ctx context.Context, cfg *config.TaskConfig) error {
	toDB := *cfg.TargetDB
	toDB.Adjust()
	if len(toDB.Password) > 0 {
		toDB.Password = utils.DecryptOrPlaintext(toDB.Password)
	}

	// clear shard meta data for pessimistic/optimist
	err := s.pessimist.RemoveMetaData(cfg.Name)
	if err != nil {
		return err
	}
	err = s.optimist.RemoveMetaData(cfg.Name)
	if err != nil {
		return err
	}

	// set up db and clear meta data in downstream db
	baseDB, err := conn.DefaultDBProvider.Apply(toDB)
	if err != nil {
		return terror.WithScope(err, terror.ScopeDownstream)
	}
	defer baseDB.Close()
	dbConn, err := baseDB.GetBaseConn(ctx)
	if err != nil {
		return terror.WithScope(err, terror.ScopeDownstream)
	}
	defer baseDB.CloseBaseConn(dbConn)
	ctctx := tcontext.Background().WithContext(ctx).WithLogger(log.With(zap.String("job", "remove metadata")))

	sqls := make([]string, 0, 4)
	// clear loader and syncer checkpoints
	sqls = append(sqls, fmt.Sprintf("DROP TABLE IF EXISTS %s",
		dbutil.TableName(cfg.MetaSchema, cputil.LoaderCheckpoint(cfg.Name))))
	sqls = append(sqls, fmt.Sprintf("DROP TABLE IF EXISTS %s",
		dbutil.TableName(cfg.MetaSchema, cputil.SyncerCheckpoint(cfg.Name))))
	sqls = append(sqls, fmt.Sprintf("DROP TABLE IF EXISTS %s",
		dbutil.TableName(cfg.MetaSchema, cputil.SyncerShardMeta(cfg.Name))))
	sqls = append(sqls, fmt.Sprintf("DROP TABLE IF EXISTS %s",
		dbutil.TableName(cfg.MetaSchema, cputil.SyncerOnlineDDL(cfg.Name))))

	_, err = dbConn.ExecuteSQL(ctctx, nil, cfg.Name, sqls)
	return err
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
		case pb.SourceOp_StartSource, pb.SourceOp_UpdateSource, pb.SourceOp_ShowSource:
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
		response = errorCommonWorkerResponse(err.Error(), sourceID, cli.BaseInfo().Name)
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
	sourceResps := make([]*pb.CommonWorkerResponse, 0, cap(sourceRespCh))
	for len(sourceRespCh) > 0 {
		r := <-sourceRespCh
		sourceResps = append(sourceResps, r)
	}
	sort.Slice(sourceResps, func(i, j int) bool {
		return sourceResps[i].Source < sourceResps[j].Source
	})
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

func (s *Server) listMemberMaster(ctx context.Context, names []string) (*pb.Members_Master, error) {

	resp := &pb.Members_Master{
		Master: &pb.ListMasterMember{},
	}

	memberList, err := s.etcdClient.MemberList(ctx)
	if err != nil {
		resp.Master.Msg = err.Error()
		return resp, nil
	}

	all := len(names) == 0
	set := make(map[string]bool)
	for _, name := range names {
		set[name] = true
	}

	etcdMembers := memberList.Members
	masters := make([]*pb.MasterInfo, 0, len(etcdMembers))
	client := http.Client{
		Timeout: 1 * time.Second,
	}

	for _, etcdMember := range etcdMembers {
		if !all && !set[etcdMember.Name] {
			continue
		}

		alive := true
		if len(etcdMember.ClientURLs) == 0 {
			alive = false
		} else {
			_, err := client.Get(etcdMember.ClientURLs[0] + "/health")
			if err != nil {
				alive = false
			}
		}

		masters = append(masters, &pb.MasterInfo{
			Name:       etcdMember.Name,
			MemberID:   etcdMember.ID,
			Alive:      alive,
			ClientURLs: etcdMember.ClientURLs,
			PeerURLs:   etcdMember.PeerURLs,
		})
	}

	sort.Slice(masters, func(lhs, rhs int) bool {
		return masters[lhs].Name < masters[rhs].Name
	})
	resp.Master.Masters = masters
	return resp, nil
}

func (s *Server) listMemberWorker(ctx context.Context, names []string) (*pb.Members_Worker, error) {
	resp := &pb.Members_Worker{
		Worker: &pb.ListWorkerMember{},
	}

	workerAgents, err := s.scheduler.GetAllWorkers()
	if err != nil {
		resp.Worker.Msg = err.Error()
		return resp, nil
	}

	all := len(names) == 0
	set := make(map[string]bool)
	for _, name := range names {
		set[name] = true
	}

	workers := make([]*pb.WorkerInfo, 0, len(workerAgents))

	for _, workerAgent := range workerAgents {
		if !all && !set[workerAgent.BaseInfo().Name] {
			continue
		}

		workers = append(workers, &pb.WorkerInfo{
			Name:   workerAgent.BaseInfo().Name,
			Addr:   workerAgent.BaseInfo().Addr,
			Stage:  string(workerAgent.Stage()),
			Source: workerAgent.Bound().Source,
		})
	}

	sort.Slice(workers, func(lhs, rhs int) bool {
		return workers[lhs].Name < workers[rhs].Name
	})
	resp.Worker.Workers = workers
	return resp, nil
}

func (s *Server) listMemberLeader(ctx context.Context, names []string) (*pb.Members_Leader, error) {
	resp := &pb.Members_Leader{
		Leader: &pb.ListLeaderMember{},
	}

	all := len(names) == 0
	set := make(map[string]bool)
	for _, name := range names {
		set[name] = true
	}

	_, name, addr, err := s.election.LeaderInfo(ctx)
	if err != nil {
		resp.Leader.Msg = err.Error()
		return resp, nil
	}

	if !all && !set[name] {
		return resp, nil
	}

	resp.Leader.Name = name
	resp.Leader.Addr = addr
	return resp, nil
}

// ListMember list member information
func (s *Server) ListMember(ctx context.Context, req *pb.ListMemberRequest) (*pb.ListMemberResponse, error) {
	var (
		resp2 *pb.ListMemberResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	if !req.Leader && !req.Master && !req.Worker {
		req.Leader = true
		req.Master = true
		req.Worker = true
	}

	resp := &pb.ListMemberResponse{}
	members := make([]*pb.Members, 0)

	if req.Leader {
		res, err := s.listMemberLeader(ctx, req.Names)
		if err != nil {
			resp.Msg = err.Error()
			return resp, nil
		}
		members = append(members, &pb.Members{
			Member: res,
		})
	}

	if req.Master {
		res, err := s.listMemberMaster(ctx, req.Names)
		if err != nil {
			resp.Msg = err.Error()
			return resp, nil
		}
		members = append(members, &pb.Members{
			Member: res,
		})
	}

	if req.Worker {
		res, err := s.listMemberWorker(ctx, req.Names)
		if err != nil {
			resp.Msg = err.Error()
			return resp, nil
		}
		members = append(members, &pb.Members{
			Member: res,
		})
	}

	resp.Result = true
	resp.Members = members
	return resp, nil
}

// OperateSchema operates schema of an upstream table.
func (s *Server) OperateSchema(ctx context.Context, req *pb.OperateSchemaRequest) (*pb.OperateSchemaResponse, error) {
	var (
		resp2 *pb.OperateSchemaResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	if len(req.Sources) == 0 {
		return &pb.OperateSchemaResponse{
			Result: false,
			Msg:    "must specify at least one source",
		}, nil
	}

	workerReq := workerrpc.Request{
		Type: workerrpc.CmdOperateSchema,
		OperateSchema: &pb.OperateWorkerSchemaRequest{
			Op:       req.Op,
			Task:     req.Task,
			Source:   "", // set below.
			Database: req.Database,
			Table:    req.Table,
			Schema:   req.Schema,
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
				workerRespCh <- errorCommonWorkerResponse(fmt.Sprintf("source %s relevant worker-client not found", source), source, "")
				return
			}
			workerReq2 := workerReq
			workerReq2.OperateSchema.Source = source
			resp, err := worker.SendRequest(ctx, &workerReq2, s.cfg.RPCTimeout)
			workerResp := &pb.CommonWorkerResponse{}
			if err != nil {
				workerResp = errorCommonWorkerResponse(err.Error(), source, worker.BaseInfo().Name)
			} else {
				workerResp = resp.OperateSchema
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

	return &pb.OperateSchemaResponse{
		Result:  true,
		Sources: workerResps,
	}, nil
}

// GetTaskCfg implements MasterServer.GetSubTaskCfg
func (s *Server) GetTaskCfg(ctx context.Context, req *pb.GetTaskCfgRequest) (*pb.GetTaskCfgResponse, error) {
	var (
		resp2 *pb.GetTaskCfgResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	cfg := s.scheduler.GetTaskCfg(req.Name)

	if len(cfg) == 0 {
		return &pb.GetTaskCfgResponse{
			Result: false,
			Msg:    "task not found",
		}, nil
	}

	return &pb.GetTaskCfgResponse{
		Result: true,
		Cfg:    cfg,
	}, nil
}

// HandleError implements MasterServer.HandleError
func (s *Server) HandleError(ctx context.Context, req *pb.HandleErrorRequest) (*pb.HandleErrorResponse, error) {
	var (
		resp2 *pb.HandleErrorResponse
		err2  error
	)
	shouldRet := s.sharedLogic(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	sources := req.Sources
	if len(sources) == 0 {
		sources = s.getTaskResources(req.Task)
		log.L().Info(fmt.Sprintf("sources: %s", sources))
		if len(sources) == 0 {
			return &pb.HandleErrorResponse{
				Result: false,
				Msg:    fmt.Sprintf("task %s has no source or not exist, please check the task name and status", req.Task),
			}, nil
		}
	}

	workerReq := workerrpc.Request{
		Type: workerrpc.CmdHandleError,
		HandleError: &pb.HandleWorkerErrorRequest{
			Op:        req.Op,
			Task:      req.Task,
			BinlogPos: req.BinlogPos,
			Sqls:      req.Sqls,
		},
	}

	workerRespCh := make(chan *pb.CommonWorkerResponse, len(sources))
	var wg sync.WaitGroup
	for _, source := range sources {
		wg.Add(1)
		go func(source string) {
			defer wg.Done()
			worker := s.scheduler.GetWorkerBySource(source)
			if worker == nil {
				workerRespCh <- errorCommonWorkerResponse(fmt.Sprintf("source %s relevant worker-client not found", source), source, "")
				return
			}
			resp, err := worker.SendRequest(ctx, &workerReq, s.cfg.RPCTimeout)
			workerResp := &pb.CommonWorkerResponse{}
			if err != nil {
				workerResp = errorCommonWorkerResponse(err.Error(), source, worker.BaseInfo().Name)
			} else {
				workerResp = resp.HandleError
			}
			workerResp.Source = source
			workerRespCh <- workerResp
		}(source)
	}
	wg.Wait()

	workerResps := make([]*pb.CommonWorkerResponse, 0, len(sources))
	for len(workerRespCh) > 0 {
		workerResp := <-workerRespCh
		workerResps = append(workerResps, workerResp)
	}

	sort.Slice(workerResps, func(i, j int) bool {
		return workerResps[i].Source < workerResps[j].Source
	})

	return &pb.HandleErrorResponse{
		Result:  true,
		Sources: workerResps,
	}, nil
}

// sharedLogic does some shared logic for each RPC implementation
// arguments with `Pointer` suffix should be pointer to that variable its name indicated
// return `true` means caller should return with variable that `xxPointer` modified
func (s *Server) sharedLogic(ctx context.Context, req interface{}, respPointer interface{}, errPointer *error) bool {
	pc, _, _, _ := runtime.Caller(1)
	fullMethodName := runtime.FuncForPC(pc).Name()
	methodName := fullMethodName[strings.LastIndexByte(fullMethodName, '.')+1:]

	log.L().Info("", zap.Any("payload", req), zap.String("request", methodName))

	// origin code:
	//  isLeader, needForward := s.isLeaderAndNeedForward()
	//	if !isLeader {
	//		if needForward {
	//			return s.leaderClient.ListMember(ctx, req)
	//		}
	//		return nil, terror.ErrMasterRequestIsNotForwardToLeader
	//	}
	isLeader, needForward := s.isLeaderAndNeedForward()
	if isLeader {
		return false
	}
	if needForward {
		log.L().Info("forwarding", zap.String("from", s.cfg.Name), zap.String("to", s.leader), zap.String("request", methodName))
		params := []reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(req)}
		results := reflect.ValueOf(s.leaderClient).MethodByName(methodName).Call(params)
		// result's inner types should be (*pb.XXResponse, error), which is same as s.leaderClient.XXRPCMethod
		reflect.ValueOf(respPointer).Elem().Set(results[0])
		errInterface := results[1].Interface()
		// nil can't pass type conversion, so we handle it separately
		if errInterface == nil {
			*errPointer = nil
		} else {
			*errPointer = errInterface.(error)
		}
		return true
	}
	respType := reflect.ValueOf(respPointer).Elem().Type()
	reflect.ValueOf(respPointer).Elem().Set(reflect.Zero(respType))
	*errPointer = terror.ErrMasterRequestIsNotForwardToLeader
	return true
}
