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

package worker

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/binlog"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/ha"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
	"github.com/pingcap/dm/syncer"

	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go/sync2"
	"github.com/soheilhy/cmux"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	cmuxReadTimeout         = 10 * time.Second
	dialTimeout             = 3 * time.Second
	keepaliveTimeout        = 3 * time.Second
	keepaliveTime           = 3 * time.Second
	retryConnectSleepTime   = time.Second
	getMinPosForSubTaskFunc = getMinPosForSubTask
)

// Server accepts RPC requests
// dispatches requests to worker
// sends responses to RPC client
type Server struct {
	sync.Mutex
	wg     sync.WaitGroup
	closed sync2.AtomicBool
	ctx    context.Context
	cancel context.CancelFunc

	cfg *Config

	rootLis    net.Listener
	svr        *grpc.Server
	worker     *Worker
	etcdClient *clientv3.Client

	// false: has retried connecting to master again.
	retryConnectMaster sync2.AtomicBool
}

// NewServer creates a new Server
func NewServer(cfg *Config) *Server {
	s := Server{
		cfg: cfg,
	}
	s.retryConnectMaster.Set(true)
	s.closed.Set(true) // not start yet
	return &s
}

// Start starts to serving
func (s *Server) Start() error {
	var err error
	s.rootLis, err = net.Listen("tcp", s.cfg.WorkerAddr)
	if err != nil {
		return terror.ErrWorkerStartService.Delegate(err)
	}

	log.L().Info("Start Server")
	s.setWorker(nil, true)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.etcdClient, err = clientv3.New(clientv3.Config{
		Endpoints:            GetJoinURLs(s.cfg.Join),
		DialTimeout:          dialTimeout,
		DialKeepAliveTime:    keepaliveTime,
		DialKeepAliveTimeout: keepaliveTimeout,
	})
	if err != nil {
		return err
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		// worker keepalive with master
		// If worker loses connect from master, it would stop all task and try to connect master again.
		s.KeepAlive()
	}()

	// create a cmux
	m := cmux.New(s.rootLis)
	m.SetReadTimeout(cmuxReadTimeout) // set a timeout, ref: https://github.com/pingcap/tidb-binlog/pull/352

	// match connections in order: first gRPC, then HTTP
	grpcL := m.MatchWithWriters(cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
	httpL := m.Match(cmux.HTTP1Fast())

	s.svr = grpc.NewServer()
	pb.RegisterWorkerServer(s.svr, s)
	go func() {
		err2 := s.svr.Serve(grpcL)
		if err2 != nil && !common.IsErrNetClosing(err2) && err2 != cmux.ErrListenerClosed {
			log.L().Error("fail to start gRPC server", log.ShortError(err2))
		}
	}()
	go InitStatus(httpL) // serve status

	s.closed.Set(false)
	log.L().Info("start gRPC API", zap.String("listened address", s.cfg.WorkerAddr))
	err = m.Serve()
	if err != nil && common.IsErrNetClosing(err) {
		err = nil
	}
	return terror.ErrWorkerStartService.Delegate(err)
}

func (s *Server) doClose() {
	s.Lock()
	defer s.Unlock()
	if s.closed.Get() {
		return
	}

	if s.rootLis != nil {
		err := s.rootLis.Close()
		if err != nil && !common.IsErrNetClosing(err) {
			log.L().Error("fail to close net listener", log.ShortError(err))
		}
	}
	if s.svr != nil {
		// GracefulStop can not cancel active stream RPCs
		// and the stream RPC may block on Recv or Send
		// so we use Stop instead to cancel all active RPCs
		s.svr.Stop()
	}

	// close worker and wait for return
	s.cancel()
	if w := s.getWorker(false); w != nil {
		w.Close()
	}
	s.closed.Set(true)
}

// Close close the RPC server, this function can be called multiple times
func (s *Server) Close() {
	s.doClose()
	s.wg.Wait()
}

func (s *Server) getWorker(needLock bool) *Worker {
	if needLock {
		s.Lock()
		defer s.Unlock()
	}
	return s.worker
}

func (s *Server) setWorker(worker *Worker, needLock bool) {
	if needLock {
		s.Lock()
		defer s.Unlock()
	}
	s.worker = worker
}

func (s *Server) stopWorker(sourceID string) error {
	s.Lock()
	w := s.getWorker(false)
	if w == nil {
		s.Unlock()
		return terror.ErrWorkerNoStart
	}
	if w.cfg.SourceID != sourceID {
		s.Unlock()
		return terror.ErrWorkerSourceNotMatch
	}
	s.setWorker(nil, false)
	s.Unlock()
	w.Close()
	return nil
}

func (s *Server) retryWriteEctd(ops ...clientv3.Op) string {
	retryTimes := 3
	cliCtx, canc := context.WithTimeout(s.etcdClient.Ctx(), time.Second)
	defer canc()
	for {
		res, err := s.etcdClient.Txn(cliCtx).Then(ops...).Commit()
		retryTimes--
		if err == nil {
			if res.Succeeded {
				return ""
			} else if retryTimes <= 0 {
				return "failed to write data in etcd"
			}
		} else if retryTimes <= 0 {
			return errors.ErrorStack(err)
		}
		time.Sleep(time.Millisecond * 50)
	}
}

// StartSubTask implements WorkerServer.StartSubTask
func (s *Server) StartSubTask(ctx context.Context, req *pb.StartSubTaskRequest) (*pb.CommonWorkerResponse, error) {
	log.L().Info("", zap.String("request", "StartSubTask"), zap.Stringer("payload", req))
	cfg := config.NewSubTaskConfig()
	err := cfg.Decode(req.Task)
	if err != nil {
		err = terror.Annotatef(err, "decode subtask config from request %+v", req.Task)
		log.L().Error("fail to decode task", zap.String("request", "StartSubTask"), zap.Stringer("payload", req), zap.Error(err))
		return &pb.CommonWorkerResponse{
			Result: false,
			Msg:    err.Error(),
		}, nil
	}
	resp := &pb.CommonWorkerResponse{
		Result: true,
		Msg:    "",
	}
	w := s.getWorker(true)
	if w == nil || w.cfg.SourceID != cfg.SourceID {
		log.L().Error("fail to call StartSubTask, because mysql worker has not been started")
		resp.Result = false
		resp.Msg = terror.ErrWorkerNoStart.Error()
		return resp, nil
	}

	cfg.LogLevel = s.cfg.LogLevel
	cfg.LogFile = s.cfg.LogFile
	err = w.StartSubTask(cfg)

	if err != nil {
		err = terror.Annotatef(err, "start sub task %s", cfg.Name)
		log.L().Error("fail to start subtask", zap.String("request", "StartSubTask"), zap.Stringer("payload", req), zap.Error(err))
		resp.Result = false
		resp.Msg = err.Error()
	}

	if resp.Result {
		op1 := clientv3.OpPut(common.UpstreamSubTaskKeyAdapter.Encode(cfg.SourceID, cfg.Name), req.Task)
		resp.Msg = s.retryWriteEctd(op1)
	}
	return resp, nil
}

// OperateSubTask implements WorkerServer.OperateSubTask
func (s *Server) OperateSubTask(ctx context.Context, req *pb.OperateSubTaskRequest) (*pb.OperateSubTaskResponse, error) {
	resp := &pb.OperateSubTaskResponse{
		Result: true,
		Op:     req.Op,
		Msg:    "",
	}
	w := s.getWorker(true)
	if w == nil {
		log.L().Error("fail to call OperateSubTask, because mysql worker has not been started")
		resp.Result = false
		resp.Msg = terror.ErrWorkerNoStart.Error()
		return resp, nil
	}

	log.L().Info("", zap.String("request", "OperateSubTask"), zap.Stringer("payload", req))
	err := w.OperateSubTask(req.Name, req.Op)
	if err != nil {
		err = terror.Annotatef(err, "operate(%s) sub task %s", req.Op.String(), req.Name)
		log.L().Error("fail to operate task", zap.String("request", "OperateSubTask"), zap.Stringer("payload", req), zap.Error(err))
		resp.Result = false
		resp.Msg = err.Error()
	} else {
		// TODO: change task state.
		// clean subtask config when we stop the subtask
		if req.Op == pb.TaskOp_Stop {
			op1 := clientv3.OpDelete(common.UpstreamSubTaskKeyAdapter.Encode(w.cfg.SourceID, req.Name))
			resp.Msg = s.retryWriteEctd(op1)
			resp.Result = len(resp.Msg) == 0
		}
	}
	return resp, nil
}

// UpdateSubTask implements WorkerServer.UpdateSubTask
func (s *Server) UpdateSubTask(ctx context.Context, req *pb.UpdateSubTaskRequest) (*pb.CommonWorkerResponse, error) {
	log.L().Info("", zap.String("request", "UpdateSubTask"), zap.Stringer("payload", req))
	cfg := config.NewSubTaskConfig()
	err := cfg.Decode(req.Task)
	if err != nil {
		err = terror.Annotatef(err, "decode config from request %+v", req.Task)
		log.L().Error("fail to decode subtask", zap.String("request", "UpdateSubTask"), zap.Stringer("payload", req), zap.Error(err))
		return &pb.CommonWorkerResponse{
			Result: false,
			Msg:    err.Error(),
		}, nil
	}
	resp := &pb.CommonWorkerResponse{
		Result: true,
		Msg:    "",
	}
	w := s.getWorker(true)
	if w == nil {
		log.L().Error("fail to call StartSubTask, because mysql worker has not been started")
		resp.Result = false
		resp.Msg = terror.ErrWorkerNoStart.Error()
		return resp, nil
	}
	err = w.UpdateSubTask(cfg)
	if err != nil {
		err = terror.Annotatef(err, "update sub task %s", cfg.Name)
		log.L().Error("fail to update task", zap.String("request", "UpdateSubTask"), zap.Stringer("payload", req), zap.Error(err))
		resp.Result = false
		resp.Msg = err.Error()
	} else {
		op1 := clientv3.OpPut(common.UpstreamSubTaskKeyAdapter.Encode(cfg.SourceID, cfg.Name), req.Task)
		resp.Msg = s.retryWriteEctd(op1)
	}
	return resp, nil
}

// QueryStatus implements WorkerServer.QueryStatus
func (s *Server) QueryStatus(ctx context.Context, req *pb.QueryStatusRequest) (*pb.QueryStatusResponse, error) {
	log.L().Info("", zap.String("request", "QueryStatus"), zap.Stringer("payload", req))

	resp := &pb.QueryStatusResponse{
		Result: true,
	}

	w := s.getWorker(true)
	if w == nil {
		log.L().Error("fail to call QueryStatus, because mysql worker has not been started")
		resp.Result = false
		resp.Msg = terror.ErrWorkerNoStart.Error()
		return resp, nil
	}

	relayStatus := &pb.RelayStatus{
		Result: &pb.ProcessResult{
			Detail: []byte("relay is not enabled"),
		},
	}
	if s.worker.relayHolder != nil {
		relayStatus = s.worker.relayHolder.Status()
	}

	resp.SubTaskStatus = w.QueryStatus(req.Name)
	resp.RelayStatus = relayStatus
	resp.Source = w.cfg.SourceID
	if len(resp.SubTaskStatus) == 0 {
		resp.Msg = "no sub task started"
	}
	return resp, nil
}

// QueryError implements WorkerServer.QueryError
func (s *Server) QueryError(ctx context.Context, req *pb.QueryErrorRequest) (*pb.QueryErrorResponse, error) {
	log.L().Info("", zap.String("request", "QueryError"), zap.Stringer("payload", req))
	resp := &pb.QueryErrorResponse{
		Result: true,
	}

	w := s.getWorker(true)
	if w == nil {
		log.L().Error("fail to call StartSubTask, because mysql worker has not been started")
		resp.Result = false
		resp.Msg = terror.ErrWorkerNoStart.Error()
		return resp, nil
	}

	resp.SubTaskError = w.QueryError(req.Name)
	if w.relayHolder != nil {
		resp.RelayError = w.relayHolder.Error()
	}
	return resp, nil
}

// HandleSQLs implements WorkerServer.HandleSQLs
func (s *Server) HandleSQLs(ctx context.Context, req *pb.HandleSubTaskSQLsRequest) (*pb.CommonWorkerResponse, error) {
	log.L().Info("", zap.String("request", "HandleSQLs"), zap.Stringer("payload", req))
	w := s.getWorker(true)
	if w == nil {
		log.L().Error("fail to call StartSubTask, because mysql worker has not been started")
		return makeCommonWorkerResponse(terror.ErrWorkerNoStart.Generate()), nil
	}

	err := w.HandleSQLs(ctx, req)
	if err != nil {
		log.L().Error("fail to handle sqls", zap.String("request", "HandleSQLs"), zap.Stringer("payload", req), zap.Error(err))
	}
	// TODO: check whether this interface need to store message in ETCD
	return makeCommonWorkerResponse(err), nil
}

// SwitchRelayMaster implements WorkerServer.SwitchRelayMaster
func (s *Server) SwitchRelayMaster(ctx context.Context, req *pb.SwitchRelayMasterRequest) (*pb.CommonWorkerResponse, error) {
	log.L().Info("", zap.String("request", "SwitchRelayMaster"), zap.Stringer("payload", req))
	w := s.getWorker(true)
	if w == nil {
		log.L().Error("fail to call StartSubTask, because mysql worker has not been started")
		return makeCommonWorkerResponse(terror.ErrWorkerNoStart.Generate()), nil
	}

	err := w.SwitchRelayMaster(ctx, req)
	if err != nil {
		log.L().Error("fail to switch relay master", zap.String("request", "SwitchRelayMaster"), zap.Stringer("payload", req), zap.Error(err))
	}
	// TODO: check whether this interface need to store message in ETCD
	return makeCommonWorkerResponse(err), nil
}

// OperateRelay implements WorkerServer.OperateRelay
func (s *Server) OperateRelay(ctx context.Context, req *pb.OperateRelayRequest) (*pb.OperateRelayResponse, error) {
	log.L().Info("", zap.String("request", "OperateRelay"), zap.Stringer("payload", req))
	resp := &pb.OperateRelayResponse{
		Op:     req.Op,
		Result: false,
	}

	w := s.getWorker(true)
	if w == nil {
		log.L().Error("fail to call StartSubTask, because mysql worker has not been started")
		resp.Msg = terror.ErrWorkerNoStart.Error()
		return resp, nil
	}

	err := w.OperateRelay(ctx, req)
	if err != nil {
		log.L().Error("fail to operate relay", zap.String("request", "OperateRelay"), zap.Stringer("payload", req), zap.Error(err))
		resp.Msg = errors.ErrorStack(err)
		return resp, nil
	}

	// TODO: check whether this interface need to store message in ETCD
	resp.Result = true
	return resp, nil
}

// PurgeRelay implements WorkerServer.PurgeRelay
func (s *Server) PurgeRelay(ctx context.Context, req *pb.PurgeRelayRequest) (*pb.CommonWorkerResponse, error) {
	log.L().Info("", zap.String("request", "PurgeRelay"), zap.Stringer("payload", req))
	w := s.getWorker(true)
	if w == nil {
		log.L().Error("fail to call StartSubTask, because mysql worker has not been started")
		return makeCommonWorkerResponse(terror.ErrWorkerNoStart.Generate()), nil
	}

	err := w.PurgeRelay(ctx, req)
	if err != nil {
		log.L().Error("fail to purge relay", zap.String("request", "PurgeRelay"), zap.Stringer("payload", req), zap.Error(err))
	}
	// TODO: check whether this interface need to store message in ETCD
	return makeCommonWorkerResponse(err), nil
}

// UpdateRelayConfig updates config for relay and (dm-worker)
func (s *Server) UpdateRelayConfig(ctx context.Context, req *pb.UpdateRelayRequest) (*pb.CommonWorkerResponse, error) {
	log.L().Info("", zap.String("request", "UpdateRelayConfig"), zap.Stringer("payload", req))
	w := s.getWorker(true)
	if w == nil {
		log.L().Error("fail to call StartSubTask, because mysql worker has not been started")
		return makeCommonWorkerResponse(terror.ErrWorkerNoStart.Generate()), nil
	}

	err := w.UpdateRelayConfig(ctx, req.Content)
	if err != nil {
		log.L().Error("fail to update relay config", zap.String("request", "UpdateRelayConfig"), zap.Stringer("payload", req), zap.Error(err))
	}
	// TODO: check whether this interface need to store message in ETCD
	return makeCommonWorkerResponse(err), nil
}

// QueryWorkerConfig return worker config
// worker config is defined in worker directory now,
// to avoid circular import, we only return db config
func (s *Server) QueryWorkerConfig(ctx context.Context, req *pb.QueryWorkerConfigRequest) (*pb.QueryWorkerConfigResponse, error) {
	log.L().Info("", zap.String("request", "QueryWorkerConfig"), zap.Stringer("payload", req))
	resp := &pb.QueryWorkerConfigResponse{
		Result: true,
	}

	w := s.getWorker(true)
	if w == nil {
		log.L().Error("fail to call StartSubTask, because mysql worker has not been started")
		resp.Result = false
		resp.Msg = terror.ErrWorkerNoStart.Error()
		return resp, nil
	}

	workerCfg, err := w.QueryConfig(ctx)
	if err != nil {
		resp.Result = false
		resp.Msg = errors.ErrorStack(err)
		log.L().Error("fail to query worker config", zap.String("request", "QueryWorkerConfig"), zap.Stringer("payload", req), zap.Error(err))
		return resp, nil
	}

	rawConfig, err := workerCfg.From.Toml()
	if err != nil {
		resp.Result = false
		resp.Msg = errors.ErrorStack(err)
		log.L().Error("fail to marshal worker config", zap.String("request", "QueryWorkerConfig"), zap.Stringer("worker from config", &workerCfg.From), zap.Error(err))
	}

	resp.Content = rawConfig
	resp.Source = workerCfg.SourceID
	return resp, nil
}

// MigrateRelay migrate relay to original binlog pos
func (s *Server) MigrateRelay(ctx context.Context, req *pb.MigrateRelayRequest) (*pb.CommonWorkerResponse, error) {
	log.L().Info("", zap.String("request", "MigrateRelay"), zap.Stringer("payload", req))
	w := s.getWorker(true)
	if w == nil {
		log.L().Error("fail to call StartSubTask, because mysql worker has not been started")
		return makeCommonWorkerResponse(terror.ErrWorkerNoStart.Generate()), nil
	}

	err := w.MigrateRelay(ctx, req.BinlogName, req.BinlogPos)
	if err != nil {
		log.L().Error("fail to migrate relay", zap.String("request", "MigrateRelay"), zap.Stringer("payload", req), zap.Error(err))
	}
	// TODO: check whether this interface need to store message in ETCD
	return makeCommonWorkerResponse(err), nil
}

func (s *Server) startWorker(cfg *config.MysqlConfig) error {
	s.Lock()
	defer s.Unlock()
	if w := s.getWorker(false); w != nil {
		if w.cfg.SourceID == cfg.SourceID {
			// This mysql task has started. It may be a repeated request. Just return true
			log.L().Info("This mysql task has started. It may be a repeated request. Just return true", zap.String("sourceID", s.worker.cfg.SourceID))
			return nil
		}
		return terror.ErrWorkerAlreadyStart.Generate()
	}

	subTaskStages, revSubTask, err := ha.GetSubTaskStage(s.etcdClient, cfg.SourceID, "")
	if err != nil {
		return err
	}
	subTaskCfgm, _, err := ha.GetSubTaskCfg(s.etcdClient, cfg.SourceID, "", revSubTask)
	if err != nil {
		return err
	}

	subTaskCfgs := make([]*config.SubTaskConfig, 0, len(subTaskCfgm))
	for _, subTaskCfg := range subTaskCfgm {
		subTaskCfg.LogLevel = s.cfg.LogLevel
		subTaskCfg.LogFile = s.cfg.LogFile

		subTaskCfgs = append(subTaskCfgs, &subTaskCfg)
	}

	dctx, dcancel := context.WithTimeout(s.etcdClient.Ctx(), time.Duration(len(subTaskCfgs))*3*time.Second)
	defer dcancel()
	minPos, err := getMinPosInAllSubTasks(dctx, subTaskCfgs)
	if err != nil {
		return err
	}

	// TODO: support GTID
	// don't contain GTID information in checkpoint table, just set it to empty
	if minPos != nil {
		cfg.RelayBinLogName = binlog.AdjustPosition(*minPos).Name
		cfg.RelayBinlogGTID = ""
	}

	log.L().Info("start workers", zap.Reflect("subTasks", subTaskCfgs))

	w, err := NewWorker(cfg, s.etcdClient)
	if err != nil {
		return err
	}
	s.setWorker(w, false)

	startRelay := false
	var revRelay int64
	if w.cfg.EnableRelay {
		var relayStage ha.Stage
		relayStage, revRelay, err = ha.GetRelayStage(s.etcdClient, cfg.SourceID)
		if err != nil {
			return err
		}
		startRelay = !relayStage.IsDeleted && relayStage.Expect == pb.Stage_Running
	}
	go func() {
		w.Start(startRelay)
	}()

	isStarted := utils.WaitSomething(50, 100*time.Millisecond, func() bool {
		return w.closed.Get() == closedFalse
	})
	if !isStarted {
		return nil
	}

	for _, subTaskCfg := range subTaskCfgs {
		expectStage := subTaskStages[subTaskCfg.Name]
		if expectStage.IsDeleted || expectStage.Expect != pb.Stage_Running {
			continue
		}
		if err = w.StartSubTask(subTaskCfg); err != nil {
			return err
		}
		log.L().Info("load subtask successful", zap.String("sourceID", subTaskCfg.SourceID), zap.String("task", subTaskCfg.Name))
	}

	subTaskStageCh := make(chan ha.Stage, 10)
	subTaskErrCh := make(chan error, 10)
	w.wg.Add(2)
	go func() {
		defer w.wg.Done()
		ha.WatchSubTaskStage(w.ctx, s.etcdClient, cfg.SourceID, revSubTask+1, subTaskStageCh, subTaskErrCh)
	}()
	go func() {
		defer w.wg.Done()
		w.HandleSubTaskStage(w.ctx, subTaskStageCh, subTaskErrCh)
	}()

	if w.cfg.EnableRelay {
		relayStageCh := make(chan ha.Stage, 10)
		relayErrCh := make(chan error, 10)
		w.wg.Add(2)
		go func() {
			defer w.wg.Done()
			ha.WatchRelayStage(w.ctx, s.etcdClient, cfg.SourceID, revRelay+1, relayStageCh, relayErrCh)
		}()
		go func() {
			defer w.wg.Done()
			w.HandleRelayStage(w.ctx, relayStageCh, relayErrCh)
		}()
	}

	return nil
}

// OperateMysqlWorker create a new mysql task which will be running in this Server
func (s *Server) OperateMysqlWorker(ctx context.Context, req *pb.MysqlWorkerRequest) (*pb.MysqlWorkerResponse, error) {
	log.L().Info("", zap.String("request", "OperateMysqlWorker"), zap.Stringer("payload", req))
	resp := &pb.MysqlWorkerResponse{
		Result: true,
		Msg:    "Operate mysql task successfully",
	}
	cfg := config.NewMysqlConfig()
	err := cfg.Parse(req.Config)
	if err != nil {
		resp.Result = false
		resp.Msg = errors.ErrorStack(err)
		return resp, nil
	}
	if req.Op == pb.WorkerOp_UpdateConfig {
		if err = s.stopWorker(cfg.SourceID); err != nil {
			resp.Result = false
			resp.Msg = errors.ErrorStack(err)
			return resp, nil
		}
	} else if req.Op == pb.WorkerOp_StopWorker {
		if err = s.stopWorker(cfg.SourceID); err == terror.ErrWorkerSourceNotMatch {
			resp.Result = false
			resp.Msg = errors.ErrorStack(err)
		}
	}
	if resp.Result && (req.Op == pb.WorkerOp_UpdateConfig || req.Op == pb.WorkerOp_StartWorker) {
		err = s.startWorker(cfg)
	}
	if err != nil {
		resp.Result = false
		resp.Msg = errors.ErrorStack(err)
	}
	if resp.Result {
		op1 := clientv3.OpPut(common.UpstreamConfigKeyAdapter.Encode(cfg.SourceID), req.Config)
		op2 := clientv3.OpPut(common.UpstreamBoundWorkerKeyAdapter.Encode(s.cfg.AdvertiseAddr), cfg.SourceID)
		if req.Op == pb.WorkerOp_StopWorker {
			op1 = clientv3.OpDelete(common.UpstreamConfigKeyAdapter.Encode(cfg.SourceID))
			op2 = clientv3.OpDelete(common.UpstreamBoundWorkerKeyAdapter.Encode(s.cfg.AdvertiseAddr))
		}
		resp.Msg = s.retryWriteEctd(op1, op2)
		// Because etcd was deployed with master in a single process, if we can not write data into etcd, most probably
		// the have lost connect from master.
	}
	return resp, nil
}

func makeCommonWorkerResponse(reqErr error) *pb.CommonWorkerResponse {
	resp := &pb.CommonWorkerResponse{
		Result: true,
	}
	if reqErr != nil {
		resp.Result = false
		resp.Msg = errors.ErrorStack(reqErr)
	}
	return resp
}

// all subTask in subTaskCfgs should have same source
// this function return the min position in all subtasks, used for relay's position
func getMinPosInAllSubTasks(ctx context.Context, subTaskCfgs []*config.SubTaskConfig) (minPos *mysql.Position, err error) {
	for _, subTaskCfg := range subTaskCfgs {
		pos, err := getMinPosForSubTaskFunc(ctx, subTaskCfg)
		if err != nil {
			return nil, err
		}

		if pos == nil {
			continue
		}

		if minPos == nil {
			minPos = pos
		} else {
			if minPos.Compare(*pos) >= 1 {
				minPos = pos
			}
		}
	}

	return minPos, nil
}

func getMinPosForSubTask(ctx context.Context, subTaskCfg *config.SubTaskConfig) (minPos *mysql.Position, err error) {
	if subTaskCfg.Mode == config.ModeFull {
		return nil, nil
	}

	tctx := tcontext.NewContext(ctx, log.L())
	checkpoint := syncer.NewRemoteCheckPoint(tctx, subTaskCfg, subTaskCfg.SourceID)
	err = checkpoint.Init(tctx)
	if err != nil {
		return nil, errors.Annotate(err, "get min position from checkpoint")
	}
	defer checkpoint.Close()

	err = checkpoint.Load(tctx, nil)
	if err != nil {
		return nil, errors.Annotate(err, "get min position from checkpoint")
	}

	pos := checkpoint.GlobalPoint()
	return &pos, nil
}
