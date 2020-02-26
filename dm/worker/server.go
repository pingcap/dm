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
	"github.com/pingcap/dm/dm/unit"
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

	// relay status will never be put in server.sourceStatus
	sourceStatus pb.SourceStatus
}

// NewServer creates a new Server
func NewServer(cfg *Config) *Server {
	s := Server{
		cfg: cfg,
	}
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

	bsm, revBound, err := ha.GetSourceBound(s.etcdClient, s.cfg.Name)
	if err != nil {
		// TODO: need retry
		return err
	}
	if bound, ok := bsm[s.cfg.Name]; ok {
		log.L().Warn("worker has been assigned source before keepalive")
		err = s.operateSourceBound(bound)
		s.setSourceStatus(bound.Source, err, true)
		if err != nil {
			log.L().Error("fail to operate sourceBound on worker", zap.String("worker", s.cfg.Name),
				zap.String("source", bound.Source))
		}
	}
	sourceBoundCh := make(chan ha.SourceBound, 10)
	sourceBoundErrCh := make(chan error, 10)
	s.wg.Add(2)
	go func() {
		defer s.wg.Done()
		ha.WatchSourceBound(s.ctx, s.etcdClient, s.cfg.Name, revBound+1, sourceBoundCh, sourceBoundErrCh)
	}()
	go func() {
		defer s.wg.Done()
		s.handleSourceBound(s.ctx, sourceBoundCh, sourceBoundErrCh)
	}()

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

	RegistryMetrics()
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

func (s *Server) getSourceStatus(needLock bool) pb.SourceStatus {
	if needLock {
		s.Lock()
		defer s.Unlock()
	}
	return s.sourceStatus
}

func (s *Server) setSourceStatus(source string, err error, needLock bool) {
	if needLock {
		s.Lock()
		defer s.Unlock()
	}
	s.sourceStatus = pb.SourceStatus{
		Source: source,
		Worker: s.cfg.Name,
	}
	if err != nil {
		s.sourceStatus.Result = &pb.ProcessResult{
			Errors: []*pb.ProcessError{
				unit.NewProcessError(pb.ErrorType_UnknownError, err),
			},
		}
	}
}

// if sourceID is set to "", worker will be closed directly
// if sourceID is not "", we will check sourceID with w.cfg.SourceID
func (s *Server) stopWorker(sourceID string) error {
	s.Lock()
	w := s.getWorker(false)
	if w == nil {
		s.Unlock()
		return terror.ErrWorkerNoStart
	}
	if sourceID != "" && w.cfg.SourceID != sourceID {
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

func (s *Server) handleSourceBound(ctx context.Context, boundCh chan ha.SourceBound, errCh chan error) {
	for {
		select {
		case <-ctx.Done():
			log.L().Info("worker server is closed, handleSourceBound will quit now")
			return
		case bound := <-boundCh:
			err := s.operateSourceBound(bound)
			s.setSourceStatus(bound.Source, err, true)
			if err != nil {
				// record the reason for operating source bound
				// TODO: add better metrics
				log.L().Error("fail to operate sourceBound on worker", zap.String("worker", s.cfg.Name),
					zap.String("source", bound.Source), zap.Error(err))
			}
		case err := <-errCh:
			// TODO: Deal with err
			log.L().Error("WatchSourceBound received an error", zap.Error(err))
		}
	}
}

func (s *Server) operateSourceBound(bound ha.SourceBound) error {
	if bound.IsDeleted {
		return s.stopWorker(bound.Source)
	}
	sourceCfg, _, err := ha.GetSourceCfg(s.etcdClient, bound.Source, bound.Revision)
	if err != nil {
		// TODO: need retry
		return err
	}
	return s.startWorker(&sourceCfg)
}

// StartSubTask implements WorkerServer.StartSubTask
func (s *Server) StartSubTask(ctx context.Context, req *pb.StartSubTaskRequest) (*pb.CommonWorkerResponse, error) {
	log.L().Info("", zap.String("request", "StartSubTask"), zap.Stringer("payload", req))
	cfg := config.NewSubTaskConfig()
	err := cfg.Decode(req.Task, true)
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
	w.StartSubTask(cfg)

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
	err := cfg.Decode(req.Task, true)
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

	sourceStatus := s.getSourceStatus(true)
	sourceStatus.Worker = s.cfg.Name
	resp := &pb.QueryStatusResponse{
		Result:       true,
		SourceStatus: &sourceStatus,
	}

	w := s.getWorker(true)
	if w == nil {
		log.L().Error("fail to call QueryStatus, because mysql worker has not been started")
		resp.Result = false
		resp.Msg = terror.ErrWorkerNoStart.Error()
		return resp, nil
	}

	resp.SubTaskStatus = w.QueryStatus(req.Name)
	if w.relayHolder != nil {
		sourceStatus.RelayStatus = w.relayHolder.Status()
	}
	if len(resp.SubTaskStatus) == 0 {
		resp.Msg = "no sub task started"
	}
	return resp, nil
}

// QueryError implements WorkerServer.QueryError
func (s *Server) QueryError(ctx context.Context, req *pb.QueryErrorRequest) (*pb.QueryErrorResponse, error) {
	log.L().Info("", zap.String("request", "QueryError"), zap.Stringer("payload", req))
	sourceStatus := s.getSourceStatus(true)
	sourceError := pb.SourceError{
		Source: sourceStatus.Source,
		Worker: s.cfg.Name,
	}
	if sourceStatus.Result != nil && len(sourceStatus.Result.Errors) > 0 {
		sourceError.SourceError = utils.JoinProcessErrors(sourceStatus.Result.Errors)
	}
	resp := &pb.QueryErrorResponse{
		Result:      true,
		SourceError: &sourceError,
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
		resp.SourceError.RelayError = w.relayHolder.Error()
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

func (s *Server) startWorker(cfg *config.SourceConfig) error {
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

	// we get the newest subtask stages directly which will omit the subtask stage PUT/DELETE event
	// because triggering these events is useless now
	subTaskStages, revSubTask, err := ha.GetSubTaskStage(s.etcdClient, cfg.SourceID, "")
	if err != nil {
		// TODO: need retry
		return err
	}
	subTaskCfgm, _, err := ha.GetSubTaskCfg(s.etcdClient, cfg.SourceID, "", revSubTask)
	if err != nil {
		// TODO: need retry
		return err
	}

	subTaskCfgs := make([]*config.SubTaskConfig, 0, len(subTaskCfgm))
	for _, subTaskCfg := range subTaskCfgm {
		subTaskCfg.LogLevel = s.cfg.LogLevel
		subTaskCfg.LogFile = s.cfg.LogFile

		subTaskCfgs = append(subTaskCfgs, &subTaskCfg)
	}

	if cfg.EnableRelay {
		dctx, dcancel := context.WithTimeout(s.etcdClient.Ctx(), time.Duration(len(subTaskCfgs))*3*time.Second)
		defer dcancel()
		minPos, err1 := getMinPosInAllSubTasks(dctx, subTaskCfgs)
		if err1 != nil {
			return err1
		}

		// TODO: support GTID
		// don't contain GTID information in checkpoint table, just set it to empty
		if minPos != nil {
			cfg.RelayBinLogName = binlog.AdjustPosition(*minPos).Name
			cfg.RelayBinlogGTID = ""
		}
	}

	log.L().Info("start worker", zap.String("sourceCfg", cfg.String()), zap.Reflect("subTasks", subTaskCfgs))

	w, err := NewWorker(cfg, s.etcdClient)
	if err != nil {
		return err
	}
	s.setWorker(w, false)

	startRelay := false
	var revRelay int64
	if cfg.EnableRelay {
		var relayStage ha.Stage
		// we get the newest relay stages directly which will omit the relay stage PUT/DELETE event
		// because triggering these events is useless now
		relayStage, revRelay, err = ha.GetRelayStage(s.etcdClient, cfg.SourceID)
		if err != nil {
			// TODO: need retry
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
		// TODO: add more mechanism to wait
		return terror.ErrWorkerNoStart
	}

	for _, subTaskCfg := range subTaskCfgs {
		expectStage := subTaskStages[subTaskCfg.Name]
		if expectStage.IsDeleted || expectStage.Expect != pb.Stage_Running {
			continue
		}
		w.StartSubTask(subTaskCfg)
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
		w.handleSubTaskStage(w.ctx, subTaskStageCh, subTaskErrCh)
	}()

	if cfg.EnableRelay {
		relayStageCh := make(chan ha.Stage, 10)
		relayErrCh := make(chan error, 10)
		w.wg.Add(2)
		go func() {
			defer w.wg.Done()
			ha.WatchRelayStage(w.ctx, s.etcdClient, cfg.SourceID, revRelay+1, relayStageCh, relayErrCh)
		}()
		go func() {
			defer w.wg.Done()
			w.handleRelayStage(w.ctx, relayStageCh, relayErrCh)
		}()
	}

	return nil
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
	subTaskCfg2, err := subTaskCfg.DecryptPassword()
	if err != nil {
		return nil, errors.Annotate(err, "get min position from checkpoint")
	}

	tctx := tcontext.NewContext(ctx, log.L())
	checkpoint := syncer.NewRemoteCheckPoint(tctx, subTaskCfg2, subTaskCfg2.SourceID)
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
