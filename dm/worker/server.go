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
	"io"
	"net"
	"sync"
	"time"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/errors"
	"github.com/siddontang/go/sync2"
	"github.com/soheilhy/cmux"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	cmuxReadTimeout  = 10 * time.Second
	dialTimeout      = 3 * time.Second
	keepaliveTimeout = 3 * time.Second
	keepaliveTime    = 10 * time.Second
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

	_, _, err = s.splitHostPort()
	if err != nil {
		return err
	}

	s.rootLis, err = net.Listen("tcp", s.cfg.WorkerAddr)
	if err != nil {
		return terror.ErrWorkerStartService.Delegate(err)
	}

	s.worker = nil
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
		shouldExit := false
		shouldStop := false
		for !shouldExit {
			shouldExit, err = s.KeepAlive()
			if err != nil || !shouldExit {
				if shouldStop {
					s.Lock()
					if s.worker != nil {
						s.worker.Close()
						s.worker = nil
					}
					s.Unlock()
					shouldStop = false
				} else {
					// Try to connect master again before stop worker
					shouldStop = true
				}
				ch := time.NewTicker(5 * time.Second)
				select {
				case <-s.ctx.Done():
					shouldExit = true
					break
				case <-ch.C:
					// Try to connect master again
					break
				}
			}
		}
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
	if s.worker != nil {
		s.worker.Close()
	}
	s.closed.Set(true)
}

// Close close the RPC server, this function can be called multiple times
func (s *Server) Close() {
	s.doClose()
	s.wg.Wait()
}

func (s *Server) checkWorkerStart() *Worker {
	s.Lock()
	defer s.Unlock()
	return s.worker
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
	w := s.checkWorkerStart()
	if w == nil {
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
	} else {
		ctx, cancel := context.WithTimeout(s.etcdClient.Ctx(), 3*time.Second)
		defer cancel()
		_, err = s.etcdClient.Put(ctx, common.UpstreamSubTaskKeyAdapter.Encode(s.cfg.WorkerAddr), cfg.String())
		if err != nil {
			resp.Result = false
			resp.Msg = err.Error()
			// FIXME: handle error
			_ = w.OperateSubTask(cfg.Name, pb.TaskOp_Stop)
		}
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
	w := s.checkWorkerStart()
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
	w := s.checkWorkerStart()
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
	}
	return resp, nil
}

// QueryStatus implements WorkerServer.QueryStatus
func (s *Server) QueryStatus(ctx context.Context, req *pb.QueryStatusRequest) (*pb.QueryStatusResponse, error) {
	log.L().Info("", zap.String("request", "QueryStatus"), zap.Stringer("payload", req))
	resp := &pb.QueryStatusResponse{
		Result: true,
	}

	w := s.checkWorkerStart()
	if w == nil {
		log.L().Error("fail to call QueryStatus, because mysql worker has not been started")
		resp.Result = false
		resp.Msg = terror.ErrWorkerNoStart.Error()
		return resp, nil
	}

	resp.SubTaskStatus = w.QueryStatus(req.Name)
	resp.RelayStatus = w.relayHolder.Status()
	resp.SourceID = w.cfg.SourceID
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

	w := s.checkWorkerStart()
	if w == nil {
		log.L().Error("fail to call StartSubTask, because mysql worker has not been started")
		resp.Result = false
		resp.Msg = terror.ErrWorkerNoStart.Error()
		return resp, nil
	}

	resp.SubTaskError = w.QueryError(req.Name)
	resp.RelayError = w.relayHolder.Error()
	return resp, nil
}

// FetchDDLInfo implements WorkerServer.FetchDDLInfo
// we do ping-pong send-receive on stream for DDL (lock) info
// if error occurred in Send / Recv, just retry in client
func (s *Server) FetchDDLInfo(stream pb.Worker_FetchDDLInfoServer) error {
	log.L().Info("", zap.String("request", "FetchDDLInfo"))
	w := s.checkWorkerStart()
	if w == nil {
		log.L().Error("fail to call StartSubTask, because mysql worker has not been started")
		return terror.ErrWorkerNoStart.Generate()
	}

	var ddlInfo *pb.DDLInfo
	for {
		// try fetch pending to sync DDL info from worker
		ddlInfo = w.FetchDDLInfo(stream.Context())
		if ddlInfo == nil {
			return nil // worker closed or context canceled
		}
		log.L().Info("", zap.String("request", "FetchDDLInfo"), zap.Stringer("ddl info", ddlInfo))
		// send DDLInfo to dm-master
		err := stream.Send(ddlInfo)
		if err != nil {
			log.L().Error("fail to send DDLInfo to RPC stream", zap.String("request", "FetchDDLInfo"), zap.Stringer("ddl info", ddlInfo), log.ShortError(err))
			return err
		}

		// receive DDLLockInfo from dm-master
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			log.L().Error("fail to receive DDLLockInfo from RPC stream", zap.String("request", "FetchDDLInfo"), zap.Stringer("ddl info", ddlInfo), log.ShortError(err))
			return err
		}
		log.L().Info("receive DDLLockInfo", zap.String("request", "FetchDDLInfo"), zap.Stringer("ddl lock info", in))

		//ddlInfo = nil // clear and protect to put it back

		err = w.RecordDDLLockInfo(in)
		if err != nil {
			// if error occurred when recording DDLLockInfo, log an error
			// user can handle this case using dmctl
			log.L().Error("fail to record DDLLockInfo", zap.String("request", "FetchDDLInfo"), zap.Stringer("ddl lock info", in), zap.Error(err))
		}
	}
}

// ExecuteDDL implements WorkerServer.ExecuteDDL
func (s *Server) ExecuteDDL(ctx context.Context, req *pb.ExecDDLRequest) (*pb.CommonWorkerResponse, error) {
	log.L().Info("", zap.String("request", "ExecuteDDL"), zap.Stringer("payload", req))
	w := s.checkWorkerStart()
	if w == nil {
		log.L().Error("fail to call StartSubTask, because mysql worker has not been started")
		return makeCommonWorkerResponse(terror.ErrWorkerNoStart.Generate()), nil
	}

	err := w.ExecuteDDL(ctx, req)
	if err != nil {
		log.L().Error("fail to execute ddl", zap.String("request", "ExecuteDDL"), zap.Stringer("payload", req), zap.Error(err))
	}
	return makeCommonWorkerResponse(err), nil
}

// BreakDDLLock implements WorkerServer.BreakDDLLock
func (s *Server) BreakDDLLock(ctx context.Context, req *pb.BreakDDLLockRequest) (*pb.CommonWorkerResponse, error) {
	log.L().Info("", zap.String("request", "BreakDDLLock"), zap.Stringer("payload", req))
	w := s.checkWorkerStart()
	if w == nil {
		log.L().Error("fail to call StartSubTask, because mysql worker has not been started")
		return makeCommonWorkerResponse(terror.ErrWorkerNoStart.Generate()), nil
	}

	err := w.BreakDDLLock(ctx, req)
	if err != nil {
		log.L().Error("fail to break ddl lock", zap.String("request", "BreakDDLLock"), zap.Stringer("payload", req), zap.Error(err))
	}
	return makeCommonWorkerResponse(err), nil
}

// HandleSQLs implements WorkerServer.HandleSQLs
func (s *Server) HandleSQLs(ctx context.Context, req *pb.HandleSubTaskSQLsRequest) (*pb.CommonWorkerResponse, error) {
	log.L().Info("", zap.String("request", "HandleSQLs"), zap.Stringer("payload", req))
	w := s.checkWorkerStart()
	if w == nil {
		log.L().Error("fail to call StartSubTask, because mysql worker has not been started")
		return makeCommonWorkerResponse(terror.ErrWorkerNoStart.Generate()), nil
	}

	err := w.HandleSQLs(ctx, req)
	if err != nil {
		log.L().Error("fail to handle sqls", zap.String("request", "HandleSQLs"), zap.Stringer("payload", req), zap.Error(err))
	}
	return makeCommonWorkerResponse(err), nil
}

// SwitchRelayMaster implements WorkerServer.SwitchRelayMaster
func (s *Server) SwitchRelayMaster(ctx context.Context, req *pb.SwitchRelayMasterRequest) (*pb.CommonWorkerResponse, error) {
	log.L().Info("", zap.String("request", "SwitchRelayMaster"), zap.Stringer("payload", req))
	w := s.checkWorkerStart()
	if w == nil {
		log.L().Error("fail to call StartSubTask, because mysql worker has not been started")
		return makeCommonWorkerResponse(terror.ErrWorkerNoStart.Generate()), nil
	}

	err := w.SwitchRelayMaster(ctx, req)
	if err != nil {
		log.L().Error("fail to switch relay master", zap.String("request", "SwitchRelayMaster"), zap.Stringer("payload", req), zap.Error(err))
	}
	return makeCommonWorkerResponse(err), nil
}

// OperateRelay implements WorkerServer.OperateRelay
func (s *Server) OperateRelay(ctx context.Context, req *pb.OperateRelayRequest) (*pb.OperateRelayResponse, error) {
	log.L().Info("", zap.String("request", "OperateRelay"), zap.Stringer("payload", req))
	resp := &pb.OperateRelayResponse{
		Op:     req.Op,
		Result: false,
	}

	w := s.checkWorkerStart()
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

	resp.Result = true
	return resp, nil
}

// PurgeRelay implements WorkerServer.PurgeRelay
func (s *Server) PurgeRelay(ctx context.Context, req *pb.PurgeRelayRequest) (*pb.CommonWorkerResponse, error) {
	log.L().Info("", zap.String("request", "PurgeRelay"), zap.Stringer("payload", req))
	w := s.checkWorkerStart()
	if w == nil {
		log.L().Error("fail to call StartSubTask, because mysql worker has not been started")
		return makeCommonWorkerResponse(terror.ErrWorkerNoStart.Generate()), nil
	}

	err := w.PurgeRelay(ctx, req)
	if err != nil {
		log.L().Error("fail to purge relay", zap.String("request", "PurgeRelay"), zap.Stringer("payload", req), zap.Error(err))
	}
	return makeCommonWorkerResponse(err), nil
}

// UpdateRelayConfig updates config for relay and (dm-worker)
func (s *Server) UpdateRelayConfig(ctx context.Context, req *pb.UpdateRelayRequest) (*pb.CommonWorkerResponse, error) {
	log.L().Info("", zap.String("request", "UpdateRelayConfig"), zap.Stringer("payload", req))
	w := s.checkWorkerStart()
	if w == nil {
		log.L().Error("fail to call StartSubTask, because mysql worker has not been started")
		return makeCommonWorkerResponse(terror.ErrWorkerNoStart.Generate()), nil
	}

	err := w.UpdateRelayConfig(ctx, req.Content)
	if err != nil {
		log.L().Error("fail to update relay config", zap.String("request", "UpdateRelayConfig"), zap.Stringer("payload", req), zap.Error(err))
	}
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

	w := s.checkWorkerStart()
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
	resp.SourceID = workerCfg.SourceID
	return resp, nil
}

// MigrateRelay migrate relay to original binlog pos
func (s *Server) MigrateRelay(ctx context.Context, req *pb.MigrateRelayRequest) (*pb.CommonWorkerResponse, error) {
	log.L().Info("", zap.String("request", "MigrateRelay"), zap.Stringer("payload", req))
	w := s.checkWorkerStart()
	if w == nil {
		log.L().Error("fail to call StartSubTask, because mysql worker has not been started")
		return makeCommonWorkerResponse(terror.ErrWorkerNoStart.Generate()), nil
	}

	err := w.MigrateRelay(ctx, req.BinlogName, req.BinlogPos)
	if err != nil {
		log.L().Error("fail to migrate relay", zap.String("request", "MigrateRelay"), zap.Stringer("payload", req), zap.Error(err))
	}
	return makeCommonWorkerResponse(err), nil
}

func (s *Server) startWorker(cfg *config.MysqlConfig) error {
	s.Lock()
	if s.worker != nil {
		return terror.ErrWorkerAlreadyClosed.Generate()
	}
	ctx, cancel := context.WithTimeout(s.etcdClient.Ctx(), 3*time.Second)
	defer cancel()
	cfgStr, err := cfg.EncodeToml()
	if err != nil {
	}

	w, err := NewWorker(cfg)
	if err != nil {
		return err
	}

	resp, err := s.etcdClient.Txn(ctx).Then(
		clientv3.OpPut(common.UpstreamConfigKeyAdapter.Encode(cfg.SourceID), cfgStr),
		clientv3.OpPut(common.UpstreamBoundWorkerKeyAdapter.Encode(s.cfg.WorkerAddr), cfg.SourceID),
	).Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return errors.New("failed to bound worker")
	}
	s.worker = w
	s.Unlock()
	go func() {
		s.worker.Start()
	}()
	return nil
}

// OperateMysqlTask create a new mysql task which will be running in this Server
func (s *Server) OperateMysqlTask(ctx context.Context, req *pb.MysqlTaskRequest) (*pb.MysqlTaskResponse, error) {
	resp := &pb.MysqlTaskResponse{
		Result: true,
		Msg:    "Operate mysql task successfully",
	}
	cfg := config.NewWorkerConfig()
	err := cfg.Parse(req.Config)
	if err != nil {
		resp.Result = false
		resp.Msg = errors.ErrorStack(err)
		return resp, nil
	}
	if req.Op == pb.WorkerOp_UpdateConfig || req.Op == pb.WorkerOp_StopWorker {
		s.Lock()
		if s.worker == nil {
			resp.Result = false
			resp.Msg = "Mysql task has not been created, please call CreateMysqlTask"
			return resp, nil
		}
		if cfg.SourceID != s.worker.cfg.SourceID {
			resp.Result = false
			resp.Msg = "stop config has not match the source id of worker, it may be a wrong request"
			return resp, nil
		}
		w := s.worker
		s.worker = nil
		s.Unlock()
		w.Close()
	}
	if req.Op == pb.WorkerOp_UpdateConfig || req.Op == pb.WorkerOp_StartWorker {
		err = s.startWorker(cfg)
	}
	if err != nil {
		resp.Result = false
		resp.Msg = errors.ErrorStack(err)
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

func (s *Server) splitHostPort() (host, port string, err error) {
	// WorkerAddr's format may be "host:port" or ":port"
	host, port, err = net.SplitHostPort(s.cfg.WorkerAddr)
	if err != nil {
		err = terror.ErrWorkerHostPortNotValid.Delegate(err, s.cfg.WorkerAddr)
	}
	return
}
