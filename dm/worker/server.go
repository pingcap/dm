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
	"github.com/pingcap/dm/pkg/etcdutil"
	"github.com/pingcap/dm/pkg/ha"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
	"github.com/pingcap/dm/syncer"

	"github.com/pingcap/errors"
	toolutils "github.com/pingcap/tidb-tools/pkg/utils"
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
	syncMasterEndpointsTime = 3 * time.Second
	getMinLocForSubTaskFunc = getMinLocForSubTask
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
	log.L().Info("starting dm-worker server")
	RegistryMetrics()
	s.ctx, s.cancel = context.WithCancel(context.Background())
	tls, err := toolutils.NewTLS(s.cfg.SSLCA, s.cfg.SSLCert, s.cfg.SSLKey, s.cfg.AdvertiseAddr, s.cfg.CertAllowedCN)
	if err != nil {
		return terror.ErrWorkerTLSConfigNotValid.Delegate(err)
	}

	rootLis, err := net.Listen("tcp", s.cfg.WorkerAddr)
	if err != nil {
		return terror.ErrWorkerStartService.Delegate(err)
	}
	s.rootLis = tls.WrapListener(rootLis)

	s.etcdClient, err = clientv3.New(clientv3.Config{
		Endpoints:            GetJoinURLs(s.cfg.Join),
		DialTimeout:          dialTimeout,
		DialKeepAliveTime:    keepaliveTime,
		DialKeepAliveTimeout: keepaliveTimeout,
		TLS:                  tls.TLSConfig(),
	})
	if err != nil {
		return err
	}

	s.setWorker(nil, true)
	bound, sourceCfg, revBound, err := ha.GetSourceBoundConfig(s.etcdClient, s.cfg.Name)
	if err != nil {
		return err
	}
	if !bound.IsEmpty() {
		log.L().Warn("worker has been assigned source before keepalive", zap.Stringer("bound", bound), zap.Bool("is deleted", bound.IsDeleted))
		err = s.startWorker(&sourceCfg)
		s.setSourceStatus(bound.Source, err, true)
		if err != nil {
			// if DM-worker can't handle pre-assigned source before keepalive, it simply exits with the error,
			// because no re-assigned mechanism exists for keepalived DM-worker yet.
			return err
		}
	}

	s.wg.Add(1)
	go func() {
		s.runBackgroundJob(s.ctx)
		s.wg.Done()
	}()

	s.wg.Add(1)
	go func() {
		s.syncMasterEndpoints(s.ctx)
		s.wg.Done()
	}()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		// TODO: handle fatal error from observeSourceBound
		//nolint:errcheck
		s.observeSourceBound(s.ctx, revBound)
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

	// NOTE: don't need to set tls config, because rootLis already use tls
	s.svr = grpc.NewServer()
	pb.RegisterWorkerServer(s.svr, s)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		err2 := s.svr.Serve(grpcL)
		if err2 != nil && !common.IsErrNetClosing(err2) && err2 != cmux.ErrListenerClosed {
			log.L().Error("gRPC server returned", log.ShortError(err2))
		}
	}()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		InitStatus(httpL) // serve status
	}()

	s.closed.Set(false)
	log.L().Info("listening gRPC API and status request", zap.String("address", s.cfg.WorkerAddr))
	err = m.Serve()
	if err != nil && common.IsErrNetClosing(err) {
		err = nil
	}
	return terror.ErrWorkerStartService.Delegate(err)
}

func (s *Server) syncMasterEndpoints(ctx context.Context) {
	lastClientUrls := []string{}
	clientURLs := []string{}

	updateF := func() {
		clientURLs = clientURLs[:0]
		resp, err := s.etcdClient.MemberList(ctx)
		if err != nil {
			log.L().Error("can't get etcd member list", zap.Error(err))
			return
		}

		for _, m := range resp.Members {
			clientURLs = append(clientURLs, m.GetClientURLs()...)
		}
		if utils.NonRepeatStringsEqual(clientURLs, lastClientUrls) {
			log.L().Debug("etcd member list doesn't change", zap.Strings("client URLs", clientURLs))
			return
		}
		log.L().Info("will sync endpoints to", zap.Strings("client URLs", clientURLs))
		s.etcdClient.SetEndpoints(clientURLs...)
		lastClientUrls = make([]string, len(clientURLs))
		copy(lastClientUrls, clientURLs)
	}

	for {
		updateF()

		select {
		case <-ctx.Done():
			return
		case <-time.After(syncMasterEndpointsTime):
		}
	}
}

func (s *Server) observeSourceBound(ctx context.Context, rev int64) error {
	var wg sync.WaitGroup
	for {
		sourceBoundCh := make(chan ha.SourceBound, 10)
		sourceBoundErrCh := make(chan error, 10)
		wg.Add(1)
		// use ctx1, cancel1 to make sure old watcher has been released
		ctx1, cancel1 := context.WithCancel(ctx)
		go func() {
			defer func() {
				close(sourceBoundCh)
				close(sourceBoundErrCh)
				wg.Done()
			}()
			ha.WatchSourceBound(ctx1, s.etcdClient, s.cfg.Name, rev+1, sourceBoundCh, sourceBoundErrCh)
		}()
		err := s.handleSourceBound(ctx1, sourceBoundCh, sourceBoundErrCh)
		cancel1()
		wg.Wait()

		if etcdutil.IsRetryableError(err) {
			rev = 0
			retryNum := 1
			for rev == 0 {
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(500 * time.Millisecond):
					bound, cfg, rev1, err1 := ha.GetSourceBoundConfig(s.etcdClient, s.cfg.Name)
					if err1 != nil {
						log.L().Error("get source bound from etcd failed, will retry later", zap.Error(err1), zap.Int("retryNum", retryNum))
						break
					}
					rev = rev1
					if bound.IsEmpty() {
						err = s.stopWorker("")
						if err != nil {
							log.L().Error("fail to stop worker", zap.Error(err))
							return err // return if failed to stop the worker.
						}
					} else {
						if w := s.getWorker(true); w != nil && w.cfg.SourceID == bound.Source {
							continue
						}
						err = s.stopWorker("")
						if err != nil {
							log.L().Error("fail to stop worker", zap.Error(err))
							return err // return if failed to stop the worker.
						}
						err1 = s.startWorker(&cfg)
						s.setSourceStatus(bound.Source, err1, true)
						if err1 != nil {
							log.L().Error("fail to operate sourceBound on worker", zap.Stringer("bound", bound), zap.Bool("is deleted", bound.IsDeleted), zap.Error(err1))
						}
					}
				}
				retryNum++
			}
		} else {
			if err != nil {
				log.L().Error("observeSourceBound is failed and will quit now", zap.Error(err))
			} else {
				log.L().Info("observeSourceBound will quit now")
			}
			return err
		}
	}
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
				unit.NewProcessError(err),
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
		log.L().Warn("worker has not been started, no need to stop", zap.String("source", sourceID))
		return nil // no need to stop because not started yet
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

func (s *Server) handleSourceBound(ctx context.Context, boundCh chan ha.SourceBound, errCh chan error) error {
OUTER:
	for {
		select {
		case <-ctx.Done():
			break OUTER
		case bound, ok := <-boundCh:
			if !ok {
				break OUTER
			}
			log.L().Info("receive source bound", zap.Stringer("bound", bound), zap.Bool("is deleted", bound.IsDeleted))
			err := s.operateSourceBound(bound)
			s.setSourceStatus(bound.Source, err, true)
			if err != nil {
				opErrCounter.WithLabelValues(s.cfg.Name, opErrTypeSourceBound).Inc()
				log.L().Error("fail to operate sourceBound on worker", zap.Stringer("bound", bound), zap.Bool("is deleted", bound.IsDeleted), zap.Error(err))
				if etcdutil.IsRetryableError(err) {
					return err
				}
			}
		case err, ok := <-errCh:
			if !ok {
				break OUTER
			}
			// TODO: Deal with err
			log.L().Error("WatchSourceBound received an error", zap.Error(err))
			if etcdutil.IsRetryableError(err) {
				return err
			}
		}
	}
	log.L().Info("worker server is closed, handleSourceBound will quit now")
	return nil
}

func (s *Server) operateSourceBound(bound ha.SourceBound) error {
	if bound.IsDeleted {
		return s.stopWorker(bound.Source)
	}
	scm, _, err := ha.GetSourceCfg(s.etcdClient, bound.Source, bound.Revision)
	if err != nil {
		// TODO: need retry
		return err
	}
	sourceCfg, ok := scm[bound.Source]
	if !ok {
		return terror.ErrWorkerFailToGetSourceConfigFromEtcd.Generate(bound.Source)
	}
	return s.startWorker(&sourceCfg)
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
		log.L().Warn("fail to call QueryStatus, because no mysql source is being handled in the worker")
		resp.Result = false
		resp.Msg = terror.ErrWorkerNoStart.Error()
		return resp, nil
	}

	resp.SubTaskStatus = w.QueryStatus(ctx, req.Name)
	if w.relayHolder != nil {
		sourceStatus.RelayStatus = w.relayHolder.Status(ctx)
	}

	unifyMasterBinlogPos(resp, w.cfg.EnableGTID)

	if len(resp.SubTaskStatus) == 0 {
		resp.Msg = "no sub task started"
	}
	return resp, nil
}

// PurgeRelay implements WorkerServer.PurgeRelay
func (s *Server) PurgeRelay(ctx context.Context, req *pb.PurgeRelayRequest) (*pb.CommonWorkerResponse, error) {
	log.L().Info("", zap.String("request", "PurgeRelay"), zap.Stringer("payload", req))
	w := s.getWorker(true)
	if w == nil {
		log.L().Warn("fail to call StartSubTask, because no mysql source is being handled in the worker")
		return makeCommonWorkerResponse(terror.ErrWorkerNoStart.Generate()), nil
	}

	err := w.PurgeRelay(ctx, req)
	if err != nil {
		log.L().Error("fail to purge relay", zap.String("request", "PurgeRelay"), zap.Stringer("payload", req), zap.Error(err))
	}
	return makeCommonWorkerResponse(err), nil
}

// OperateSchema operates schema for an upstream table.
func (s *Server) OperateSchema(ctx context.Context, req *pb.OperateWorkerSchemaRequest) (*pb.CommonWorkerResponse, error) {
	log.L().Info("", zap.String("request", "OperateSchema"), zap.Stringer("payload", req))

	w := s.getWorker(true)
	if w == nil {
		log.L().Warn("fail to call OperateSchema, because no mysql source is being handled in the worker")
		return makeCommonWorkerResponse(terror.ErrWorkerNoStart.Generate()), nil
	} else if req.Source != w.cfg.SourceID {
		log.L().Error("fail to call OperateSchema, because source mismatch", zap.String("request", req.Source), zap.String("current", w.cfg.SourceID))
		return makeCommonWorkerResponse(terror.ErrWorkerSourceNotMatch.Generate()), nil
	}

	schema, err := w.OperateSchema(ctx, req)
	if err != nil {
		return makeCommonWorkerResponse(err), nil
	}
	return &pb.CommonWorkerResponse{
		Result: true,
		Msg:    schema, // if any schema return for `GET`, we place it in the `msg` field now.
		Source: req.Source,
		Worker: s.cfg.Name,
	}, nil
}

func (s *Server) startWorker(cfg *config.SourceConfig) error {
	s.Lock()
	defer s.Unlock()
	if w := s.getWorker(false); w != nil {
		if w.cfg.SourceID == cfg.SourceID {
			log.L().Info("mysql source is being handled", zap.String("sourceID", s.worker.cfg.SourceID))
			return nil
		}
		return terror.ErrWorkerAlreadyStart.Generate()
	}

	// we get the newest subtask stages directly which will omit the subtask stage PUT/DELETE event
	// because triggering these events is useless now
	subTaskStages, subTaskCfgm, revSubTask, err := ha.GetSubTaskStageConfig(s.etcdClient, cfg.SourceID)
	if err != nil {
		return err
	}

	subTaskCfgs := make([]*config.SubTaskConfig, 0, len(subTaskCfgm))
	for _, subTaskCfg := range subTaskCfgm {
		subTaskCfg.LogLevel = s.cfg.LogLevel
		subTaskCfg.LogFile = s.cfg.LogFile
		subTaskCfg.LogFormat = s.cfg.LogFormat
		subTaskCfgClone := subTaskCfg
		if err = copyConfigFromSource(&subTaskCfgClone, cfg); err != nil {
			return err
		}
		subTaskCfgs = append(subTaskCfgs, &subTaskCfgClone)
	}

	if cfg.EnableRelay {
		dctx, dcancel := context.WithTimeout(s.etcdClient.Ctx(), time.Duration(len(subTaskCfgs))*3*time.Second)
		defer dcancel()
		minLoc, err1 := getMinLocInAllSubTasks(dctx, subTaskCfgs)
		if err1 != nil {
			return err1
		}

		if minLoc != nil {
			log.L().Info("get min location in all subtasks", zap.Stringer("location", *minLoc))
			cfg.RelayBinLogName = binlog.AdjustPosition(minLoc.Position).Name
			cfg.RelayBinlogGTID = minLoc.GTIDSetStr()
			// set UUIDSuffix when bound to a source
			cfg.UUIDSuffix, err = binlog.ExtractSuffix(minLoc.Position.Name)
			if err != nil {
				return err
			}
		} else {
			// set UUIDSuffix even not checkpoint exist
			// so we will still remove relay dir
			cfg.UUIDSuffix = binlog.MinUUIDSuffix
		}
	}

	log.L().Info("starting to handle mysql source", zap.String("sourceCfg", cfg.String()), zap.Reflect("subTasks", subTaskCfgs))
	w, err := NewWorker(cfg, s.etcdClient, s.cfg.Name)
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
		// TODO: PurgeRelayDir if not found relay, not if not found, that's not bound !
	}
	go func() {
		w.Start(startRelay)
	}()

	isStarted := utils.WaitSomething(50, 100*time.Millisecond, func() bool {
		return w.closed.Get() == closedFalse
	})
	if !isStarted {
		// TODO: add more mechanism to wait or un-bound the source
		return terror.ErrWorkerNoStart
	}

	for _, subTaskCfg := range subTaskCfgs {
		expectStage := subTaskStages[subTaskCfg.Name]
		if expectStage.IsDeleted {
			continue
		}
		log.L().Info("start to create subtask", zap.String("sourceID", subTaskCfg.SourceID), zap.String("task", subTaskCfg.Name))
		if err := w.StartSubTask(subTaskCfg, expectStage.Expect); err != nil {
			return err
		}
	}

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		// TODO: handle fatal error from observeSubtaskStage
		//nolint:errcheck
		w.observeSubtaskStage(w.ctx, s.etcdClient, revSubTask)
	}()

	if cfg.EnableRelay {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			// TODO: handle fatal error from observeRelayStage
			//nolint:errcheck
			w.observeRelayStage(w.ctx, s.etcdClient, revRelay)
		}()
	}

	log.L().Info("started to handle mysql source", zap.String("sourceCfg", cfg.String()))
	return nil
}

func makeCommonWorkerResponse(reqErr error) *pb.CommonWorkerResponse {
	resp := &pb.CommonWorkerResponse{
		Result: true,
	}
	if reqErr != nil {
		resp.Result = false
		resp.Msg = reqErr.Error()
	}
	return resp
}

// all subTask in subTaskCfgs should have same source
// this function return the min location in all subtasks, used for relay's location
func getMinLocInAllSubTasks(ctx context.Context, subTaskCfgs []*config.SubTaskConfig) (minLoc *binlog.Location, err error) {
	for _, subTaskCfg := range subTaskCfgs {
		loc, err := getMinLocForSubTaskFunc(ctx, subTaskCfg)
		if err != nil {
			return nil, err
		}

		if loc == nil {
			continue
		}

		if minLoc == nil {
			minLoc = loc
		} else {
			if binlog.CompareLocation(*minLoc, *loc, subTaskCfg.EnableGTID) >= 1 {
				minLoc = loc
			}
		}
	}

	return minLoc, nil
}

func getMinLocForSubTask(ctx context.Context, subTaskCfg *config.SubTaskConfig) (minLoc *binlog.Location, err error) {
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

	err = checkpoint.Load(tctx)
	if err != nil {
		return nil, errors.Annotate(err, "get min position from checkpoint")
	}

	location := checkpoint.GlobalPoint()
	return &location, nil
}

// unifyMasterBinlogPos eliminates different masterBinlog in one response
// see https://github.com/pingcap/dm/issues/727
func unifyMasterBinlogPos(resp *pb.QueryStatusResponse, enableGTID bool) {
	var (
		syncStatus          []*pb.SubTaskStatus_Sync
		syncMasterBinlog    []*mysql.Position
		lastestMasterBinlog mysql.Position // not pointer, to make use of zero value and avoid nil check
		relayMasterBinlog   *mysql.Position
	)

	// uninitialized mysql.Position is less than any initialized mysql.Position
	if resp.SourceStatus.RelayStatus != nil && resp.SourceStatus.RelayStatus.Stage != pb.Stage_Stopped {
		var err error
		relayMasterBinlog, err = utils.DecodeBinlogPosition(resp.SourceStatus.RelayStatus.MasterBinlog)
		if err != nil {
			log.L().Error("failed to decode relay's master binlog position", zap.Stringer("response", resp), zap.Error(err))
			return
		}
		lastestMasterBinlog = *relayMasterBinlog
	}

	for _, stStatus := range resp.SubTaskStatus {
		if stStatus.Unit == pb.UnitType_Sync {
			s := stStatus.Status.(*pb.SubTaskStatus_Sync)
			syncStatus = append(syncStatus, s)

			position, err := utils.DecodeBinlogPosition(s.Sync.MasterBinlog)
			if err != nil {
				log.L().Error("failed to decode sync's master binlog position", zap.Stringer("response", resp), zap.Error(err))
				return
			}
			if lastestMasterBinlog.Compare(*position) < 0 {
				lastestMasterBinlog = *position
			}
			syncMasterBinlog = append(syncMasterBinlog, position)
		}
	}

	// re-check relay
	if resp.SourceStatus.RelayStatus != nil && resp.SourceStatus.RelayStatus.Stage != pb.Stage_Stopped &&
		lastestMasterBinlog.Compare(*relayMasterBinlog) != 0 {

		resp.SourceStatus.RelayStatus.MasterBinlog = lastestMasterBinlog.String()

		// if enableGTID, modify output binlog position doesn't affect RelayCatchUpMaster, skip check
		if !enableGTID {
			relayPos, err := utils.DecodeBinlogPosition(resp.SourceStatus.RelayStatus.RelayBinlog)
			if err != nil {
				log.L().Error("failed to decode relay binlog position", zap.Stringer("response", resp), zap.Error(err))
				return
			}
			catchUp := lastestMasterBinlog.Compare(*relayPos) == 0

			resp.SourceStatus.RelayStatus.RelayCatchUpMaster = catchUp
		}
	}
	// re-check syncer
	for i, sStatus := range syncStatus {
		if lastestMasterBinlog.Compare(*syncMasterBinlog[i]) != 0 {
			syncerPos, err := utils.DecodeBinlogPosition(sStatus.Sync.SyncerBinlog)
			if err != nil {
				log.L().Error("failed to decode syncer binlog position", zap.Stringer("response", resp), zap.Error(err))
				return
			}
			synced := lastestMasterBinlog.Compare(*syncerPos) == 0

			sStatus.Sync.MasterBinlog = lastestMasterBinlog.String()
			sStatus.Sync.Synced = synced
		}
	}
}

// HandleError handle error
func (s *Server) HandleError(ctx context.Context, req *pb.HandleWorkerErrorRequest) (*pb.CommonWorkerResponse, error) {
	log.L().Info("", zap.String("request", "HandleError"), zap.Stringer("payload", req))

	w := s.getWorker(true)
	if w == nil {
		log.L().Warn("fail to call HandleError, because no mysql source is being handled in the worker")
		return makeCommonWorkerResponse(terror.ErrWorkerNoStart.Generate()), nil
	}

	err := w.HandleError(ctx, req)
	if err != nil {
		return makeCommonWorkerResponse(err), nil
	}
	return &pb.CommonWorkerResponse{
		Result: true,
		Worker: s.cfg.Name,
	}, nil
}

// GetWorkerCfg get worker config
func (s *Server) GetWorkerCfg(ctx context.Context, req *pb.GetWorkerCfgRequest) (*pb.GetWorkerCfgResponse, error) {
	log.L().Info("", zap.String("request", "GetWorkerCfg"), zap.Stringer("payload", req))
	var err error
	resp := &pb.GetWorkerCfgResponse{}

	resp.Cfg, err = s.cfg.Toml()
	return resp, err
}
