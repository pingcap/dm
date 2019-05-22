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
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/errors"
	"github.com/siddontang/go/sync2"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"

	"github.com/pingcap/dm/checker"
	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/master/sql-operator"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/tracing"
)

var (
	retryTimeout       = 5 * time.Second
	tryResolveInterval = 30 * time.Second
	cmuxReadTimeout    = 10 * time.Second
)

// Server handles RPC requests for dm-master
type Server struct {
	sync.Mutex

	cfg *Config

	rootLis net.Listener
	svr     *grpc.Server

	// dm-worker-ID(host:ip) -> dm-worker-client
	workerClients map[string]pb.WorkerClient

	// task-name -> worker-list
	taskWorkers map[string][]string

	// DDL lock keeper
	lockKeeper *LockKeeper

	// SQL operator holder
	sqlOperatorHolder *operator.Holder

	// trace group id generator
	idGen *tracing.IDGenerator

	closed sync2.AtomicBool
}

// NewServer creates a new Server
func NewServer(cfg *Config) *Server {
	server := Server{
		cfg:               cfg,
		workerClients:     make(map[string]pb.WorkerClient),
		taskWorkers:       make(map[string][]string),
		lockKeeper:        NewLockKeeper(),
		sqlOperatorHolder: operator.NewHolder(),
		idGen:             tracing.NewIDGen(),
	}
	return &server
}

// Start starts to serving
func (s *Server) Start() error {
	var err error
	s.rootLis, err = net.Listen("tcp", s.cfg.MasterAddr)
	if err != nil {
		return errors.Trace(err)
	}

	for _, workerAddr := range s.cfg.DeployMap {
		conn, err2 := grpc.Dial(workerAddr, grpc.WithInsecure(), grpc.WithBackoffMaxDelay(3*time.Second))
		if err2 != nil {
			return errors.Trace(err2)
		}
		s.workerClients[workerAddr] = pb.NewWorkerClient(conn)
	}
	s.closed.Set(false)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
			return
		case <-time.After(3 * time.Second):
			// update task -> workers after started
			s.updateTaskWorkers(ctx)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		// fetch DDL info from dm-workers to sync sharding DDL
		s.fetchWorkerDDLInfo(ctx)
	}()

	// auto resolve DDL lock is not very useful, comment it
	//wg.Add(1)
	//go func() {
	//	defer wg.Done()
	//	timer := time.NewTicker(tryResolveInterval)
	//	defer timer.Stop()
	//	for {
	//		select {
	//		case <-ctx.Done():
	//			return
	//		case <-timer.C:
	//			s.tryResolveDDLLocks(ctx)
	//		}
	//	}
	//}()

	// create a cmux
	m := cmux.New(s.rootLis)
	m.SetReadTimeout(cmuxReadTimeout) // set a timeout, ref: https://github.com/pingcap/tidb-binlog/pull/352

	// match connections in order: first gRPC, then HTTP
	grpcL := m.Match(cmux.HTTP2HeaderField("content-type", "application/grpc"))
	httpL := m.Match(cmux.HTTP1Fast())

	s.svr = grpc.NewServer()
	pb.RegisterMasterServer(s.svr, s)
	go func() {
		err2 := s.svr.Serve(grpcL)
		if err2 != nil && !common.IsErrNetClosing(err2) && err2 != cmux.ErrListenerClosed {
			log.Errorf("[server] gRPC server return with error %s", err2.Error())
		}
	}()
	go InitStatus(httpL) // serve status

	log.Infof("[server] listening on %v for gRPC API and status request", s.cfg.MasterAddr)
	err = m.Serve() // start serving, block
	if err != nil && common.IsErrNetClosing(err) {
		err = nil
	}

	cancel()
	wg.Wait()
	return err
}

// Close close the RPC server
func (s *Server) Close() {
	s.Lock()
	defer s.Unlock()
	if s.closed.Get() {
		return
	}
	err := s.rootLis.Close()
	if err != nil && !common.IsErrNetClosing(err) {
		log.Errorf("[server] close net listener with error %s", err.Error())
	}
	if s.svr != nil {
		s.svr.GracefulStop()
	}
	s.closed.Set(true)
}

// StartTask implements MasterServer.StartTask
func (s *Server) StartTask(ctx context.Context, req *pb.StartTaskRequest) (*pb.StartTaskResponse, error) {
	log.Infof("[server] receive StartTask request %+v", req)

	cfg, stCfgs, err := s.generateSubTask(ctx, req.Task)
	if err != nil {
		return &pb.StartTaskResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}, nil
	}
	log.Infof("[server] starting task with config:\n%v", cfg)

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
				workerRespCh <- &pb.CommonWorkerResponse{
					Result: false,
					Worker: worker,
					Msg:    "worker not found in task's config or deployment config",
				}
			}
		}
	}

	validWorkerCh := make(chan string, len(stCfgs))
	var wg sync.WaitGroup
	for _, stCfg := range stCfgs {
		wg.Add(1)
		go func(stCfg *config.SubTaskConfig) {
			defer wg.Done()
			worker, ok1 := s.cfg.DeployMap[stCfg.SourceID]
			cli, ok2 := s.workerClients[worker]
			if !ok1 || !ok2 {
				workerRespCh <- &pb.CommonWorkerResponse{
					Result: false,
					Msg:    fmt.Sprintf("%s relevant worker not found", stCfg.SourceID),
				}
				return
			}
			validWorkerCh <- worker
			stCfgToml, err := stCfg.Toml() // convert to TOML format
			if err != nil {
				workerRespCh <- &pb.CommonWorkerResponse{
					Result: false,
					Worker: worker,
					Msg:    errors.ErrorStack(err),
				}
				return
			}
			workerResp, err := cli.StartSubTask(ctx, &pb.StartSubTaskRequest{Task: stCfgToml})
			workerResp = s.handleOperationResult(ctx, cli, stCfg.Name, err, workerResp)
			workerResp.Meta.Worker = worker
			workerRespCh <- workerResp.Meta
		}(stCfg)
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
	log.Infof("[server] receive OperateTask request %+v", req)

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

	subReq := &pb.OperateSubTaskRequest{
		Op:   req.Op,
		Name: req.Name,
	}
	workerRespCh := make(chan *pb.OperateSubTaskResponse, len(workers))
	var wg sync.WaitGroup
	for _, worker := range workers {
		wg.Add(1)
		go func(worker string) {
			defer wg.Done()
			cli, ok := s.workerClients[worker]
			if !ok {
				workerResp := &pb.OperateSubTaskResponse{
					Meta: &pb.CommonWorkerResponse{
						Result: false,
						Worker: worker,
						Msg:    fmt.Sprintf("%s relevant worker-client not found", worker),
					},
					Op: req.Op,
				}
				workerRespCh <- workerResp
				return
			}
			workerResp, err := cli.OperateSubTask(ctx, subReq)
			workerResp = s.handleOperationResult(ctx, cli, req.Name, err, workerResp)
			workerResp.Op = req.Op
			workerResp.Meta.Worker = worker
			workerRespCh <- workerResp
		}(worker)
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
	log.Infof("[server] receive UpdateTask request %+v", req)

	cfg, stCfgs, err := s.generateSubTask(ctx, req.Task)
	if err != nil {
		return &pb.UpdateTaskResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}, nil
	}
	log.Infof("[server] update task with config:\n%v", cfg)

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
				workerRespCh <- &pb.CommonWorkerResponse{
					Result: false,
					Worker: worker,
					Msg:    "worker not found in task's config or deployment config",
				}
			}
		}
	}

	var wg sync.WaitGroup
	for _, stCfg := range stCfgs {
		wg.Add(1)
		go func(stCfg *config.SubTaskConfig) {
			defer wg.Done()
			worker, ok1 := s.cfg.DeployMap[stCfg.SourceID]
			cli, ok2 := s.workerClients[worker]
			if !ok1 || !ok2 {
				workerRespCh <- &pb.CommonWorkerResponse{
					Result: false,
					Msg:    fmt.Sprintf("%s relevant worker not found", stCfg.SourceID),
				}
				return
			}
			stCfgToml, err := stCfg.Toml() // convert to TOML format
			if err != nil {
				workerRespCh <- &pb.CommonWorkerResponse{
					Result: false,
					Worker: worker,
					Msg:    errors.ErrorStack(err),
				}
				return
			}
			workerResp, err := cli.UpdateSubTask(ctx, &pb.UpdateSubTaskRequest{Task: stCfgToml})
			workerResp = s.handleOperationResult(ctx, cli, stCfg.Name, err, workerResp)
			workerResp.Meta.Worker = worker
			workerRespCh <- workerResp.Meta
		}(stCfg)
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
	log.Infof("[server] receive QueryStatus request %+v", req)

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
	log.Infof("[server] receive QueryError request %+v", req)

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
	log.Infof("[server] receive ShowDDLLocks request %+v", req)

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
	log.Infof("[server] receive UnlockDDLLock request %+v", req)

	workerResps, err := s.resolveDDLLock(ctx, req.ID, req.ReplaceOwner, req.Workers)
	resp := &pb.UnlockDDLLockResponse{
		Result:  true,
		Workers: workerResps,
	}
	if err != nil {
		resp.Result = false
		resp.Msg = errors.ErrorStack(err)
		log.Errorf("[sever] UnlockDDLLock %s error %v", req.ID, errors.ErrorStack(err))

		if req.ForceRemove {
			s.lockKeeper.RemoveLock(req.ID)
			log.Warnf("[server] force to remove DDL lock %s because of `ForceRemove` set", req.ID)
		}
	} else {
		log.Infof("[server] UnlockDDLLock %s successfully, remove it", req.ID)
	}

	return resp, nil
}

// BreakWorkerDDLLock implements MasterServer.BreakWorkerDDLLock
func (s *Server) BreakWorkerDDLLock(ctx context.Context, req *pb.BreakWorkerDDLLockRequest) (*pb.BreakWorkerDDLLockResponse, error) {
	log.Infof("[server] receive BreakWorkerDDLLock request %+v", req)

	workerReq := &pb.BreakDDLLockRequest{
		Task:         req.Task,
		RemoveLockID: req.RemoveLockID,
		ExecDDL:      req.ExecDDL,
		SkipDDL:      req.SkipDDL,
	}

	workerRespCh := make(chan *pb.CommonWorkerResponse, len(req.Workers))
	var wg sync.WaitGroup
	for _, worker := range req.Workers {
		wg.Add(1)
		go func(worker string) {
			defer wg.Done()
			cli, ok := s.workerClients[worker]
			if !ok {
				workerRespCh <- &pb.CommonWorkerResponse{
					Result: false,
					Worker: worker,
					Msg:    fmt.Sprintf("worker %s relevant worker-client not found", worker),
				}
				return
			}
			workerResp, err := cli.BreakDDLLock(ctx, workerReq)
			if err != nil {
				workerResp = &pb.CommonWorkerResponse{
					Result: false,
					Msg:    errors.ErrorStack(err),
				}
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
	log.Infof("[server] receive HandleSQLs request %+v", req)

	// save request for --sharding operation
	if req.Sharding {
		err := s.sqlOperatorHolder.Set(req)
		if err != nil {
			return &pb.HandleSQLsResponse{
				Result: false,
				Msg:    fmt.Sprintf("save request with --sharding error:\n%s", errors.ErrorStack(err)),
			}, nil
		}
		log.Infof("[server] saved handle --sharding SQLs request %v", req)
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
	subReq := &pb.HandleSubTaskSQLsRequest{
		Name:       req.Name,
		Op:         req.Op,
		Args:       req.Args,
		BinlogPos:  req.BinlogPos,
		SqlPattern: req.SqlPattern,
	}
	cli, ok := s.workerClients[req.Worker]
	if !ok {
		resp.Msg = fmt.Sprintf("worker %s client not found in %v", req.Worker, s.workerClients)
		return resp, nil
	}
	workerResp, err := cli.HandleSQLs(ctx, subReq)
	if err != nil {
		workerResp = &pb.CommonWorkerResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}
	}

	resp.Workers = []*pb.CommonWorkerResponse{workerResp}
	resp.Result = true
	return resp, nil
}

// PurgeWorkerRelay implements MasterServer.PurgeWorkerRelay
func (s *Server) PurgeWorkerRelay(ctx context.Context, req *pb.PurgeWorkerRelayRequest) (*pb.PurgeWorkerRelayResponse, error) {
	log.Infof("[server] receive PurgeWorkerRelay request %+v", req)

	workerReq := &pb.PurgeRelayRequest{
		Inactive: req.Inactive,
		Time:     req.Time,
		Filename: req.Filename,
		SubDir:   req.SubDir,
	}

	workerRespCh := make(chan *pb.CommonWorkerResponse, len(req.Workers))
	var wg sync.WaitGroup
	for _, worker := range req.Workers {
		wg.Add(1)
		go func(worker string) {
			defer wg.Done()
			cli, ok := s.workerClients[worker]
			if !ok {
				workerRespCh <- &pb.CommonWorkerResponse{
					Result: false,
					Worker: worker,
					Msg:    fmt.Sprintf("worker %s relevant worker-client not found", worker),
				}
				return
			}
			workerResp, err := cli.PurgeRelay(ctx, workerReq)
			if err != nil {
				workerResp = &pb.CommonWorkerResponse{
					Result: false,
					Msg:    errors.ErrorStack(err),
				}
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
	log.Infof("[server] receive SwitchWorkerRelayMaster request %+v", req)

	workerReq := &pb.SwitchRelayMasterRequest{}

	workerRespCh := make(chan *pb.CommonWorkerResponse, len(req.Workers))
	var wg sync.WaitGroup
	for _, worker := range req.Workers {
		wg.Add(1)
		go func(worker string) {
			defer wg.Done()
			cli, ok := s.workerClients[worker]
			if !ok {
				workerRespCh <- &pb.CommonWorkerResponse{
					Result: false,
					Worker: worker,
					Msg:    fmt.Sprintf("worker %s relevant worker-client not found", worker),
				}
				return
			}
			workerResp, err := cli.SwitchRelayMaster(ctx, workerReq)
			if err != nil {
				workerResp = &pb.CommonWorkerResponse{
					Result: false,
					Msg:    errors.ErrorStack(err),
				}
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

	return &pb.SwitchWorkerRelayMasterResponse{
		Result:  true,
		Workers: workerResps,
	}, nil
}

// OperateWorkerRelayTask implements MasterServer.OperateWorkerRelayTask
func (s *Server) OperateWorkerRelayTask(ctx context.Context, req *pb.OperateWorkerRelayRequest) (*pb.OperateWorkerRelayResponse, error) {
	log.Infof("[server] receive OperateWorkerRelayTask request %+v", req)

	workerReq := &pb.OperateRelayRequest{Op: req.Op}

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
			workerResp, err := cli.OperateRelay(ctx, workerReq)
			if err != nil {
				workerResp = &pb.OperateRelayResponse{
					Result: false,
					Msg:    errors.ErrorStack(err),
				}
			}
			workerReq.Op = req.Op
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
	log.Infof("[server] receive RefreshWorkerTasks request %+v", req)

	taskWorkers, workerMsgMap := s.fetchTaskWorkers(ctx)
	if len(taskWorkers) > 0 {
		s.replaceTaskWorkers(taskWorkers)
	}
	log.Infof("[server] update task workers to %v", taskWorkers)

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
	log.Infof("[server] update task %s workers to %v", task, valid)
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
		log.Warnf("[server] %s has no workers", task)
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
		log.Infof("[server] remove task %s workers", task)
	} else {
		s.taskWorkers[task] = remain
		log.Infof("[server] update task %s workers to %v", task, remain)
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
	workerReq := &pb.QueryStatusRequest{
		Name: taskName,
	}

	workerRespCh := make(chan *pb.QueryStatusResponse, len(workers))
	var wg sync.WaitGroup
	for _, worker := range workers {
		wg.Add(1)
		go func(worker string) {
			defer wg.Done()
			cli := s.workerClients[worker]
			workerStatus, err := cli.QueryStatus(ctx, workerReq)
			if err != nil {
				workerStatus = &pb.QueryStatusResponse{
					Result: false,
					Msg:    errors.ErrorStack(err),
				}
			}
			workerStatus.Worker = worker
			workerRespCh <- workerStatus
		}(worker)
	}
	wg.Wait()
	return workerRespCh
}

// getErrorFromWorkers does RPC request to get error information from dm-workers
func (s *Server) getErrorFromWorkers(ctx context.Context, workers []string, taskName string) chan *pb.QueryErrorResponse {
	workerReq := &pb.QueryErrorRequest{
		Name: taskName,
	}

	workerRespCh := make(chan *pb.QueryErrorResponse, len(workers))
	var wg sync.WaitGroup
	for _, worker := range workers {
		wg.Add(1)
		go func(worker string) {
			defer wg.Done()
			cli := s.workerClients[worker]
			workerError, err := cli.QueryError(ctx, workerReq)
			if err != nil {
				workerError = &pb.QueryErrorResponse{
					Result: false,
					Msg:    errors.ErrorStack(err),
				}
			}
			workerError.Worker = worker
			workerRespCh <- workerError
		}(worker)
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
	log.Infof("[server] update task workers to %v", taskWorkers)
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

	for worker, cli := range s.workerClients {
		wg.Add(1)
		go func(worker string, cli pb.WorkerClient) {
			defer wg.Done()
			var doRetry bool

			for {
				if doRetry {
					select {
					case <-ctx.Done():
						return
					case <-time.After(retryTimeout):
					}
				}
				doRetry = false // reset

				select {
				case <-ctx.Done():
					return
				default:
					stream, err := cli.FetchDDLInfo(ctx)
					if err != nil {
						log.Errorf("[server] create FetchDDLInfo stream for worker %s fail %v", worker, err)
						doRetry = true
						continue
					}
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
							log.Errorf("[server] receive DDLInfo from worker %s fail %v", worker, err)
							doRetry = true
							break
						}
						log.Infof("[server] receive DDLInfo %v from worker %s", in, worker)

						workers := s.getTaskWorkers(in.Task)
						if len(workers) == 0 {
							// should happen only when starting and before updateTaskWorkers return
							log.Errorf("[server] try to sync sharding DDL for task %s, but with no workers", in.Task)
							doRetry = true
							break
						}
						if !s.containWorker(workers, worker) {
							// should not happen
							log.Errorf("[server] try to sync sharding DDL for task %s, but worker %s not in workers %v", in.Task, worker, workers)
							doRetry = true
							break
						}

						lockID, synced, remain, err := s.lockKeeper.TrySync(in.Task, in.Schema, in.Table, worker, in.DDLs, workers)
						if err != nil {
							log.Errorf("[server] try to sync lock for worker %s fail %v", worker, err)
							doRetry = true
							break
						}

						out := &pb.DDLLockInfo{
							Task: in.Task,
							ID:   lockID,
						}
						err = stream.Send(out)
						if err != nil {
							log.Errorf("[server] send DDLLockInfo %v to worker %s fail %v", out, worker, err)
							doRetry = true
							break
						}

						if !synced {
							// still need wait other workers to sync
							log.Infof("[server] sharding DDL %s in syncing, waiting %v workers to sync", lockID, remain)
							continue
						}

						log.Infof("[server] sharding DDL %s synced", lockID)

						// resolve DDL lock
						wg.Add(1)
						go func(lockID string) {
							defer wg.Done()
							resps, err := s.resolveDDLLock(ctx, lockID, "", nil)
							if err == nil {
								log.Infof("[server] resolve DDL lock %s successfully, remove it", lockID)
							} else {
								log.Errorf("[server] resolve DDL lock %s fail %v, responses is:\n%+v", lockID, errors.ErrorStack(err), resps)
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

		}(worker, cli)
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
		return nil, errors.NotFoundf("lock with ID %s", lockID)
	}

	if lock.Resolving.Get() {
		return nil, errors.Errorf("lock %s is resolving", lockID)
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
		return nil, errors.NotFoundf("worker %s relevant worker-client", owner)
	}
	if _, ok := ready[owner]; !ok {
		return nil, errors.Errorf("worker %s not waiting for DDL lock %s", owner, lockID)
	}

	// try send handle SQLs request to owner if exists
	key, oper := s.sqlOperatorHolder.Get(lock.Task, lock.DDLs())
	if oper != nil {
		ownerReq := &pb.HandleSubTaskSQLsRequest{
			Name:       oper.Req.Name,
			Op:         oper.Req.Op,
			Args:       oper.Req.Args,
			BinlogPos:  oper.Req.BinlogPos,
			SqlPattern: oper.Req.SqlPattern,
		}
		ownerResp, err := cli.HandleSQLs(ctx, ownerReq)
		if err != nil {
			return nil, errors.Annotatef(err, "send handle SQLs request %s to DDL lock %s owner %s fail", ownerReq, lockID, owner)
		} else if !ownerResp.Result {
			return nil, errors.Errorf("request DDL lock %s owner %s handle SQLs request %s fail %s", lockID, owner, ownerReq, ownerResp.Msg)
		}
		log.Infof("[server] sent handle --sharding DDL request %s to owner %s for lock %s", ownerReq, owner, lockID)
		s.sqlOperatorHolder.Remove(lock.Task, key) // remove SQL operator after sent to owner
	}

	// If send ExecuteDDL request failed, we will generate more tracer group id,
	// this is acceptable if each ExecuteDDL request successes at last.
	// TODO: we need a better way to combine brain split tracing events into one
	// single group.
	traceGID := s.idGen.NextID("resolveDDLLock", 0)
	log.Infof("[server] requesting %s to execute DDL (with ID %s)", owner, lockID)
	ownerResp, err := cli.ExecuteDDL(ctx, &pb.ExecDDLRequest{
		Task:     lock.Task,
		LockID:   lockID,
		Exec:     true,
		TraceGID: traceGID,
	})
	if err != nil {
		ownerResp = &pb.CommonWorkerResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}
	}
	ownerResp.Worker = owner
	if !ownerResp.Result {
		// owner execute DDL fail, do not continue
		return []*pb.CommonWorkerResponse{
			ownerResp,
		}, errors.Errorf("owner %s ExecuteDDL fail", owner)
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

	req := &pb.ExecDDLRequest{
		Task:     lock.Task,
		LockID:   lockID,
		Exec:     false, // ignore and skip DDL
		TraceGID: traceGID,
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
				workerRespCh <- &pb.CommonWorkerResponse{
					Result: false,
					Worker: worker,
					Msg:    fmt.Sprintf("worker %s relevant worker-client not found", worker),
				}
				return
			}
			if _, ok := ready[worker]; !ok {
				workerRespCh <- &pb.CommonWorkerResponse{
					Result: false,
					Worker: worker,
					Msg:    fmt.Sprintf("worker %s not waiting for DDL lock %s", owner, lockID),
				}
				return
			}

			log.Infof("[server] requesting %s to skip DDL (with ID %s)", worker, lockID)
			workerResp, err2 := cli.ExecuteDDL(ctx, req)
			if err2 != nil {
				workerResp = &pb.CommonWorkerResponse{
					Result: false,
					Msg:    errors.ErrorStack(err2),
				}
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
		err = errors.Errorf("DDL lock %s owner ExecuteDDL successfully, so DDL lock removed. but some dm-workers ExecuteDDL fail, you should to handle dm-worker directly", lockID)
	}
	return workerResps, err
}

// UpdateMasterConfig implements MasterServer.UpdateConfig
func (s *Server) UpdateMasterConfig(ctx context.Context, req *pb.UpdateMasterConfigRequest) (*pb.UpdateMasterConfigResponse, error) {
	log.Infof("[server] receive UpdateMasterConfig request %+v", req)
	s.Lock()

	err := s.cfg.UpdateConfigFile(req.Config)
	if err != nil {
		s.Unlock()
		return &pb.UpdateMasterConfigResponse{
			Result: false,
			Msg:    "Failed to write config to local file. detail: " + errors.ErrorStack(err),
		}, nil
	}
	log.Infof("[server] saving dm-master config file to local file %s", s.cfg.ConfigFile)

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
	log.Infof("[server] updating dm-master config file with config:\n%+v", cfg)

	// delete worker
	wokerList := make([]string, 0, len(s.cfg.DeployMap))
	for k, workerAddr := range s.cfg.DeployMap {
		if _, ok := cfg.DeployMap[k]; !ok {
			wokerList = append(wokerList, workerAddr)
			DDLreq := &pb.ShowDDLLocksRequest{
				Task:    "",
				Workers: wokerList,
			}
			resp, err := s.ShowDDLLocks(ctx, DDLreq)
			if err != nil {
				s.Unlock()
				return &pb.UpdateMasterConfigResponse{
					Result: false,
					Msg:    fmt.Sprintf("Failed to get DDL lock Info from %s, detail: ", workerAddr) + errors.ErrorStack(err),
				}, nil
			}
			if len(resp.Locks) != 0 {
				err = errors.Errorf("worker %s exist ddl lock, please unlock ddl lock first!", workerAddr)
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
			conn, err2 := grpc.Dial(workerAddr, grpc.WithInsecure(), grpc.WithBackoffMaxDelay(3*time.Second))
			if err2 != nil {
				s.Unlock()
				return &pb.UpdateMasterConfigResponse{
					Result: false,
					Msg:    fmt.Sprintf("Failed to add woker %s, detail: ", workerAddr) + errors.ErrorStack(err2),
				}, nil
			}
			s.workerClients[workerAddr] = pb.NewWorkerClient(conn)
		}
	}

	// update log configure
	log.SetLevelByString(strings.ToLower(cfg.LogLevel))
	if len(cfg.LogFile) > 0 {
		log.SetOutputByName(cfg.LogFile)
	}

	s.cfg = cfg
	log.Infof("[server] update dm-master config file success")
	s.Unlock()

	workers := make([]string, 0, len(s.workerClients))
	for worker := range s.workerClients {
		workers = append(workers, worker)
	}

	workerRespCh := s.getStatusFromWorkers(ctx, workers, "")
	log.Infof("[server] checking every dm-worker status...")

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

// tryResolveDDLLocks tries to resolve synced DDL locks
// only when auto-triggered resolve by fetchWorkerDDLInfo fail, we need to auto-retry
// this can only handle a few cases, like owner unreachable temporary
// other cases need to handle by user to use dmctl manually
func (s *Server) tryResolveDDLLocks(ctx context.Context) {
	locks := s.lockKeeper.Locks()
	for ID, lock := range locks {
		isSynced, _ := lock.IsSync()
		if !isSynced || !lock.AutoRetry.Get() {
			continue
		}
		log.Infof("[server] try auto re-resolve DDL lock %s", ID)
		s.resolveDDLLock(ctx, ID, "", nil)
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

// UpdateWorkerRelayConfig updates config for relay and (dm-worker)
func (s *Server) UpdateWorkerRelayConfig(ctx context.Context, req *pb.UpdateWorkerRelayConfigRequest) (*pb.CommonWorkerResponse, error) {
	worker := req.Worker
	content := req.Config
	cli, ok := s.workerClients[worker]
	if !ok {
		return &pb.CommonWorkerResponse{
			Result: false,
			Worker: worker,
			Msg:    fmt.Sprintf("worker %s relevant worker-client not found", worker),
		}, nil
	}
	log.Infof("[server] try to update %s relay configure", worker)
	workerResp, err := cli.UpdateRelayConfig(ctx, &pb.UpdateRelayRequest{Content: content})
	if err != nil {
		return &pb.CommonWorkerResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}, nil
	}
	return workerResp, nil
}

func (s *Server) allWorkerConfigs(ctx context.Context) (map[string]config.DBConfig, error) {
	var (
		wg          sync.WaitGroup
		workerMutex sync.Mutex
		workerCfgs  = make(map[string]config.DBConfig)
		errCh       = make(chan error, len(s.workerClients))
		err         error
	)
	handErr := func(err2 error) {
		if err2 != nil {
			log.Error(err2)
		}
		errCh <- errors.Trace(err2)
	}

	for id, worker := range s.workerClients {
		wg.Add(1)
		go Emit(func(args ...interface{}) {
			defer wg.Done()
			if len(args) != 2 {
				handErr(errors.Errorf("fail to call emit to fetch worker config, miss some arguments %v", args))
				return
			}

			id1, ok := args[0].(string)
			if !ok {
				handErr(errors.Errorf("fail to call emit to fetch worker config, can't get id from args[0], arguments %v", args))
				return
			}

			worker1, ok := args[1].(pb.WorkerClient)
			if !ok {
				handErr(errors.Errorf("fail to call emit to fetch config of worker %s, can't get worker client from args[1], arguments %v", id1, args))
				return
			}

			resp, err1 := worker1.QueryWorkerConfig(ctx, &pb.QueryWorkerConfigRequest{})
			if err1 != nil {
				handErr(errors.Annotatef(err1, "fetch config of worker %s", id1))
				return
			}

			if !resp.Result {
				handErr(errors.Errorf("fail to query config from worker %s, message %s", id1, resp.Msg))
				return
			}

			if len(resp.Content) == 0 {
				handErr(errors.Errorf("fail to query config from worker %s, config is empty", id1))
				return
			}

			if len(resp.SourceID) == 0 {
				handErr(errors.Errorf("fail to query config from worker %s, source ID is empty, it should be set in worker config", id1))
				return
			}

			dbCfg := &config.DBConfig{}
			err2 := dbCfg.Decode(resp.Content)
			if err2 != nil {
				handErr(errors.Annotatef(err2, "unmarshal worker %s config", id1))
				return
			}

			workerMutex.Lock()
			workerCfgs[resp.SourceID] = *dbCfg
			workerMutex.Unlock()

		}, []interface{}{id, worker}...)
	}

	wg.Wait()

	if len(errCh) > 0 {
		err = <-errCh
	}

	return workerCfgs, errors.Trace(err)
}

// MigrateWorkerRelay migrates dm-woker relay unit
func (s *Server) MigrateWorkerRelay(ctx context.Context, req *pb.MigrateWorkerRelayRequest) (*pb.CommonWorkerResponse, error) {
	worker := req.Worker
	binlogPos := req.BinlogPos
	binlogName := req.BinlogName
	cli, ok := s.workerClients[worker]
	if !ok {
		return &pb.CommonWorkerResponse{
			Result: false,
			Worker: worker,
			Msg:    fmt.Sprintf("worker %s relevant worker-client not found", worker),
		}, nil
	}
	log.Infof("[server] try to migrate %s relay", worker)
	workerResp, err := cli.MigrateRelay(ctx, &pb.MigrateRelayRequest{BinlogName: binlogName, BinlogPos: binlogPos})
	if err != nil {
		return &pb.CommonWorkerResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}, nil
	}
	return workerResp, nil
}

// CheckTask checks legality of task configuration
func (s *Server) CheckTask(ctx context.Context, req *pb.CheckTaskRequest) (*pb.CheckTaskResponse, error) {
	log.Infof("[server] check task request %+v", req)

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
		return nil, nil, errors.Trace(err)
	}

	sourceCfgs, err := s.allWorkerConfigs(ctx)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	stCfgs, err := cfg.SubTaskConfigs(sourceCfgs)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	err = checker.CheckSyncConfig(ctx, stCfgs)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	return cfg, stCfgs, nil
}

var (
	maxRetryNum   = 30
	retryInterval = time.Second
)

func (s *Server) waitOperationOk(ctx context.Context, cli pb.WorkerClient, name string, opLogID int64) error {
	request := &pb.QueryTaskOperationRequest{
		Name:  name,
		LogID: opLogID,
	}

	for num := 0; num < maxRetryNum; num++ {
		res, err := cli.QueryTaskOperation(ctx, request)
		if err != nil {
			log.Errorf("fail to query task operation %v", err)
		} else if res.Log.Success {
			return nil
		} else if len(res.Log.Message) != 0 {
			return errors.New(res.Log.Message)
		}

		log.Infof("wait task %s op log %d, current result %+v", name, opLogID, res)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryInterval):
		}

	}

	return errors.New("request is timeout, but request may be successful")
}

func (s *Server) handleOperationResult(ctx context.Context, cli pb.WorkerClient, name string, err error, response *pb.OperateSubTaskResponse) *pb.OperateSubTaskResponse {
	if err != nil {
		return &pb.OperateSubTaskResponse{
			Meta: &pb.CommonWorkerResponse{
				Result: false,
				Msg:    errors.ErrorStack(err),
			},
		}
	}

	err = s.waitOperationOk(ctx, cli, name, response.LogID)
	if err != nil {
		response.Meta = &pb.CommonWorkerResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}
	}

	return response
}
