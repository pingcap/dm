// Copyright 2018 PingCAP, Inc.
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
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-enterprise-tools/dm/config"
	"github.com/pingcap/tidb-enterprise-tools/dm/pb"
	"github.com/siddontang/go/sync2"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// Server handles RPC requests for dm-master
type Server struct {
	sync.Mutex

	cfg *Config

	svr *grpc.Server

	// dm-worker-ID(host:ip) -> dm-worker-client
	workerClients map[string]pb.WorkerClient

	// task-name -> worker-list
	taskWorkers map[string][]string

	closed sync2.AtomicBool
}

// NewServer creates a new Server
func NewServer(cfg *Config) *Server {
	server := Server{
		cfg:           cfg,
		workerClients: make(map[string]pb.WorkerClient),
		taskWorkers:   make(map[string][]string),
	}
	return &server
}

// Start starts to serving
func (s *Server) Start() error {
	lis, err := net.Listen("tcp", s.cfg.MasterAddr)
	if err != nil {
		return errors.Trace(err)
	}

	for _, workerAddr := range s.cfg.DeployMap {
		conn, err := grpc.Dial(workerAddr, grpc.WithInsecure(), grpc.WithBackoffMaxDelay(3*time.Second))
		if err != nil {
			return errors.Trace(err)
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

	s.svr = grpc.NewServer()
	pb.RegisterMasterServer(s.svr, s)
	log.Infof("[server] listening on %v for API request", s.cfg.MasterAddr)
	err = s.svr.Serve(lis) // start serving, block
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
	if s.svr != nil {
		s.svr.GracefulStop()
	}
	s.closed.Set(true)
}

// StartTask implements MasterServer.StartTask
func (s *Server) StartTask(ctx context.Context, req *pb.StartTaskRequest) (*pb.StartTaskResponse, error) {
	log.Infof("[server] receive StartTask request %+v", req)

	cfg := config.NewTaskConfig()
	err := cfg.Decode(req.Task)
	if err != nil {
		return &pb.StartTaskResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}, nil
	}
	log.Infof("[server] starting task with config:\n%v", cfg)

	stCfgs := cfg.SubTaskConfigs()
	workerRespCh := make(chan *pb.CommonWorkerResponse, len(stCfgs)+len(req.Workers))
	if len(req.Workers) > 0 {
		// specify only start task on partial dm-workers
		workerCfg := make(map[string]*config.SubTaskConfig)
		for _, stCfg := range stCfgs {
			worker, ok := s.cfg.DeployMap[stCfg.MySQLInstanceID()]
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
			worker, ok1 := s.cfg.DeployMap[stCfg.MySQLInstanceID()]
			cli, ok2 := s.workerClients[worker]
			if !ok1 || !ok2 {
				workerRespCh <- &pb.CommonWorkerResponse{
					Result: false,
					Worker: fmt.Sprintf("MySQL-Instance:%s", stCfg.MySQLInstanceID()),
					Msg:    fmt.Sprintf("%s relevant worker not found", stCfg.MySQLInstanceID()),
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
			if err != nil {
				workerResp = &pb.CommonWorkerResponse{
					Result: false,
					Msg:    errors.ErrorStack(err),
				}
			}
			workerResp.Worker = worker
			workerRespCh <- workerResp
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
					Result: false,
					Worker: worker,
					Msg:    fmt.Sprintf("%s relevant worker-client not found", worker),
				}
				workerRespCh <- workerResp
				return
			}
			workerResp, err := cli.OperateSubTask(ctx, subReq)
			if err != nil {
				workerResp = &pb.OperateSubTaskResponse{
					Result: false,
					Msg:    errors.ErrorStack(err),
				}
			}
			workerResp.Worker = worker
			workerRespCh <- workerResp
		}(worker)
	}
	wg.Wait()

	validWorkers := make([]string, 0, len(workers))
	workerRespMap := make(map[string]*pb.OperateSubTaskResponse, len(workers))
	for len(workerRespCh) > 0 {
		workerResp := <-workerRespCh
		workerRespMap[workerResp.Worker] = workerResp
		if len(workerResp.Msg) == 0 { // no error occurred
			validWorkers = append(validWorkers, workerResp.Worker)
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
func (s *Server) UpdateTask(context.Context, *pb.UpdateTaskRequest) (*pb.CommonTaskResponse, error) {
	return &pb.CommonTaskResponse{
		Result: false,
		Msg:    "not implement",
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

// ApplyForDDLLock implements MasterServer.ApplyForDDLLock
func (s *Server) ApplyForDDLLock(context.Context, *pb.ApplyForDDLLockRequest) (*pb.CommonTaskResponse, error) {
	return &pb.CommonTaskResponse{
		Result: false,
		Msg:    "not implement",
	}, nil
}

// UnlockDDLLock implements MasterServer.UnlockDDLLock
func (s *Server) UnlockDDLLock(context.Context, *pb.UnlockDDLLockRequest) (*pb.CommonTaskResponse, error) {
	return &pb.CommonTaskResponse{
		Result: false,
		Msg:    "not implement",
	}, nil
}

// HandleSQLs implements MasterServer.HandleSQLs
func (s *Server) HandleSQLs(context.Context, *pb.HandleSQLsRequest) (*pb.CommonTaskResponse, error) {
	return &pb.CommonTaskResponse{
		Result: false,
		Msg:    "not implement",
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
	return workers
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
