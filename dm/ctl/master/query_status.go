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

	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"

	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
)

const stageError = "Error"

type taskResult struct {
	Result bool        `json:"result"`
	Msg    string      `json:"msg"`
	Tasks  []*taskInfo `json:"tasks"`
}

type taskInfo struct {
	TaskName   string   `json:"taskName,omitempty"`
	TaskStatus string   `json:"taskStatus,omitempty"`
	Workers    []string `json:"workers,omitempty"`
}

// NewQueryStatusCmd creates a QueryStatus command
func NewQueryStatusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "query-status [-w worker ...] [task-name]",
		Short: "query task status",
		Run:   queryStatusFunc,
	}
	return cmd
}

// queryStatusFunc does query task's status
func queryStatusFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) > 1 {
		fmt.Println(cmd.Usage())
		return
	}
	taskName := cmd.Flags().Arg(0) // maybe empty

	workers, err := common.GetWorkerArgs(cmd)
	if err != nil {
		common.PrintLines("%s", errors.ErrorStack(err))
		return
	}

	cli := common.MasterClient()
	ctx, cancel := context.WithTimeout(context.Background(), common.GlobalConfig().RPCTimeout)
	defer cancel()
	resp, err := cli.QueryStatus(ctx, &pb.QueryStatusListRequest{
		Name:    taskName,
		Workers: workers,
	})
	if err != nil {
		common.PrintLines("can not query %s task's status(in workers %v):\n%s", taskName, workers, errors.ErrorStack(err))
		return
	}

	if resp.Result && taskName == "" && len(workers) == 0 {
		result := wrapTaskResult(resp)
		common.PrettyPrintInterface(result)
	} else {
		common.PrettyPrintResponse(resp)
	}
}

// wrapTaskResult pick task info and generate tasks' status and relative workers
func wrapTaskResult(resp *pb.QueryStatusListResponse) *taskResult {
	taskStatusMap := make(map[string]string, len(resp.Workers))
	taskCorrespondingWorkers := make(map[string][]string, len(resp.Workers))
	for _, worker := range resp.Workers {
		for _, subTask := range worker.SubTaskStatus {
			subTaskName := subTask.Name
			subTaskStage := subTask.Stage

			taskCorrespondingWorkers[subTaskName] = append(taskCorrespondingWorkers[subTaskName], worker.Worker)
			taskStage := taskStatusMap[subTaskName]
			switch {
			case taskStage == stageError:
			case subTaskStage == pb.Stage_Paused && subTask.Result != nil && len(subTask.Result.Errors) > 0:
				taskStatusMap[subTaskName] = stageError
			case taskStage == pb.Stage_Paused.String():
			case taskStage == "", subTaskStage == pb.Stage_Paused:
				taskStatusMap[subTaskName] = subTaskStage.String()
			case taskStage != subTaskStage.String():
				taskStatusMap[subTaskName] = pb.Stage_Running.String()
			}
		}
	}
	taskList := make([]*taskInfo, 0, len(taskStatusMap))
	for curTaskName, taskStatus := range taskStatusMap {
		taskList = append(taskList,
			&taskInfo{
				TaskName:   curTaskName,
				TaskStatus: taskStatus,
				Workers:    taskCorrespondingWorkers[curTaskName],
			})
	}
	return &taskResult{
		Result: resp.Result,
		Msg:    resp.Msg,
		Tasks:  taskList,
	}
}
