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
	"os"
	"strings"

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
		Use:   "query-status [-s source ...] [task-name]",
		Short: "query task status",
		Run:   queryStatusFunc,
	}
	return cmd
}

// queryStatusFunc does query task's status
func queryStatusFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) > 1 {
		cmd.SetOut(os.Stdout)
		cmd.Usage()
		return
	}
	taskName := cmd.Flags().Arg(0) // maybe empty

	sources, err := common.GetSourceArgs(cmd)
	if err != nil {
		common.PrintLines("%s", errors.ErrorStack(err))
		return
	}

	cli := common.MasterClient()
	ctx, cancel := context.WithTimeout(context.Background(), common.GlobalConfig().RPCTimeout)
	defer cancel()
	resp, err := cli.QueryStatus(ctx, &pb.QueryStatusListRequest{
		Name:    taskName,
		Sources: sources,
	})
	if err != nil {
		common.PrintLines("can not query %s task's status(in sources %v):\n%s", taskName, sources, errors.ErrorStack(err))
		return
	}

	if resp.Result && taskName == "" && len(sources) == 0 {
		result := wrapTaskResult(resp)
		common.PrettyPrintInterface(result)
	} else {
		common.PrettyPrintResponse(resp)
	}
}

// errorOccurred checks ProcessResult and return true if some error occurred
func errorOccurred(result *pb.ProcessResult) bool {
	return result != nil && len(result.Errors) > 0
}

// getRelayStage returns current relay stage (including stageError)
func getRelayStage(relayStatus *pb.RelayStatus) string {
	if errorOccurred(relayStatus.Result) {
		return stageError
	}
	return relayStatus.Stage.String()
}

// wrapTaskResult picks task info and generate tasks' status and relative workers
func wrapTaskResult(resp *pb.QueryStatusListResponse) *taskResult {
	taskStatusMap := make(map[string]string)
	taskCorrespondingWorkers := make(map[string][]string)
	for _, worker := range resp.Workers {
		relayStatus := worker.RelayStatus
		for _, subTask := range worker.SubTaskStatus {
			subTaskName := subTask.Name
			subTaskStage := subTask.Stage

			taskCorrespondingWorkers[subTaskName] = append(taskCorrespondingWorkers[subTaskName], worker.Worker)
			taskStage := taskStatusMap[subTaskName]
			// the status of a task is decided by its subtasks, the rule is listed as follows:
			// |                     Subtasks' status                       |                Task's status                 |
			// | :--------------------------------------------------------: | :------------------------------------------: |
			// |           Any Paused and len(result.errors) > 0            |    Error - Some error occurred in subtask    |
			// | Any Running and unit is "Sync" and relay is Paused/Stopped | Error - Relay status is Error/Paused/Stopped |
			// |              Any Paused but without error                  |                    Paused                    |
			// |                        All New                             |                     New                      |
			// |                      All Finished                          |                   Finished                   |
			// |                      All Stopped                           |                   Stopped                    |
			// |                         Others                             |                   Running                    |
			switch {
			case strings.HasPrefix(taskStage, stageError):
			case subTaskStage == pb.Stage_Paused && errorOccurred(subTask.Result):
				taskStatusMap[subTaskName] = stageError + " - Some error occurred in subtask"
			case subTask.Unit == pb.UnitType_Sync && subTask.Stage == pb.Stage_Running && (relayStatus.Stage == pb.Stage_Paused || relayStatus.Stage == pb.Stage_Stopped):
				taskStatusMap[subTaskName] = stageError + " - Relay status is " + getRelayStage(relayStatus)
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
		if strings.HasPrefix(taskStatus, stageError) {
			taskStatus += ". Please run `query-status " + curTaskName + "` to get more details."
		}
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
