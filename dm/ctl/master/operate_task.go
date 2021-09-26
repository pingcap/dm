// Copyright 2021 PingCAP, Inc.
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
	"errors"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/spf13/cobra"

	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
)

const (
	batchSizeFlag    = "batch-size"
	defaultBatchSize = 5
)

type batchTaskResult struct {
	Result       bool                 `json:"result"`
	Msg          string               `json:"msg"`
	BatchResults []*operateTaskResult `json:"batchResults"`
}

type operateTaskResult struct {
	TaskName string                     `json:"taskName"`
	Op       string                     `json:"op"`
	Result   bool                       `json:"result"`
	Msg      string                     `json:"msg"`
	Sources  []*pb.CommonWorkerResponse `json:"sources"`
}

func operateTaskFunc(taskOp pb.TaskOp, cmd *cobra.Command) error {
	argLen := len(cmd.Flags().Args())
	if argLen == 0 {
		// may want to operate tasks bound to a source
		return operateSourceTaskFunc(taskOp, cmd)
	} else if argLen > 1 {
		// can pass at most one task-name/task-conf
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.New("please check output to see error")
	}

	name := common.GetTaskNameFromArgOrFile(cmd.Flags().Arg(0))
	sources, err := common.GetSourceArgs(cmd)
	if err != nil {
		return err
	}

	resp, err := common.OperateTask(taskOp, name, sources)
	if err != nil {
		common.PrintLinesf("can not %s task %s", strings.ToLower(taskOp.String()), name)
		return err
	}

	common.PrettyPrintResponse(resp)
	return nil
}

func addOperateSourceTaskFlags(cmd *cobra.Command) {
	// control workload to dm-cluster for sources with large number of tasks.
	cmd.Flags().Int(batchSizeFlag, defaultBatchSize, "batch size")
}

func operateSourceTaskFunc(taskOp pb.TaskOp, cmd *cobra.Command) error {
	source, batchSize, ok := parseOperateSourceTaskParams(cmd)
	if !ok {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.New("please check output to see error")
	}

	sources := []string{source}
	ctx, cancel := context.WithTimeout(context.Background(), common.GlobalConfig().RPCTimeout)
	defer cancel()

	req := pb.QueryStatusListRequest{Sources: sources}
	resp := &pb.QueryStatusListResponse{}
	if err := common.SendRequest(ctx, "QueryStatus", &req, &resp); err != nil {
		common.PrintLinesf("cannot query status of source: %v", sources)
		return err
	}

	if !resp.Result || len(resp.Sources) == 0 {
		common.PrettyPrintInterface(&batchTaskResult{Result: false, Msg: resp.Msg, BatchResults: []*operateTaskResult{}})
		return nil
	}

	result := batchOperateTask(taskOp, batchSize, sources, resp.Sources[0].SubTaskStatus)
	common.PrettyPrintInterface(result)

	return nil
}

func batchOperateTask(taskOp pb.TaskOp, batchSize int, sources []string, subTaskStatus []*pb.SubTaskStatus) *batchTaskResult {
	result := batchTaskResult{Result: true, BatchResults: []*operateTaskResult{}}

	if len(subTaskStatus) < batchSize {
		batchSize = len(subTaskStatus)
	}

	workCh := make(chan string)
	go func() {
		for _, subTask := range subTaskStatus {
			workCh <- subTask.Name
		}
		close(workCh)
	}()

	var wg sync.WaitGroup
	resultCh := make(chan *operateTaskResult, 1)
	for i := 0; i < batchSize; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for name := range workCh {
				taskResult := operateTaskResult{TaskName: name, Op: taskOp.String()}
				taskOpResp, err := common.OperateTask(taskOp, name, sources)
				if err != nil {
					taskResult.Result = false
					taskResult.Msg = err.Error()
				} else {
					taskResult.Result = taskOpResp.Result
					taskResult.Msg = taskOpResp.Msg
					taskResult.Sources = taskOpResp.Sources
				}
				resultCh <- &taskResult
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	for item := range resultCh {
		result.BatchResults = append(result.BatchResults, item)
	}

	sort.Slice(result.BatchResults, func(i, j int) bool {
		return result.BatchResults[i].TaskName < result.BatchResults[j].TaskName
	})

	return &result
}

func parseOperateSourceTaskParams(cmd *cobra.Command) (string, int, bool) {
	sources, err := common.GetSourceArgs(cmd)
	if err != nil {
		return "", 0, false
	}
	if len(sources) == 0 {
		common.PrintLinesf(`must give one source-name when task-name/task-conf is not specified`)
		return "", 0, false
	} else if len(sources) > 1 {
		common.PrintLinesf(`can give only one source-name when task-name/task-conf is not specified`)
		return "", 0, false
	}
	batchSize, err := cmd.Flags().GetInt(batchSizeFlag)
	if err != nil {
		common.PrintLinesf("error in parse `--" + batchSizeFlag + "`")
		return "", 0, false
	}
	return sources[0], batchSize, true
}
