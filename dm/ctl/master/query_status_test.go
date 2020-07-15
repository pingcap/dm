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
	"sort"
	"testing"

	"github.com/pingcap/dm/dm/pb"

	"github.com/pingcap/check"
)

func TestCtlMaster(t *testing.T) {
	check.TestingT(t)
}

type testCtlMaster struct {
}

var _ = check.Suite(&testCtlMaster{})

func generateAndCheckTaskResult(c *check.C, resp *pb.QueryStatusListResponse, expectedResult []*taskInfo) {
	result := wrapTaskResult(resp)
	c.Assert(result.Result, check.IsTrue)
	c.Assert(result.Tasks, check.HasLen, 1)
	sort.Strings(result.Tasks[0].Workers)
	c.Assert(result.Tasks, check.DeepEquals, expectedResult)
}

func subTestSameSubTaskStatus(c *check.C, resp *pb.QueryStatusListResponse, expectedResult []*taskInfo, stage pb.Stage) {
	for i := range resp.Workers {
		resp.Workers[i].SubTaskStatus[0].Stage = stage
	}
	expectedResult[0].TaskStatus = stage.String()
	generateAndCheckTaskResult(c, resp, expectedResult)
}

func (t *testCtlMaster) TestWrapTaskResult(c *check.C) {
	resp := new(pb.QueryStatusListResponse)
	resp.Result = true

	// Should return error when some error occurs in subtask
	resp.Workers = []*pb.QueryStatusResponse{
		{
			Result: true,
			Worker: "172.17.0.2:8262",
			SubTaskStatus: []*pb.SubTaskStatus{{
				Name:  "test",
				Stage: pb.Stage_Running,
			}},
		},
		{
			Result: true,
			Worker: "172.17.0.3:8262",
			SubTaskStatus: []*pb.SubTaskStatus{{
				Name:  "test",
				Stage: pb.Stage_Running,
			}},
		},
		{
			Result: true,
			Worker: "172.17.0.6:8262",
			SubTaskStatus: []*pb.SubTaskStatus{{
				Name:  "test",
				Stage: pb.Stage_Paused,
				Result: &pb.ProcessResult{
					Errors: []*pb.ProcessError{{}},
				},
			}},
		},
	}
	extraInfo := ". Please run `query-status test` to get more details."
	expectedResult := []*taskInfo{{
		TaskName:   "test",
		TaskStatus: stageError + " - Some error occurred in subtask" + extraInfo,
		Workers:    []string{"172.17.0.2:8262", "172.17.0.3:8262", "172.17.0.6:8262"},
	}}
	generateAndCheckTaskResult(c, resp, expectedResult)
	// Should return error when subtask unit is "Sync" while relay status is not running
	resp.Workers[2].SubTaskStatus[0].Result = nil
	resp.Workers[0].SubTaskStatus[0].Unit = pb.UnitType_Sync
	// relay status is Error
	resp.Workers[0].RelayStatus = &pb.RelayStatus{
		Stage: pb.Stage_Paused,
		Result: &pb.ProcessResult{
			Errors: []*pb.ProcessError{{}},
		}}
	expectedResult[0].TaskStatus = stageError + " - Relay status is " + stageError + extraInfo
	generateAndCheckTaskResult(c, resp, expectedResult)
	// relay status is Paused
	resp.Workers[0].RelayStatus = &pb.RelayStatus{Stage: pb.Stage_Paused}
	expectedResult[0].TaskStatus = stageError + " - Relay status is " + pb.Stage_Paused.String() + extraInfo
	generateAndCheckTaskResult(c, resp, expectedResult)
	// relay status is Stopped
	resp.Workers[0].RelayStatus = &pb.RelayStatus{Stage: pb.Stage_Stopped}
	expectedResult[0].TaskStatus = stageError + " - Relay status is " + pb.Stage_Stopped.String() + extraInfo
	generateAndCheckTaskResult(c, resp, expectedResult)

	// one subtask is paused and no error occurs, should return paused
	resp.Workers[2].SubTaskStatus[0].Result = nil
	resp.Workers[0].SubTaskStatus[0].Unit = 0
	resp.Workers[0].RelayStatus = nil
	expectedResult[0].TaskStatus = pb.Stage_Paused.String()
	generateAndCheckTaskResult(c, resp, expectedResult)
	// All subtasks are Finished/Stopped/.../New
	stageArray := []pb.Stage{pb.Stage_Finished, pb.Stage_Stopped, pb.Stage_Paused, pb.Stage_Running, pb.Stage_New}
	for _, stage := range stageArray {
		subTestSameSubTaskStatus(c, resp, expectedResult, stage)
	}
	// All subtasks are New except the last one(which is Finished)
	resp.Workers[2].SubTaskStatus[0].Stage = pb.Stage_Finished
	expectedResult[0].TaskStatus = pb.Stage_Running.String()
	generateAndCheckTaskResult(c, resp, expectedResult)

	// test situation with two tasks
	resp.Workers = append(resp.Workers, &pb.QueryStatusResponse{
		Result: true,
		Worker: "172.17.0.4:8262",
		SubTaskStatus: []*pb.SubTaskStatus{{
			Name:  "test2",
			Stage: pb.Stage_Paused,
			Result: &pb.ProcessResult{
				Errors: []*pb.ProcessError{{}},
			},
		}},
	})
	result := wrapTaskResult(resp)
	c.Assert(result.Tasks, check.HasLen, 2)
	if result.Tasks[0].TaskName == "test2" {
		result.Tasks[0], result.Tasks[1] = result.Tasks[1], result.Tasks[0]
	}
	sort.Strings(result.Tasks[0].Workers)
	expectedResult = []*taskInfo{{
		TaskName:   "test",
		TaskStatus: pb.Stage_Running.String(),
		Workers:    []string{"172.17.0.2:8262", "172.17.0.3:8262", "172.17.0.6:8262"},
	}, {
		TaskName:   "test2",
		TaskStatus: stageError + " - Some error occurred in subtask. Please run `query-status test2` to get more details.",
		Workers:    []string{"172.17.0.4:8262"},
	},
	}
	c.Assert(result.Tasks, check.DeepEquals, expectedResult)
}
