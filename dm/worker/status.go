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
	"encoding/json"
	"fmt"
	"sort"

	"github.com/golang/protobuf/jsonpb"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/errors"
)

// Status returns the status of the current sub task
func (st *SubTask) Status() interface{} {
	return st.CurrUnit().Status()
}

// Error returns the error of the current sub task
func (st *SubTask) Error() interface{} {
	return st.CurrUnit().Error()
}

// StatusJSON returns the status of the current sub task as json string
func (st *SubTask) StatusJSON() string {
	sj, err := json.Marshal(st.Status())
	if err != nil {
		log.Errorf("[subtask] marshal status error %s", errors.ErrorStack(err))
		return ""
	}
	return string(sj)
}

// Status returns the status of the worker (and sub tasks)
// if stName is empty, all sub task's status will be returned
func (w *Worker) Status(stName string) []*pb.SubTaskStatus {
	w.Lock()
	defer w.Unlock()

	if len(w.subTasks) == 0 {
		return nil // no sub task started
	}

	status := make([]*pb.SubTaskStatus, 0, len(w.subTasks))

	// return status order by name
	names := make([]string, 0, len(w.subTasks))
	if len(stName) > 0 {
		names = append(names, stName)
	} else {
		for name := range w.subTasks {
			names = append(names, name)
		}
	}
	sort.Strings(names)

	for _, name := range names {
		st, ok := w.subTasks[name]
		var stStatus pb.SubTaskStatus
		if !ok {
			stStatus = pb.SubTaskStatus{
				Name:   name,
				Status: &pb.SubTaskStatus_Msg{Msg: fmt.Sprintf("no sub task with name %s has started", name)},
			}
		} else {
			var lockID = ""
			lockInfo := st.DDLLockInfo()
			if lockInfo != nil {
				lockID = lockInfo.ID
			}
			cu := st.CurrUnit()

			stStatus = pb.SubTaskStatus{
				Name:                name,
				Stage:               st.Stage(),
				Result:              st.Result(),
				UnresolvedDDLLockID: lockID,
			}

			if cu != nil {
				stStatus.Unit = cu.Type()
				// oneof status
				us := st.Status()
				switch stStatus.Unit {
				case pb.UnitType_Check:
					stStatus.Status = &pb.SubTaskStatus_Check{Check: us.(*pb.CheckStatus)}
				case pb.UnitType_Dump:
					stStatus.Status = &pb.SubTaskStatus_Dump{Dump: us.(*pb.DumpStatus)}
				case pb.UnitType_Load:
					stStatus.Status = &pb.SubTaskStatus_Load{Load: us.(*pb.LoadStatus)}
				case pb.UnitType_Sync:
					stStatus.Status = &pb.SubTaskStatus_Sync{Sync: us.(*pb.SyncStatus)}
				}
			}
		}
		status = append(status, &stStatus)
	}

	return status
}

// StatusJSON returns the status of the worker as json string
func (w *Worker) StatusJSON(stName string) string {
	sl := &pb.SubTaskStatusList{Status: w.Status(stName)}
	mar := jsonpb.Marshaler{EmitDefaults: true, Indent: "    "}
	s, err := mar.MarshalToString(sl)
	if err != nil {
		log.Errorf("[worker] marshal status error %s", errors.ErrorStack(err))
		return ""
	}
	return s
}

// Error returns the error information of the worker (and sub tasks)
// if stName is empty, all sub task's error information will be returned
func (w *Worker) Error(stName string) []*pb.SubTaskError {
	w.Lock()
	defer w.Unlock()
	if len(w.subTasks) == 0 {
		return nil // no sub task started
	}

	error := make([]*pb.SubTaskError, 0, len(w.subTasks))

	// return error order by name
	names := make([]string, 0, len(w.subTasks))
	if len(stName) > 0 {
		names = append(names, stName)
	} else {
		for name := range w.subTasks {
			names = append(names, name)
		}
	}
	sort.Strings(names)

	for _, name := range names {
		st, ok := w.subTasks[name]
		var stError pb.SubTaskError
		if !ok {
			stError = pb.SubTaskError{
				Error: &pb.SubTaskError_Msg{Msg: fmt.Sprintf("no sub task with name %s has started", name)},
			}
		} else {
			cu := st.CurrUnit()
			stError = pb.SubTaskError{
				Name:  name,
				Stage: st.Stage(),
				Unit:  cu.Type(),
			}

			// oneof error
			us := st.Error()
			switch cu.Type() {
			case pb.UnitType_Check:
				stError.Error = &pb.SubTaskError_Check{Check: us.(*pb.CheckError)}
			case pb.UnitType_Dump:
				stError.Error = &pb.SubTaskError_Dump{Dump: us.(*pb.DumpError)}
			case pb.UnitType_Load:
				stError.Error = &pb.SubTaskError_Load{Load: us.(*pb.LoadError)}
			case pb.UnitType_Sync:
				stError.Error = &pb.SubTaskError_Sync{Sync: us.(*pb.SyncError)}
			}
		}
		error = append(error, &stError)
	}

	return error
}
