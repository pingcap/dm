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
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/dm/dm/pb"
)

func (t *testWorker) TestPointer(c *C) {
	p := &Pointer{
		Location: 2,
	}

	np := new(Pointer)
	c.Assert(np.UnmarshalBinary(p.MarshalBinary()), IsNil)
	c.Assert(np, DeepEquals, p)
	c.Assert(np.UnmarshalBinary([]byte("xx")), NotNil)
}

func (t *testWorker) TestLoadHandledPointer(c *C) {
	p, err := LoadHandledPointer(nil)
	c.Assert(err, Equals, ErrInValidHandler)
	c.Assert(p.Location, Equals, int64(0))

	db, _ := t.setUpDB(c)
	defer db.Close()
	p, err = LoadHandledPointer(db)
	c.Assert(err, IsNil)
	c.Assert(p.Location, Equals, int64(0))

	p = Pointer{
		Location: 1,
	}

	c.Assert(db.Put(HandledPointerKey, p.MarshalBinary(), nil), IsNil)
	p, err = LoadHandledPointer(db)
	c.Assert(err, IsNil)
	c.Assert(p.Location, Equals, int64(1))

	c.Assert(db.Put(HandledPointerKey, []byte("xx"), nil), IsNil)
	_, err = LoadHandledPointer(db)
	c.Assert(strings.Contains(err.Error(), "no enough data"), IsTrue)
}

func (t *testWorker) TestTaskLogKey(c *C) {
	var id int64 = 1
	idc, err := DecodeTaskLogKey(EncodeTaskLogKey(id))
	c.Assert(err, IsNil)
	c.Assert(idc, DeepEquals, id)

	_, err = DecodeTaskLogKey([]byte("xx"))
	c.Assert(strings.Contains(err.Error(), "no enough data"), IsTrue)
}

func (t *testWorker) TestTaskLog(c *C) {
	logger := new(Logger)

	_, err := logger.Initial(nil)
	c.Assert(err, Equals, ErrInValidHandler)

	db, _ := t.setUpDB(c)
	defer db.Close()
	logs, err := logger.Initial(db)
	c.Assert(logs, HasLen, 0)
	c.Assert(err, IsNil)
	c.Assert(logger.handledPointer.Location, Equals, int64(0))
	c.Assert(logger.endPointer.Location, Equals, int64(1))

	// try to get log from empty queue
	l, err := logger.GetTaskLog(nil, 0)
	c.Assert(err, Equals, ErrInValidHandler)

	l, err = logger.GetTaskLog(db, 0)
	c.Assert(err, IsNil)
	c.Assert(l, IsNil)

	// try to append log
	c.Assert(logger.Append(nil, nil), Equals, ErrInValidHandler)

	taskLog1 := &pb.TaskLog{
		Id: 100,
		Task: &pb.TaskMeta{
			Name: "task",
		},
	}

	c.Assert(logger.Append(db, taskLog1), IsNil)
	c.Assert(taskLog1.Id, Equals, int64(1))
	c.Assert(taskLog1.Ts > 0, IsTrue)
	c.Assert(taskLog1.Task.Name, Equals, "task")

	taskLog2 := &pb.TaskLog{
		Id: 200,
		Task: &pb.TaskMeta{
			Name: "task",
		},
	}

	c.Assert(logger.Append(db, taskLog2), IsNil)
	c.Assert(taskLog2.Id, Equals, int64(2))
	c.Assert(taskLog2.Ts > 0, IsTrue)
	c.Assert(taskLog2.Task.Name, Equals, "task")

	// try to get log
	l, err = logger.GetTaskLog(db, 0)
	c.Assert(err, IsNil)
	c.Assert(l, IsNil)

	l, err = logger.GetTaskLog(db, 1)
	c.Assert(err, IsNil)
	c.Assert(l, DeepEquals, taskLog1)

	l, err = logger.GetTaskLog(db, 2)
	c.Assert(err, IsNil)
	c.Assert(l, DeepEquals, taskLog2)

	logs, err = logger.Initial(db)
	c.Assert(err, IsNil)
	c.Assert(logs, DeepEquals, []*pb.TaskLog{taskLog1, taskLog2})
	c.Assert(logger.handledPointer.Location, Equals, int64(0))
	c.Assert(logger.endPointer.Location, Equals, int64(3))

	// try to forward
	c.Assert(logger.ForwardTo(db, 1), IsNil)
	c.Assert(logger.handledPointer.Location, Equals, int64(1))
	hp, err := LoadHandledPointer(db)
	c.Assert(err, IsNil)
	c.Assert(hp.Location, Equals, int64(1))

	c.Assert(logger.ForwardTo(db, 2), IsNil)
	c.Assert(logger.handledPointer.Location, Equals, int64(2))
	hp, err = LoadHandledPointer(db)
	c.Assert(err, IsNil)
	c.Assert(hp.Location, Equals, int64(2))

	c.Assert(logger.ForwardTo(db, 0), IsNil)
	c.Assert(logger.handledPointer.Location, Equals, int64(0))
	hp, err = LoadHandledPointer(db)
	c.Assert(err, IsNil)
	c.Assert(hp.Location, Equals, int64(0))

	// try to mark and forward
	c.Assert(logger.MarkAndForwardLog(nil, nil), Equals, ErrInValidHandler)
	c.Assert(logger.MarkAndForwardLog(db, taskLog1), IsNil)
	c.Assert(logger.handledPointer.Location, Equals, int64(1))
	hp, err = LoadHandledPointer(db)
	c.Assert(err, IsNil)
	c.Assert(hp.Location, Equals, int64(1))
	c.Assert(logger.endPointer.Location, Equals, int64(3))

	c.Assert(logger.MarkAndForwardLog(db, taskLog2), IsNil)
	c.Assert(logger.handledPointer.Location, Equals, int64(2))
	hp, err = LoadHandledPointer(db)
	c.Assert(err, IsNil)
	c.Assert(hp.Location, Equals, int64(2))
	c.Assert(logger.endPointer.Location, Equals, int64(3))

	// append again
	taskLog3 := &pb.TaskLog{
		Id: 300,
		Task: &pb.TaskMeta{
			Name: "task",
		},
	}

	c.Assert(logger.Append(db, taskLog3), IsNil)
	c.Assert(taskLog3.Id, Equals, int64(3))
	c.Assert(taskLog3.Ts > 0, IsTrue)
	c.Assert(taskLog3.Task.Name, Equals, "task")
	c.Assert(logger.endPointer.Location, Equals, int64(4))
	c.Assert(logger.handledPointer.Location, Equals, int64(2))

	logs, err = logger.Initial(db)
	c.Assert(err, IsNil)
	c.Assert(logs, DeepEquals, []*pb.TaskLog{taskLog3})
	c.Assert(logger.handledPointer.Location, Equals, int64(2))
	c.Assert(logger.endPointer.Location, Equals, int64(4))
}

func (t *testWorker) TestTaskLogGC(c *C) {
	logger := new(Logger)

	db, _ := t.setUpDB(c)
	defer db.Close()

	// append logs
	taskLog1 := &pb.TaskLog{
		Id: 1,
		Task: &pb.TaskMeta{
			Name: "task",
		},
		Ts: 10,
	}
	log1Bytes, err := taskLog1.Marshal()
	c.Assert(err, IsNil)
	c.Assert(db.Put(EncodeTaskLogKey(1), log1Bytes, nil), IsNil)

	taskLog2 := &pb.TaskLog{
		Id: 30,
		Task: &pb.TaskMeta{
			Name: "task",
		},
		Ts: 30,
	}
	log2Bytes, err := taskLog2.Marshal()
	c.Assert(err, IsNil)
	c.Assert(db.Put(EncodeTaskLogKey(30), log2Bytes, nil), IsNil)

	taskLog3 := &pb.TaskLog{
		Id: 60,
		Task: &pb.TaskMeta{
			Name: "task",
		},
		Ts: 60,
	}
	log3Bytes, err := taskLog3.Marshal()
	c.Assert(err, IsNil)
	c.Assert(db.Put(EncodeTaskLogKey(60), log3Bytes, nil), IsNil)

	// forward
	c.Assert(logger.ForwardTo(db, 59), IsNil)

	// gc
	logger.doGC(db, 59)

	logs, err := logger.Initial(db)
	c.Assert(logs, DeepEquals, []*pb.TaskLog{taskLog3})
	c.Assert(logger.handledPointer.Location, Equals, int64(59))
	c.Assert(logger.endPointer.Location, Equals, int64(61))
}

func (t *testWorker) TestTaskMeta(c *C) {
	db, _ := t.setUpDB(c)
	defer db.Close()

	// set task meta
	c.Assert(SetTaskMeta(nil, nil), Equals, ErrInValidHandler)
	err := SetTaskMeta(db, nil)
	c.Assert(strings.Contains(err.Error(), "task is empty"), IsTrue)

	task1 := &pb.TaskMeta{}
	err = SetTaskMeta(db, task1)
	c.Assert(strings.Contains(err.Error(), "task name is empty"), IsTrue)

	task1 = &pb.TaskMeta{
		Name:  "task1",
		Task:  []byte("xx"),
		Stage: pb.Stage_New,
		Op:    pb.TaskOp_Start,
	}
	c.Assert(SetTaskMeta(db, task1), IsNil)

	task2 := &pb.TaskMeta{
		Name:  "task2",
		Task:  []byte("xx"),
		Stage: pb.Stage_New,
		Op:    pb.TaskOp_Start,
	}
	c.Assert(SetTaskMeta(db, task2), IsNil)

	// load task meta
	_, err = LoadTaskMetas(nil)
	c.Assert(err, Equals, ErrInValidHandler)
	tasks, err := LoadTaskMetas(db)
	c.Assert(err, IsNil)
	c.Assert(tasks, DeepEquals, map[string]*pb.TaskMeta{
		"task1": task1,
		"task2": task2,
	})

	// get task meta
	t1, err := GetTaskMeta(db, "task1")
	c.Assert(err, IsNil)
	c.Assert(t1, DeepEquals, task1)
	t2, err := GetTaskMeta(db, "task2")
	c.Assert(err, IsNil)
	c.Assert(t2, DeepEquals, task2)

	// delete task meta
	c.Assert(DeleteTaskMeta(db, "task1"), IsNil)

	// load task meta
	tasks, err = LoadTaskMetas(db)
	c.Assert(err, IsNil)
	c.Assert(tasks, DeepEquals, map[string]*pb.TaskMeta{
		"task2": task2,
	})

	// get task meta
	t1, err = GetTaskMeta(db, "task1")
	c.Assert(err, NotNil)
	t2, err = GetTaskMeta(db, "task2")
	c.Assert(err, IsNil)
	c.Assert(t2, DeepEquals, task2)

	// delete task meta
	c.Assert(DeleteTaskMeta(db, "task2"), IsNil)

	// load task meta
	tasks, err = LoadTaskMetas(db)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 0)

	// get task meta
	t1, err = GetTaskMeta(db, "task1")
	c.Assert(err, NotNil)
	t2, err = GetTaskMeta(db, "task2")
	c.Assert(err, NotNil)
}
