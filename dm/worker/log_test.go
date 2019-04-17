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
	c.Assert(logger.endPointer.Location, Equals, int64(0))

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
	c.Assert(taskLog1.Id, Equals, int64(0))
	c.Assert(taskLog1.Ts > 0, IsTrue)
	c.Assert(taskLog1.Task.Name, Equals, "task")

	taskLog2 := &pb.TaskLog{
		Id: 200,
		Task: &pb.TaskMeta{
			Name: "task",
		},
	}

	c.Assert(logger.Append(db, taskLog2), IsNil)
	c.Assert(taskLog2.Id, Equals, int64(1))
	c.Assert(taskLog2.Ts > 0, IsTrue)
	c.Assert(taskLog2.Task.Name, Equals, "task")

	// try to get log
	l, err = logger.GetTaskLog(db, 0)
	c.Assert(err, IsNil)
	c.Assert(l, DeepEquals, taskLog1)

	l, err = logger.GetTaskLog(db, 1)
	c.Assert(err, IsNil)
	c.Assert(l, DeepEquals, taskLog2)

	logs, err = logger.Initial(db)
	c.Assert(err, IsNil)
	c.Assert(logs, DeepEquals, []*pb.TaskLog{taskLog1, taskLog2})
	c.Assert(logger.handledPointer.Location, Equals, int64(0))
	c.Assert(logger.endPointer.Location, Equals, int64(1))
}
