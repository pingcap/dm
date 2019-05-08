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
	"io/ioutil"
	"os"
	"path"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/errors"
	"github.com/syndtr/goleveldb/leveldb"
)

func TestMeta(t *testing.T) {
	TestingT(t)
}

var (
	testTask1 = &config.SubTaskConfig{
		Name:     "task1",
		SourceID: "replica-1",
	}
	testTask1Meta  *pb.TaskMeta
	testTask1Bytes []byte

	testTask2 = &config.SubTaskConfig{
		Name:     "task2",
		SourceID: "replica-1",
	}
	testTask2Meta  *pb.TaskMeta
	testTask2Bytes []byte
)

type testMeta struct{}

var _ = Suite(&testMeta{})

func testSetUpDB(c *C) (*leveldb.DB, string) {
	c.Assert(testTask1.Adjust(), IsNil)
	c.Assert(testTask2.Adjust(), IsNil)

	testTask1Str, err := testTask1.Toml()
	c.Assert(err, IsNil)
	testTask1Bytes = []byte(testTask1Str)
	testTask1Meta = &pb.TaskMeta{
		Op:    pb.TaskOp_Start,
		Name:  testTask1.Name,
		Stage: pb.Stage_New,
		Task:  testTask1Bytes,
	}

	testTask2Str, err := testTask2.Toml()
	c.Assert(err, IsNil)
	testTask2Bytes = []byte(testTask2Str)
	testTask2Meta = &pb.TaskMeta{
		Op:    pb.TaskOp_Start,
		Name:  testTask2.Name,
		Stage: pb.Stage_New,
		Task:  testTask2Bytes,
	}

	dir := c.MkDir()
	dbDir := path.Join(dir, "kv")
	db, err := openDB(dbDir, defaultKVConfig)
	if err != nil {
		c.Fatalf("fail to open leveldb %v", err)
	}

	return db, dir
}

func (t *testMeta) TestNewMetaDB(c *C) {
	db, dir := testSetUpDB(c)
	defer db.Close()

	metaDB, err := NewMetadata(dir, db)
	c.Assert(err, IsNil)
	c.Assert(metaDB.tasks, HasLen, 0)
	c.Assert(metaDB.logs, HasLen, 0)
	c.Assert(metaDB.log, NotNil)

	// test fail to recover from old fashion meta
	err = ioutil.WriteFile(path.Join(dir, "meta"), []byte("xxxx"), 0644)
	c.Assert(err, IsNil)
	metaDB, err = NewMetadata(dir, db)
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, ".*decode old metadata.*")

	// normal old fashion meta
	oldMeta := &Meta{
		SubTasks: map[string]*config.SubTaskConfig{
			"task1": testTask1,
			"task2": testTask2,
		},
	}
	data, err := oldMeta.Toml()
	c.Assert(err, IsNil)
	err = ioutil.WriteFile(path.Join(dir, "meta"), []byte(data), 0644)
	c.Assert(err, IsNil)

	// recover from old fashion meta
	metaDB, err = NewMetadata(dir, db)
	c.Assert(err, IsNil)
	c.Assert(len(metaDB.tasks), Equals, 2)
	c.Assert(metaDB.logs, HasLen, 0)
	c.Assert(metaDB.log, NotNil)

	// check old fashion meta file
	_, err = os.Open(path.Join(dir, "meta"))
	c.Assert(os.IsNotExist(err), IsTrue)

	// check nil db
	err = ioutil.WriteFile(path.Join(dir, "meta"), []byte(data), 0644)
	c.Assert(err, IsNil)
	metaDB, err = NewMetadata(dir, nil)
	c.Assert(err, NotNil)
	c.Assert(metaDB, IsNil)

	// test no old fashion meta file but data in kv db
	metaDB, err = NewMetadata("", db)
	c.Assert(err, IsNil)
	c.Assert(len(metaDB.tasks), Equals, 2)
	c.Assert(metaDB.logs, HasLen, 0)
	c.Assert(metaDB.log, NotNil)
}

func (t *testMeta) TestTask(c *C) {
	db, dir := testSetUpDB(c)
	defer db.Close()

	meta, err := NewMetadata(dir, db)
	c.Assert(err, IsNil)
	c.Assert(len(meta.tasks), Equals, 0)
	c.Assert(meta.logs, HasLen, 0)
	c.Assert(meta.log, NotNil)
	c.Assert(meta.LoadTaskMeta(), HasLen, 0)
	c.Assert(meta.GetTask("no exists"), IsNil)

	// recover from old fashion meta
	oldMeta := &Meta{
		SubTasks: map[string]*config.SubTaskConfig{
			"task1": testTask1,
			"task2": testTask2,
		},
	}
	data, err := oldMeta.Toml()
	c.Assert(err, IsNil)
	err = ioutil.WriteFile(path.Join(dir, "meta"), []byte(data), 0644)
	c.Assert(err, IsNil)

	meta, err = NewMetadata(dir, db)
	c.Assert(err, IsNil)
	c.Assert(len(meta.tasks), Equals, 2)
	c.Assert(meta.logs, HasLen, 0)
	c.Assert(meta.log, NotNil)

	tasks := meta.LoadTaskMeta()
	c.Assert(tasks, HasLen, 2)
	c.Assert(tasks[testTask1.Name], DeepEquals, testTask1Meta)
	c.Assert(tasks[testTask2.Name], DeepEquals, testTask2Meta)

	// change previous loaded task set member
	tasks[testTask1.Name] = &pb.TaskMeta{
		Op:   pb.TaskOp_Stop,
		Name: testTask1.Name,
		Task: []byte("xxxxx"),
	}
	// change previous loaded task's field
	tasks[testTask2.Name].Task = []byte("yyyyyy")

	// test clone again
	tasks2 := meta.LoadTaskMeta()
	c.Assert(tasks2[testTask1.Name], DeepEquals, testTask1Meta)

	// test get
	task1 := meta.GetTask(testTask1.Name)
	c.Assert(task1, DeepEquals, testTask1Meta)

	task2 := meta.GetTask(testTask2.Name)
	c.Assert(task2, DeepEquals, testTask2Meta)

	task3 := meta.GetTask("no exists")
	c.Assert(task3, IsNil)
}

func (t *testMeta) TestTaskOperation(c *C) {
	db, dir := testSetUpDB(c)
	defer db.Close()

	meta, err := NewMetadata(dir, db)
	c.Assert(err, IsNil)
	c.Assert(len(meta.tasks), Equals, 0)
	c.Assert(meta.logs, HasLen, 0)
	c.Assert(meta.log, NotNil)

	// get log which doesn't exist
	log1, err := meta.GetTaskLog(1)
	c.Assert(err, IsNil)
	c.Assert(log1, IsNil)

	log1 = meta.PeekLog()
	c.Assert(log1, IsNil)

	err = meta.MarkOperation(&pb.TaskLog{
		Task: testTask1Meta,
	})
	c.Assert(errors.IsNotFound(err), IsTrue)

	/****** test append operation *******/

	/***  append two create operation ***/
	id, err := meta.AppendOperation(testTask1Meta)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(1))

	id, err = meta.AppendOperation(testTask2Meta)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(2))

	/*** append two stop operation ***/
	testTask1MetaC := CloneTaskMeta(testTask1Meta)
	testTask1MetaC.Op = pb.TaskOp_Stop
	id, err = meta.AppendOperation(testTask1MetaC)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(3))

	testTask2MetaC := CloneTaskMeta(testTask2Meta)
	testTask2MetaC.Op = pb.TaskOp_Stop
	id, err = meta.AppendOperation(testTask2MetaC)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(4))

	// test status
	c.Assert(meta.logs, HasLen, 4)
	c.Assert(meta.tasks, HasLen, 0)
	c.Assert(meta.LoadTaskMeta(), HasLen, 0)

	log1 = meta.PeekLog()
	c.Assert(log1.Task, DeepEquals, testTask1Meta)
	c.Assert(log1.Id, DeepEquals, int64(1))

	log1C, err := meta.GetTaskLog(1)
	c.Assert(err, IsNil)
	c.Assert(log1C, DeepEquals, log1)

	log2, err := meta.GetTaskLog(2)
	c.Assert(log2.Task, DeepEquals, testTask2Meta)
	c.Assert(log2.Id, DeepEquals, int64(2))

	log1s, err := meta.GetTaskLog(3)
	c.Assert(log1s.Task, DeepEquals, testTask1MetaC)
	c.Assert(log1s.Id, DeepEquals, int64(3))

	log2s, err := meta.GetTaskLog(4)
	c.Assert(log2s.Task, DeepEquals, testTask2MetaC)
	c.Assert(log2s.Id, DeepEquals, int64(4))

	/****** test mark operation *******/

	// mark disorder log
	err = meta.MarkOperation(&pb.TaskLog{
		Id:   2,
		Task: testTask2Meta,
	})
	c.Assert(err, ErrorMatches, ".*please handle task operation order by log ID.*")

	// make sucessful  task1 create log
	err = meta.MarkOperation(&pb.TaskLog{
		Id:      1,
		Task:    testTask1Meta,
		Success: true,
	})
	c.Assert(err, IsNil)

	// check log and meta
	logx := meta.PeekLog()
	c.Assert(logx, DeepEquals, log2)
	c.Assert(meta.logs, HasLen, 3)

	c.Assert(meta.tasks, HasLen, 1)
	c.Assert(meta.LoadTaskMeta(), HasLen, 1)
	task1 := meta.GetTask(testTask1Meta.Name)
	c.Assert(task1, DeepEquals, testTask1Meta)

	// make sucessful  task2 create log
	err = meta.MarkOperation(&pb.TaskLog{
		Id:      2,
		Task:    testTask2Meta,
		Success: true,
	})
	c.Assert(err, IsNil)

	// check log and meta
	logx = meta.PeekLog()
	c.Assert(logx, DeepEquals, log1s)
	c.Assert(meta.logs, HasLen, 2)
	c.Assert(meta.tasks, HasLen, 2)
	c.Assert(meta.LoadTaskMeta(), HasLen, 2)
	task2 := meta.GetTask(testTask2Meta.Name)
	c.Assert(task2, DeepEquals, testTask2Meta)

	// make failed task1 stop log
	err = meta.MarkOperation(&pb.TaskLog{
		Id:      3,
		Task:    testTask1MetaC,
		Success: false,
	})
	c.Assert(err, IsNil)

	// check log and meta
	logx = meta.PeekLog()
	c.Assert(logx, DeepEquals, log2s)
	c.Assert(meta.logs, HasLen, 1)
	c.Assert(meta.tasks, HasLen, 2)
	c.Assert(meta.LoadTaskMeta(), HasLen, 2)
	task1 = meta.GetTask(testTask1Meta.Name)
	c.Assert(task1, DeepEquals, testTask1Meta)

	// make successful task2 stop log
	err = meta.MarkOperation(&pb.TaskLog{
		Id:      4,
		Task:    testTask2MetaC,
		Success: true,
	})
	c.Assert(err, IsNil)

	// check log and meta
	logx = meta.PeekLog()
	c.Assert(logx, IsNil)
	c.Assert(meta.logs, HasLen, 0)
	c.Assert(meta.tasks, HasLen, 1)
	c.Assert(meta.LoadTaskMeta(), HasLen, 1)
	task1 = meta.GetTask(testTask1Meta.Name)
	c.Assert(task1, DeepEquals, testTask1Meta)
	task2 = meta.GetTask(testTask2Meta.Name)
	c.Assert(task2, IsNil)
}

func (t *testMeta) TestMetaClose(c *C) {
	db, dir := testSetUpDB(c)
	defer db.Close()

	meta, err := NewMetadata(dir, db)
	c.Assert(err, IsNil)
	meta.Close()
	meta.Close()

	// recover from old fashion meta
	oldMeta := &Meta{
		SubTasks: map[string]*config.SubTaskConfig{
			"task1": testTask1,
			"task2": testTask2,
		},
	}
	data, err := oldMeta.Toml()
	c.Assert(err, IsNil)
	err = ioutil.WriteFile(path.Join(dir, "meta"), []byte(data), 0644)
	c.Assert(err, IsNil)

	meta, err = NewMetadata(dir, db)
	c.Assert(err, IsNil)
	meta.Close()
	meta.Close()
}
