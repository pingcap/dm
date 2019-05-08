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
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"path"
	"sync"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/errors"
	"github.com/syndtr/goleveldb/leveldb"
)

// Meta information contains (deprecated, instead of proto.WorkMeta)
// * sub-task
type Meta struct {
	SubTasks map[string]*config.SubTaskConfig `json:"sub-tasks" toml:"sub-tasks"`
}

// Toml returns TOML format representation of config
func (m *Meta) Toml() (string, error) {
	var b bytes.Buffer
	enc := toml.NewEncoder(&b)
	err := enc.Encode(m)
	if err != nil {
		return "", errors.Trace(err)
	}
	return b.String(), nil
}

// DecodeFile loads and decodes config from file
func (m *Meta) DecodeFile(fpath string) error {
	_, err := toml.DecodeFile(fpath, m)
	if err != nil {
		return errors.Trace(err)
	}

	return m.adjust()
}

// Decode loads config from file data
func (m *Meta) Decode(data string) error {
	_, err := toml.Decode(data, m)
	if err != nil {
		return errors.Trace(err)
	}

	return m.adjust()
}

func (m *Meta) adjust() error {
	// adjust the config
	for name, subTask := range m.SubTasks {
		err := subTask.Adjust()
		if err != nil {
			return errors.Annotatef(err, "task %s", name)
		}
	}
	return nil
}

// Metadata stores metadata and log of task
// it also provides logger feature
// * append log
// * forward to specified log location
type Metadata struct {
	sync.RWMutex // we need to ensure only a thread can access to `metaDB` at a time
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc

	// cache
	tasks map[string]*pb.TaskMeta
	logs  []*pb.TaskLog

	// task operation log
	log *Logger

	dir string
	db  *leveldb.DB
}

// NewMetadata returns a metadata object
func NewMetadata(dir string, db *leveldb.DB) (*Metadata, error) {
	meta := &Metadata{
		dir: dir,
		db:  db,
		log: new(Logger),
	}

	// restore from old metadata
	oldPath := path.Join(dir, "meta")
	err := meta.tryToRecoverMetaFromOldFashion(oldPath)
	if err != nil {
		log.Errorf("[worker metadata]fail to recover from old metadata file %s, meta file may be corrupt, error message: %v", oldPath, err)
		return nil, errors.Trace(err)
	}

	err = meta.loadFromDB()
	if err != nil {
		return nil, errors.Trace(err)
	}

	meta.ctx, meta.cancel = context.WithCancel(context.Background())

	meta.wg.Add(1)
	go func() {
		defer meta.wg.Done()
		meta.log.GC(meta.ctx, meta.db)
	}()

	return meta, nil
}

// Close closes meta DB
func (meta *Metadata) Close() {
	if meta.cancel != nil {
		meta.cancel()
	}

	meta.wg.Wait()
}

// LoadTaskMeta returns meta of all tasks
func (meta *Metadata) LoadTaskMeta() map[string]*pb.TaskMeta {
	meta.Lock()
	defer meta.Unlock()

	tasks := make(map[string]*pb.TaskMeta, len(meta.tasks))

	for name, task := range meta.tasks {
		tasks[name] = CloneTaskMeta(task)
	}

	return tasks
}

// GetTask returns task meta by given name
func (meta *Metadata) GetTask(name string) (task *pb.TaskMeta) {
	meta.RLock()
	t, ok := meta.tasks[name]
	if ok {
		task = CloneTaskMeta(t)
	}
	meta.RUnlock()

	return
}

// GetTaskLog returns task log by give log ID
func (meta *Metadata) GetTaskLog(opLogID int64) (*pb.TaskLog, error) {
	log, err := meta.log.GetTaskLog(meta.db, opLogID)
	return log, errors.Trace(err)
}

// PeekLog returns first need to be handled task log
func (meta *Metadata) PeekLog() (log *pb.TaskLog) {
	meta.RLock()
	if len(meta.logs) > 0 {
		log = CloneTaskLog(meta.logs[0])
	}
	meta.RUnlock()

	return
}

// AppendOperation appends operation into task log
func (meta *Metadata) AppendOperation(subTask *pb.TaskMeta) (int64, error) {
	meta.Lock()
	defer meta.Unlock()

	opLog := &pb.TaskLog{
		Task: CloneTaskMeta(subTask),
	}

	if err := meta.log.Append(meta.db, opLog); err != nil {
		return 0, errors.Trace(err)
	}

	meta.logs = append(meta.logs, opLog)
	return opLog.Id, nil
}

// MarkOperation marks operation result
func (meta *Metadata) MarkOperation(log *pb.TaskLog) error {
	meta.Lock()
	defer meta.Unlock()

	if len(meta.logs) == 0 {
		return errors.NotFoundf("any task operation log")
	}

	if meta.logs[0].Id != log.Id {
		return errors.Errorf("please handle task operation order by log ID, the log need to be handled is %+v, not %+v", meta.logs[0], log)
	}

	txn, err := meta.db.OpenTransaction()
	if err != nil {
		return errors.Trace(err)
	}

	err = meta.log.MarkAndForwardLog(txn, log)
	if err != nil {
		txn.Discard()
		return errors.Trace(err)
	}

	if log.Success {
		if log.Task.Op == pb.TaskOp_Stop {
			err = DeleteTaskMeta(txn, log.Task.Name)
		} else {
			err = SetTaskMeta(txn, log.Task)
		}
		if err != nil {
			txn.Discard()
			return errors.Trace(err)
		}
	}

	err = txn.Commit()
	if err != nil {
		return errors.Trace(err)
	}

	if log.Success {
		if log.Task.Op == pb.TaskOp_Stop {
			delete(meta.tasks, log.Task.Name)
		} else {
			meta.tasks[log.Task.Name] = log.Task
		}
	}
	meta.logs = meta.logs[1:]
	return nil
}

func (meta *Metadata) loadFromDB() (err error) {
	meta.logs, err = meta.log.Initial(meta.db)
	if err != nil {
		return errors.Trace(err)
	}

	meta.tasks, err = LoadTaskMetas(meta.db)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// to be compatible with the old fashion meta
func (meta *Metadata) tryToRecoverMetaFromOldFashion(path string) error {
	_, err := os.Stat(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return errors.Trace(err)
		}
		return nil
	}

	// old metadata file exists, recover metadata from it
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return errors.Annotatef(err, "read old metadata file %s", path)
	}

	oldMeta := &Meta{}
	err = oldMeta.Decode(string(data))
	if err != nil {
		return errors.Annotatef(err, "decode old metadata file %s", path)
	}

	log.Infof("[worker metadata] find %d tasks from old metadata file", len(oldMeta.SubTasks))

	for name, task := range oldMeta.SubTasks {
		log.Infof("[worker metadata] from old metadata file: subtask %s => %+v", name, task)
		var b bytes.Buffer
		enc := toml.NewEncoder(&b)
		err = enc.Encode(task)
		if err != nil {
			return errors.Annotatef(err, "encode task %v", task)
		}

		taskMeta := &pb.TaskMeta{
			Name:  name,
			Op:    pb.TaskOp_Start,
			Stage: pb.Stage_New,
			Task:  b.Bytes(),
		}

		err := SetTaskMeta(meta.db, taskMeta)
		if err != nil {
			return errors.Errorf("fail to set task meta %s error message: %v", taskMeta.Name, err)
		}
	}

	return errors.Trace(os.Remove(path))
}
