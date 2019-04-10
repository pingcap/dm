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

// Decode loads config from file data
func (m *Meta) Decode(data string) error {
	_, err := toml.Decode(data, m)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// Metadata stores metadata and log of task
// it also provides logger feature
// * append log
// * forward to specified log location
type Metadata struct {
	sync.Mutex // we need to ensure only a thread can access to `metaDB` at a time

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
	}

	// restore from old metadata
	oldPath = path.Join(dir, "meta")
	fd, err := os.Open(oldPath)
	// old metadata file exists, recover metadata from it
	if err == nil {
		fd.Close()
		err = meta.recoverMetaFromOldFashion(oldPath)
		if err != nil {
			log.Errorf("fail to recover meta from old metadata file %s, meta file may be correuption, error message: %v", oldPath, err)
			return nil, errors.Trace(err)
		}
		return meta, nil
	}
	// old metadata file exists, fail to open old metadata file
	if !os.IsNotExist(err) {
		return nil, errors.Trace(err)
	}

	err = meta.loadFromDB()
	if err != nil {
		return nil, errors.Trace(err)
	}

	return meta, nil
}

// Close closes meta DB
func (meta *Metadata) Close() error {
	metaDB.Lock()
	defer metaDB.Unlock()

	return errors.Trace(metaDB.db.Close())
}

// LoadTaskMeta returns meta of all tasks
func (meta *Metadata) LoadTaskMeta() *pb.WorkMeta {
	meta.Lock()
	defer meta.Unlock()

	meta := &pb.WorkMeta{
		Tasks: make(map[string]*pb.TaskMeta),
	}

	for name, task := range metaDB.meta.Tasks {
		meta.Tasks[name] = &pb.TaskMeta{
			Op:   task.Op,
			Name: task.Name,
			Task: task.Task,
		}
	}

	return meta
}

// GetTask returns task meta by given name
func (meta *Metadata) GetTask(name string) *pb.TaskMeta {
	metaDB.Lock()
	defer metaDB.Unlock()

	task, ok := meta.tasks[name]
	if !ok {
		return nil
	}

	return &pb.TaskMeta{
		Op:   task.Op,
		Name: task.Name,
		Task: task.Task,
	}
}

// SetTask sets task metadata in db
func (meta *Metadata) SetTask(subTask *pb.TaskMeta) error {
	meta.Lock()
	defer meta.Unlock()

	meta.tasks[subTask.Name] = subTask
	// todo: save in leveldb
	return nil
}

// DeleteTask deletes task meta in db
func (meta *Metadata) DeleteTask(name string) error {
	meta.Lock()
	defer meta.Unlock()

	delete(meta.tasks, name)
	// todo: delete from leveldb
	return nil
}

func (meta *Metadata) loadFromDB() error {
	err := meta.handlePointer.Load(meta.db)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// to be compatible with the old fashion meta
func (meta *Metadata) recoverMetaFromOldFashion(path string) error {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, errors.Annotatef(err, "read old metadata file %s", path)
	}

	oldMeta := &Meta{}
	err = oldMeta.Decode(string(data))
	if err != nil {
		return errors.Annotatef(err, "decode old metadata file %s", path)
	}

	log.Infof("find %d tasks from old metadata file", len(oldMeta.SubTasks))

	batch := new(leveldb.Batch)
	for name, task := range oldMeta.SubTasks {
		log.Infof("[old metadata file] subtask %s => %+v", name, task)
		var b bytes.Buffer
		enc := toml.NewEncoder(&b)
		err := enc.Encode(task)
		if err != nil {
			return errors.Annotatef(err1, "encode task %v", task)
		}

		taskMeta := &pb.TaskMeta{
			Name: name,
			Op:   pb.TaskOp_Start,
			Stage, pb.Stage_New,
			Task: b.Bytes(),
		}

		taskByte, err := taskMeta.Marshal()
		if err != nil {
			return errors.Annotatef(err1, "encode task meta %v", task)
		}

		batch.Put([]byte(name), metaByte)
		meta.tasks[name] = taskMeta
	}

	err = metaDB.db.Write(batch, nil)
	if err != nil {
		return errors.Annotatef(err, "save task meta into kv db")
	}

	return errors.Trace(os.Remove(path))
}
