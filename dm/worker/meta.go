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

// FileMetaDB stores meta information in disk
type FileMetaDB struct {
	lock sync.Mutex // we need to ensure only a thread can access to `metaDB` at a time
	meta *pb.WorkMeta
	path string
}

// NewFileMetaDB returns a meta file db
func NewFileMetaDB(dir string) (*FileMetaDB, error) {
	metaDB := &FileMetaDB{
		path: path.Join(dir, "meta"),
		meta: &pb.WorkMeta{
			Tasks: make(map[string]*pb.TaskMeta),
		},
	}

	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, errors.Annotatef(err, "create meta directory %s", dir)
	}

	fd, err := os.Open(metaDB.path)
	if os.IsNotExist(err) {
		log.Warnf("failed to open meta file %s, going to create a new one: %v", metaDB.path, err)
		return metaDB, nil
	} else if err != nil {
		return nil, errors.Trace(err)
	}
	fd.Close()

	bs, err := ioutil.ReadFile(metaDB.path)
	if err != nil {
		return nil, errors.Annotatef(err, "read meta file %s", metaDB.path)
	}

	if err = metaDB.meta.Unmarshal(bs); err != nil {
		// try to decode it using old toml definition
		if err1 := metaDB.recoverMetaFromOldFashion(bs); err1 != nil {
			log.Errorf("fail to recover meta from old fashion, meta file may be correuption, error message: %v", err1)
			return nil, errors.Trace(err)
		}
	}

	return metaDB, nil
}

// Close closes meta DB
func (metaDB *FileMetaDB) Close() error {
	metaDB.lock.Lock()
	defer metaDB.lock.Unlock()

	return errors.Trace(metaDB.save())
}

func (metaDB *FileMetaDB) save() error {
	serialized, err := metaDB.meta.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	if err := ioutil.WriteFile(metaDB.path, serialized, 0644); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (metaDB *FileMetaDB) recoverMetaFromOldFashion(data []byte) error {
	oldMeta := &Meta{}

	if err := oldMeta.Decode(string(data)); err != nil {
		return err
	}

	for name, task := range oldMeta.SubTasks {
		var b bytes.Buffer
		enc := toml.NewEncoder(&b)
		if err1 := enc.Encode(task); err1 != nil {
			return errors.Annotatef(err1, "encode task %v", task)
		}

		metaDB.meta.Tasks[name] = &pb.TaskMeta{
			Name: name,
			Task: b.Bytes(),
		}
	}

	return errors.Trace(metaDB.save())
}

// Load returns work meta
func (metaDB *FileMetaDB) Load() *pb.WorkMeta {
	metaDB.lock.Lock()
	defer metaDB.lock.Unlock()

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

// Get returns task meta by given name
func (metaDB *FileMetaDB) Get(name string) *pb.TaskMeta {
	metaDB.lock.Lock()
	defer metaDB.lock.Unlock()

	task, ok := metaDB.meta.Tasks[name]
	if !ok {
		return nil
	}

	return &pb.TaskMeta{
		Op:   task.Op,
		Name: task.Name,
		Task: task.Task,
	}
}

// Set sets task information in Meta
func (metaDB *FileMetaDB) Set(subTask *pb.TaskMeta) error {
	metaDB.lock.Lock()
	defer metaDB.lock.Unlock()

	metaDB.meta.Tasks[subTask.Name] = subTask
	return errors.Trace(metaDB.save())
}

// Delete deletes task information in Meta
func (metaDB *FileMetaDB) Delete(name string) error {
	metaDB.lock.Lock()
	defer metaDB.lock.Unlock()

	delete(metaDB.meta.Tasks, name)
	return errors.Trace(metaDB.save())
}
