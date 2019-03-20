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
	"io/ioutil"
	"path"
	"sync"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/errors"
)

// Meta information contains
// * sub-task
type Meta struct {
	SubTasks map[string]*config.SubTaskConfig `json:"sub-tasks"`
}

// FileMetaDB stores meta information in disk
type FileMetaDB struct {
	lock sync.Mutex // we need to ensure only a thread can access to `metaDB` at a time
	meta *Meta
	path string
}

// NewFileMetaDB return a meta file db
func NewFileMetaDB(dir string) (*FileMetaDB, error) {
	metaDB := &FileMetaDB{
		path: path.Join(dir, "meta"),
		meta: &Meta{
			SubTasks: make(map[string]*config.SubTaskConfig),
		},
	}
	// ignore all errors -- file maybe not created yet (and it is fine).
	content, err := ioutil.ReadFile(metaDB.path)
	if err == nil {
		err1 := json.Unmarshal(content, metaDB.meta)
		if err1 != nil {
			return nil, errors.Annotatef(err1, "decode meta %s", content)
		}
	} else {
		log.Warnf("failed to open meta file %s, going to create a new one: %v", dir, err)
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
	serialized, err := json.Marshal(metaDB.meta)
	if err != nil {
		return errors.Trace(err)
	}
	if err := ioutil.WriteFile(metaDB.path, serialized, 0644); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Get returns `Meta` object
func (metaDB *FileMetaDB) Get() *Meta {
	metaDB.lock.Lock()
	defer metaDB.lock.Unlock()

	meta := &Meta{
		SubTasks: make(map[string]*config.SubTaskConfig),
	}

	for name, task := range metaDB.meta.SubTasks {
		meta.SubTasks[name] = task
	}

	return meta
}

// Set sets subtask in Meta
func (metaDB *FileMetaDB) Set(subTask *config.SubTaskConfig) error {
	metaDB.lock.Lock()
	defer metaDB.lock.Unlock()

	metaDB.meta.SubTasks[subTask.Name] = subTask

	return errors.Trace(metaDB.save())
}

// Del deletes subtask in Meta
func (metaDB *FileMetaDB) Del(name string) error {
	metaDB.lock.Lock()
	defer metaDB.lock.Unlock()

	delete(metaDB.meta.SubTasks, name)

	return errors.Trace(metaDB.save())
}
