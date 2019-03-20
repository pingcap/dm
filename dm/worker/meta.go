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
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/errors"
)

// Meta information contains
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

	return nil
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

	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, errors.Annotatef(err, "create meta directory")
	}

	fd, err := os.Open(metaDB.path)
	if os.IsNotExist(err) {
		log.Warnf("failed to open meta file %s, going to create a new one: %v", metaDB.path, err)
		return metaDB, nil
	} else if err != nil {
		return nil, errors.Trace(err)
	}
	defer fd.Close()

	err = metaDB.meta.DecodeFile(metaDB.path)
	if err != nil {
		return metaDB, errors.Trace(err)
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
	serialized, err := metaDB.meta.Toml()
	if err != nil {
		return errors.Trace(err)
	}

	if err := ioutil.WriteFile(metaDB.path, []byte(serialized), 0644); err != nil {
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
