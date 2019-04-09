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
	"github.com/syndtr/goleveldb/leveldb/opt"
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

// Metadata stores metadata of task
type Metadata struct {
	lock  sync.Mutex                        // we need to ensure only a thread can access to `metaDB` at a time
	tasks map[string]*pb.TaskMeta           // name: task
	logs  map[string]map[int64]*pb.TaskMeta // name: [log id: task]

	dir string
	db  *leveldb.DB
}

// NewMetaDB returns a meta file db
func NewMetaDB(dir string) (*FileMetaDB, error) {
	metaDB := &FileMetaDB{
		dir:   dir,
		tasks: make(map[string]*pb.TaskMeta),
		logs:  make(map[string]map[int64]*pb.TaskMeta),
	}

	kvDir := path.Join(dir, "kv")
	metadata, err := openMetadataDB(kvDir, defaultKVConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// restore from old metadata
	oldPath = path.Join(dir, "meta")
	fd, err := os.Open(oldPath)
	if os.IsNotExist(err) {
		return metaDB, nil
	} else if err != nil {
		return nil, errors.Trace(err)
	}
	fd.Close()

	err1 = metaDB.recoverMetaFromOldFashion(oldPath)
	if err != nil {
		log.Errorf("fail to recover meta from old metadata file %s, meta file may be correuption, error message: %v", oldPath, err)
		return nil, errors.Trace(err)
	}

	return metaDB, nil
}

// Close closes meta DB
func (metaDB *Metadata) Close() error {
	metaDB.lock.Lock()
	defer metaDB.lock.Unlock()

	return errors.Trace(metaDB.db.Close())
}

// Load returns work meta
func (metaDB *Metadata) Load() *pb.WorkMeta {
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
func (metaDB *Metadata) Get(name string) *pb.TaskMeta {
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
func (metaDB *Metadata) Set(subTask *pb.TaskMeta) error {
	metaDB.lock.Lock()
	defer metaDB.lock.Unlock()

	metaDB.meta.Tasks[subTask.Name] = subTask
	return errors.Trace(metaDB.save())
}

// Delete deletes task information in Meta
func (metaDB *Metadata) Delete(name string) error {
	metaDB.lock.Lock()
	defer metaDB.lock.Unlock()

	delete(metaDB.meta.Tasks, name)
	return errors.Trace(metaDB.save())
}

func (metaDB *Metadata) recoverMetaFromOldFashion(path string) error {
	oldMeta := &Meta{}

	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, errors.Annotatef(err, "read old metadata file %s", path)
	}

	err = oldMeta.Decode(string(data))
	if err != nil {
		return errors.Annotatef(err, "decode old metadata file %s", path)
	}

	batch := new(leveldb.Batch)
	for name, task := range oldMeta.SubTasks {
		var b bytes.Buffer
		enc := toml.NewEncoder(&b)
		err := enc.Encode(task)
		if err != nil {
			return errors.Annotatef(err1, "encode task %v", task)
		}

		meta := &pb.TaskMeta{
			Name: name,
			Op:   pb.TaskOp_Start,
			Stage, pb.Stage_New,
			Task: b.Bytes(),
		}
		metaByte, err := meta.Marshal()
		if err != nil {
			return errors.Annotatef(err1, "encode task meta %v", task)
		}

		batch.Put([]byte(name), metaByte)
	}

	err = metaDB.db.Write(batch, nil)
	if err != nil {
		return errors.Annotatef(err, "save task meta into kv db")
	}

	return nil
}

// KVConfig is the configuration of goleveldb
type KVConfig struct {
	BlockCacheCapacity            int     `toml:"block-cache-capacity" json:"block-cache-capacity"`
	BlockRestartInterval          int     `toml:"block-restart-interval" json:"block-restart-interval"`
	BlockSize                     int     `toml:"block-size" json:"block-size"`
	CompactionL0Trigger           int     `toml:"compaction-L0-trigger" json:"compaction-L0-trigger"`
	CompactionTableSize           int     `toml:"compaction-table-size" json:"compaction-table-size"`
	CompactionTotalSize           int     `toml:"compaction-total-size" json:"compaction-total-size"`
	CompactionTotalSizeMultiplier float64 `toml:"compaction-total-size-multiplier" json:"compaction-total-size-multiplier"`
	WriteBuffer                   int     `toml:"write-buffer" json:"write-buffer"`
	WriteL0PauseTrigger           int     `toml:"write-L0-pause-trigger" json:"write-L0-pause-trigger"`
	WriteL0SlowdownTrigger        int     `toml:"write-L0-slowdown-trigger" json:"write-L0-slowdown-trigger"`
}

// default leveldb config
var defaultKVConfig = &KVConfig{
	BlockCacheCapacity:            8388608,
	BlockRestartInterval:          16,
	BlockSize:                     4096,
	CompactionL0Trigger:           8,
	CompactionTableSize:           67108864,
	CompactionTotalSize:           536870912,
	CompactionTotalSizeMultiplier: 8,
	WriteBuffer:                   67108864,
	WriteL0PauseTrigger:           24,
	WriteL0SlowdownTrigger:        17,
}

func openMetadataDB(kvDir string, config *KVConfig) (*leveldb.DB, error) {
	log.Infof("metadat kvdbconfig: %+v", config)

	var opt opt.Options
	opt.BlockCacheCapacity = config.BlockCacheCapacity
	opt.BlockRestartInterval = config.BlockRestartInterval
	opt.BlockSize = config.BlockSize
	opt.CompactionL0Trigger = config.CompactionL0Trigger
	opt.CompactionTableSize = config.CompactionTableSize
	opt.CompactionTotalSize = config.CompactionTotalSize
	opt.CompactionTotalSizeMultiplier = config.CompactionTotalSizeMultiplier
	opt.WriteBuffer = config.WriteBuffer
	opt.WriteL0PauseTrigger = config.WriteL0PauseTrigger
	opt.WriteL0SlowdownTrigger = config.WriteL0SlowdownTrigger

	return leveldb.OpenFile(kvDir, &opt)
}
