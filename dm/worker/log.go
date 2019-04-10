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
	"encoding/binary"
	"sync/atomic"
	"time"

	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type Putter interface {
	Put(key, value []byte, opts *opt.WriteOptions) error
}

type Deleter interface {
	Delete(key []byte, wo *opt.WriteOptions) error
}

type Getter interface {
	Get(key []byte, ro *opt.ReadOptions) ([]byte, error)
}

// HandlePointerKey is key of HandlePointerKey we should start to handle operation in log
var HandlePointerKey = []byte("!DM!handlePointer")

type Pointer struct {
	Offset int64
}

// MarshalBinary never return not nil err now
func (p *Pointer) MarshalBinary() (data []byte, err error) {
	data = make([]byte, 8)
	binary.LittleEndian.PutUint64(data, uint64(p.offset))

	return
}

// UnmarshalBinary implement encoding.BinaryMarshal
func (p *Pointer) UnmarshalBinary(data []byte) error {
	if len(data) < 8 {
		return errors.New("not enough data as pointer %v", data)
	}

	p.Offset = int64(binary.LittleEndian.Uint64(data))
	return nil
}

// LoadHandlePointer loads handle pointer value from kv DB
func LoadHandlePointer(db *leveldb) (Pointer, error) {
	value, err := db.Get(HandlePointerKey, nil)
	if err != nil {
		// return zero value when not found
		if err == leveldb.ErrNotFound {
			return nil, nil
		}

		return nil, errors.Trace(err)
	}

	p := Pointer{}
	err = p.UnmarshalBinary(value)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return p, nil
}

var (
	defaultMaxLogEntries = 10 * 1024
	defaultGCForwardTime = 72 * time.Hour

	// TaskLogPrefix is  prefix of task log key
	TaskLogPrefix = []byte("!DM!TaskLog")
)

// DecodeTaskLogKey decodes task operation log key and return log ID
func DecodeTaskLogKey(key []byte) int64 {
	idBytes := key[len(TaskLogPrefix):]
	return int64(binary.LittleEndian.Uint64(idBytes))
}

// EncodeTaskLogKey encodes task operation log key using given task name
func EncodeTaskLogKey(id int64) []byte {
	key := make([]byte, 8+len(TaskLogPrefix))

	copy(key[:len(TaskLogPrefix)], TaskLogPrefix)
	binary.LittleEndian.PutUint64(key[len(TaskLogPrefix):], uint64(id))

	return key
}

// Logger manage task operation logs
type Logger struct {
	endPointer    Pointer
	handlePointer Pointer
}

// Initial initials Logger
func (log *Logger) Initial(db *leveldb.DB) ([]*TaskLog, error) {
	handlePointer, err := LoadHandlePointer(db)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var (
		endPointer Pointer
		logs       = make([]*pb.TaskLog, 0, 4)
	)
	iter := db.NewIterator(nil, nil)
	for ok := iter.Seek(EncodeTaskLogKey(handlePointer.Offset)); ok; ok = iter.Next() {
		logBytes := iter.Value()
		log := &pb.TaskLog{}

		err = log.Unmarshal(logBytes)
		if err != nil {
			err = errors.Annotatef(err, "unmarshal task meta %s", logBytes)
		}

		logID := DecodeTaskLogKey(iter.key())
		if logID > endPointer.Offset {
			endPointer.Offset = logID
		} else {
			panic("out of sorted from level db")
		}

		logs = append(logs, log)
	}

	iter.Release()
	if err == nil {
		return nil, errors.Trace(err)
	}

	err = iter.Error()
	if err != nil {
		return nil, errors.Annotatef(err, "fetch logs from meta")
	}

	log.handlePointer = handlePointer
	log.endPointer = endPointer

	return logs, nil
}

// not thread safe
func (log *Logger) ForwardTo(db Putter, ID int64) error {
	handlePointer := &Pointer{
		Offset: ID,
	}

	handlePointerBytes, err := handlePointer.MarshalBinary()
	if err != nil {
		return errors.Trace(err)
	}

	err = db.Put(HandlePointerKey, handlePointerBytes, nil)
	if err != nil {
		return errors.Trace(err)
	}

	log.handlePointer = HandlePointerKey
	return nil
}

func (log *Logger) Append(db Putter, log *TaskLog) error {
	var id int
	for {
		id = atomic.LoadInt64(&log.endPointer.Offset)
		if atomic.CompareAndSwapInt64(&log.endPointer.Offset, id, id+1) {
			break
		}
	}

	log.Id = id
	log.Ts = time.Now().UnixNano()
	logBytes, err := log.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	err = db.Put(EncodeTaskLogKey(id), logBytes, nil)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// TaskMetaPrefix is prefix of task meta key
var TaskMetaPrefix = []byte("!DM!TaskMeta")

// DecodeTaskMetaKey decode task name from task meta key
func DecodeTaskMetaKey(key []byte) string {
	return string(key[len(TaskMetaPrefix):])
}

// EncodeTaskMetaKey enable task meta key using given task name
func EncodeTaskMetaKey(name string) []byte {
	key := append([]byte, TaskMetaPrefix...)
	return append(key, name...)
}

// SetTaskMeta saves task meta into kv db
func (m *Metadata) SetTaskMeta(h Putter, task *pb.TaskMeta) error {
	err := verifyTaskMeta(task)
	if err != nil {
		return errors.Trace(err)
	}

	taskBytes, err := task.Marshal()
	if err != nil {
		return errors.Annotatef(err, "marshal task %+v", task)
	}

	err = h.Put(EncodeTaskMetaKey(task.Name), taskBytes)
	if err != nil {
		return errors.Annotatef(err, "save into kv db")
	}

	return nil
}

// GetTaskMeta returns task meta by given name
func (m *Metadata) GetTaskMeta(h Getter, name string) error {
	taskBytes, err := h.Get(EncodeTaskMetaKey(task.Name), nil)
	if err != nil {
		return errors.Annotatef(err, "get task meta from leveldb")
	}

	task := &pb.TaskMeta{}
	err = task.UnmarshalBinary(taskBytes)
	if err != nil {
		return errors.Annotatef(err, "unmarshal binary %s", taskBytes)
	}

	return task
}

// DeleteByTxn delete task meta from kv DB
func (t *TaskMetas) DeleteByTxn(h Deleter, name string) error {
	err := verifyTaskMeta(task)
	if err != nil {
		return errors.Trace(err)
	}

	err = h.Delete(EncodeTaskMetaKey(task.Name))
	if err != nil {
		return errors.Annotatef(err, "save into kv db")
	}

	return nil
}

func verifyTaskMeta(task *pb.TaskMeta) error {
	if len(task.Name) == 0 {
		return errors.NotValidf("task name is empty")
	}

	if len(task.Task) == 0 {
		return errors.NotValidf("task config is empty")
	}

	if task.Stage == pb.Stage_InvalidStage {
		return errors.NotValidf("stage")
	}

	if task.Stage == pb.TaskOp_InvalidOp {
		return errors.NotValidf("task operation")
	}

	return nil
}
