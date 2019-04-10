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

// Putter is interface which has Put method
type Putter interface {
	Put(key, value []byte, opts *opt.WriteOptions) error
}

// Deleter is interface which has Delete method
type Deleter interface {
	Delete(key []byte, wo *opt.WriteOptions) error
}

// Getter is interface which has Get method
type Getter interface {
	Get(key []byte, ro *opt.ReadOptions) ([]byte, error)
}

// HandledPointerKey is key of HandledPointer which point to the last handled log
var HandledPointerKey = []byte("!DM!handledPointer")

// Pointer is a logic pointer that point to a location of log
type Pointer struct {
	Location int64
}

// MarshalBinary never return not nil err now
func (p *Pointer) MarshalBinary() []byte {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, uint64(p.Location))

	return data
}

// UnmarshalBinary implement encoding.BinaryMarshal
func (p *Pointer) UnmarshalBinary(data []byte) error {
	if len(data) < 8 {
		return errors.Errorf("not enough data as pointer %v", data)
	}

	p.Location = int64(binary.LittleEndian.Uint64(data))
	return nil
}

// LoadHandledPointer loads handled pointer value from kv DB
func LoadHandledPointer(db *leveldb.DB) (Pointer, error) {
	var p Pointer
	value, err := db.Get(HandledPointerKey, nil)
	if err != nil {
		// return zero value when not found
		if err == leveldb.ErrNotFound {
			return p, nil
		}

		return p, errors.Trace(err)
	}

	err = p.UnmarshalBinary(value)
	if err != nil {
		return p, errors.Trace(err)
	}

	return p, nil
}

var (
	defaultMaxLogEntries = 10 * 1024
	defaultGCForwardTime = 72 * time.Hour

	// TaskLogPrefix is  prefix of task log key
	TaskLogPrefix = []byte("!DM!TaskLog")
)

// DecodeTaskLogKey decodes task log key and returns its log ID
func DecodeTaskLogKey(key []byte) (int64, error) {
	if len(key) != len(TaskLogPrefix)+8 {
		return 0, errors.Errorf("not enough data as task log key %v", key)
	}

	return int64(binary.LittleEndian.Uint64(key[len(TaskLogPrefix):])), nil
}

// EncodeTaskLogKey encodes log ID into a task log key
func EncodeTaskLogKey(id int64) []byte {
	key := make([]byte, 8+len(TaskLogPrefix))
	copy(key[:len(TaskLogPrefix)], TaskLogPrefix)
	binary.LittleEndian.PutUint64(key[len(TaskLogPrefix):], uint64(id))

	return key
}

// Logger manage task operation logs
type Logger struct {
	endPointer     Pointer
	handledPointer Pointer
}

// Initial initials Logger
func (log *Logger) Initial(db *leveldb.DB) ([]*pb.TaskLog, error) {
	handledPointer, err := LoadHandledPointer(db)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var (
		endPointer Pointer
		logs       = make([]*pb.TaskLog, 0, 4)
		logID      int64
	)
	iter := db.NewIterator(nil, nil)
	for ok := iter.Seek(EncodeTaskLogKey(handledPointer.Location)); ok; ok = iter.Next() {
		logBytes := iter.Value()
		log := &pb.TaskLog{}
		err = log.Unmarshal(logBytes)
		if err != nil {
			err = errors.Annotatef(err, "unmarshal task meta %s", logBytes)
			break
		}

		logID, err = DecodeTaskLogKey(iter.Key())
		if err != nil {
			break
		}
		if logID > endPointer.Location {
			endPointer.Location = logID
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

	log.handledPointer = handledPointer
	log.endPointer = endPointer

	return logs, nil
}

// ForwardTo forward handled pointer to specified ID location
// not thread safe
func (log *Logger) ForwardTo(db Putter, ID int64) error {
	handledPointer := Pointer{
		Location: ID,
	}

	err := db.Put(HandledPointerKey, handledPointer.MarshalBinary(), nil)
	if err != nil {
		return errors.Trace(err)
	}

	log.handledPointer = handledPointer
	return nil
}

// Append appends a task log
func (log *Logger) Append(db Putter, opLog *pb.TaskLog) error {
	var id int64
	for {
		id = atomic.LoadInt64(&log.endPointer.Location)
		if atomic.CompareAndSwapInt64(&log.endPointer.Location, id, id+1) {
			break
		}
	}

	opLog.Id = id
	opLog.Ts = time.Now().UnixNano()
	logBytes, err := opLog.Marshal()
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

// DecodeTaskMetaKey decodes task meta key and returns task name
func DecodeTaskMetaKey(key []byte) string {
	return string(key[len(TaskMetaPrefix):])
}

// EncodeTaskMetaKey encodes take name into a task meta key
func EncodeTaskMetaKey(name string) []byte {
	key := append([]byte{}, TaskMetaPrefix...)
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

	err = h.Put(EncodeTaskMetaKey(task.Name), taskBytes, nil)
	if err != nil {
		return errors.Annotatef(err, "save into kv db")
	}

	return nil
}

// GetTaskMeta returns task meta by given name
func (m *Metadata) GetTaskMeta(h Getter, name string) (*pb.TaskMeta, error) {
	taskBytes, err := h.Get(EncodeTaskMetaKey(name), nil)
	if err != nil {
		return nil, errors.Annotatef(err, "get task meta from leveldb")
	}

	task := &pb.TaskMeta{}
	err = task.Unmarshal(taskBytes)
	if err != nil {
		return nil, errors.Annotatef(err, "unmarshal binary %s", taskBytes)
	}

	return task, nil
}

// DeleteByTxn delete task meta from kv DB
func (m *Metadata) DeleteByTxn(h Deleter, name string) error {
	err := h.Delete(EncodeTaskMetaKey(name), nil)
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

	if task.Op == pb.TaskOp_InvalidOp {
		return errors.NotValidf("task operation")
	}

	return nil
}
