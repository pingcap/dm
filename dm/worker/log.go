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
	"encoding/binary"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// ErrInValidHandler indicates we meet an invalid Putter/Getter/Deleter
var ErrInValidHandler = errors.New("handler is nil, please pass a leveldb.DB or leveldb.Transaction")

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
func (p *Pointer) MarshalBinary() ([]byte, error) {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, uint64(p.Location))

	return data, nil
}

// UnmarshalBinary implement encoding.BinaryMarshal
func (p *Pointer) UnmarshalBinary(data []byte) error {
	if len(data) != 8 {
		return errors.Errorf("not valid length data as pointer % X", data)
	}

	p.Location = int64(binary.LittleEndian.Uint64(data))
	return nil
}

// LoadHandledPointer loads handled pointer value from kv DB
func LoadHandledPointer(db *leveldb.DB) (Pointer, error) {
	var p Pointer
	if db == nil {
		return p, errors.Trace(ErrInValidHandler)
	}

	value, err := db.Get(HandledPointerKey, nil)
	if err != nil {
		// return zero value when not found
		if err == leveldb.ErrNotFound {
			return p, nil
		}

		return p, errors.Annotatef(err, "fetch handled pointer")
	}

	err = p.UnmarshalBinary(value)
	if err != nil {
		return p, errors.Annotatef(err, "unmarshal handle pointer % X", value)
	}

	return p, nil
}

var (
	defaultGCForwardLog int64 = 10000

	// TaskLogPrefix is  prefix of task log key
	TaskLogPrefix = []byte("!DM!TaskLog")
)

// DecodeTaskLogKey decodes task log key and returns its log ID
func DecodeTaskLogKey(key []byte) (int64, error) {
	if len(key) != len(TaskLogPrefix)+8 {
		return 0, errors.Errorf("not valid length data as task log key % X", key)
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
func (logger *Logger) Initial(db *leveldb.DB) ([]*pb.TaskLog, error) {
	if db == nil {
		return nil, errors.Trace(ErrInValidHandler)
	}

	handledPointer, err := LoadHandledPointer(db)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var (
		endPointer = Pointer{
			Location: handledPointer.Location + 1,
		}
		logs = make([]*pb.TaskLog, 0, 4)
	)
	iter := db.NewIterator(util.BytesPrefix(TaskLogPrefix), nil)
	startLocation := handledPointer.Location + 1
	for ok := iter.Seek(EncodeTaskLogKey(startLocation)); ok; ok = iter.Next() {
		logBytes := iter.Value()
		opLog := &pb.TaskLog{}
		err = opLog.Unmarshal(logBytes)
		if err != nil {
			err = errors.Annotatef(err, "unmarshal task log % X", logBytes)
			break
		}

		if opLog.Id >= endPointer.Location {
			// move to next location
			endPointer.Location = opLog.Id + 1
			logs = append(logs, opLog)
		} else {
			panic(fmt.Sprintf("out of sorted order from level db for task log key % X (log ID %d)", iter.Key(), opLog.Id))
		}
	}
	iter.Release()
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = iter.Error()
	if err != nil {
		return nil, errors.Annotatef(err, "fetch logs from meta")
	}

	logger.handledPointer = handledPointer
	logger.endPointer = endPointer

	log.Infof("[task log] initialized, handle pointer %+v, end pointer %+v", logger.handledPointer, logger.endPointer)

	return logs, nil
}

// GetTaskLog returns task log by given log ID
func (logger *Logger) GetTaskLog(h Getter, id int64) (*pb.TaskLog, error) {
	if whetherNil(h) {
		return nil, errors.Trace(ErrInValidHandler)
	}

	logBytes, err := h.Get(EncodeTaskLogKey(id), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, nil
		}
		return nil, errors.Annotatef(err, "get task log %d from leveldb", id)
	}

	opLog := &pb.TaskLog{}
	err = opLog.Unmarshal(logBytes)
	if err != nil {
		return nil, errors.Annotatef(err, "unmarshal task log binary % X", logBytes)
	}

	return opLog, nil
}

// ForwardTo forward handled pointer to specified ID location
// not thread safe
func (logger *Logger) ForwardTo(db Putter, ID int64) error {
	if whetherNil(db) {
		return errors.Trace(ErrInValidHandler)
	}

	handledPointer := Pointer{
		Location: ID,
	}

	handledPointerBytes, _ := handledPointer.MarshalBinary()

	err := db.Put(HandledPointerKey, handledPointerBytes, nil)
	if err != nil {
		return errors.Annotatef(err, "forward handled pointer to %d", ID)
	}

	logger.handledPointer = handledPointer
	return nil
}

// MarkAndForwardLog marks result sucess or not in log, and forwards handledPointer
func (logger *Logger) MarkAndForwardLog(db Putter, opLog *pb.TaskLog) error {
	if whetherNil(db) {
		return errors.Trace(ErrInValidHandler)
	}

	logBytes, err := opLog.Marshal()
	if err != nil {
		return errors.Annotatef(err, "marshal task log %+v", opLog)
	}

	err = db.Put(EncodeTaskLogKey(opLog.Id), logBytes, nil)
	if err != nil {
		return errors.Annotatef(err, "save task log %d", opLog.Id)
	}

	return errors.Trace(logger.ForwardTo(db, opLog.Id))
}

// Append appends a task log
func (logger *Logger) Append(db Putter, opLog *pb.TaskLog) error {
	if whetherNil(db) {
		return errors.Trace(ErrInValidHandler)
	}

	var id int64
	for {
		id = atomic.LoadInt64(&logger.endPointer.Location)
		if atomic.CompareAndSwapInt64(&logger.endPointer.Location, id, id+1) {
			break
		}
	}

	opLog.Id = id
	opLog.Ts = time.Now().UnixNano()
	logBytes, err := opLog.Marshal()
	if err != nil {
		return errors.Annotatef(err, "marshal task log %+v", opLog)
	}

	err = db.Put(EncodeTaskLogKey(id), logBytes, nil)
	if err != nil {
		return errors.Annotatef(err, "save task log %+v", opLog)
	}

	return nil
}

// GC deletes useless log
func (logger *Logger) GC(ctx context.Context, db *leveldb.DB) {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Infof("[task log gc] goroutine exits!")
			return
		case <-ticker.C:
			var gcID int64
			handledPointerLocaltion := atomic.LoadInt64(&logger.handledPointer.Location)
			if handledPointerLocaltion > defaultGCForwardLog {
				gcID = handledPointerLocaltion - defaultGCForwardLog
			}
			logger.doGC(db, gcID)
		}
	}
}

func (logger *Logger) doGC(db *leveldb.DB, id int64) {
	if db == nil {
		log.Error(ErrInValidHandler)
		return
	}

	firstKey := make([]byte, 0, len(TaskLogPrefix)+8)
	endKey := EncodeTaskLogKey(id + 1)
	irange := &util.Range{
		Start: EncodeTaskLogKey(0),
	}
	iter := db.NewIterator(irange, nil)
	batch := new(leveldb.Batch)
	for iter.Next() {
		if bytes.Compare(endKey, iter.Key()) <= 0 {
			break
		}

		if len(firstKey) == 0 {
			firstKey = append(firstKey, iter.Key()...)
		}

		batch.Delete(iter.Key())
		if batch.Len() == 1024 {
			err := db.Write(batch, nil)
			if err != nil {
				log.Errorf("[task log gc] fail to delete keys from kv db %v", err)
			}
			log.Infof("[task log gc] delete range [%s(% X), %s(% X)]", firstKey, firstKey, iter.Key(), iter.Key())
			firstKey = firstKey[:0]
			batch.Reset()
		}
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		log.Errorf("[task log gc] query logs from meta error %v", err)
	}

	if batch.Len() > 0 {
		log.Infof("[task log gc] delete range [%s(% X), %s(% X))", firstKey, firstKey, endKey, endKey)
		err := db.Write(batch, nil)
		if err != nil {
			log.Errorf("[task log gc] fail to delete keys from kv db %v", err)
		}
	}
}

// **************** task meta oepration *************** //

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

// LoadTaskMetas loads all task metas from kv db
func LoadTaskMetas(db *leveldb.DB) (map[string]*pb.TaskMeta, error) {
	if db == nil {
		return nil, errors.Trace(ErrInValidHandler)
	}

	var (
		tasks = make(map[string]*pb.TaskMeta)
		err   error
	)

	iter := db.NewIterator(util.BytesPrefix(TaskMetaPrefix), nil)
	for iter.Next() {
		taskBytes := iter.Value()
		task := &pb.TaskMeta{}
		err = task.Unmarshal(taskBytes)
		if err != nil {
			err = errors.Annotatef(err, "unmarshal task meta % X", taskBytes)
			break
		}

		tasks[task.Name] = task
	}
	iter.Release()
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = iter.Error()
	if err != nil {
		return nil, errors.Annotatef(err, "fetch tasks from meta")
	}

	return tasks, nil
}

// SetTaskMeta saves task meta into kv db
func SetTaskMeta(h Putter, task *pb.TaskMeta) error {
	if whetherNil(h) {
		return errors.Trace(ErrInValidHandler)
	}

	err := VerifyTaskMeta(task)
	if err != nil {
		return errors.Annotatef(err, "verify task meta %+v", task)
	}

	taskBytes, err := task.Marshal()
	if err != nil {
		return errors.Annotatef(err, "marshal task %+v", task)
	}

	err = h.Put(EncodeTaskMetaKey(task.Name), taskBytes, nil)
	if err != nil {
		return errors.Annotatef(err, "save task meta %s into kv db", task.Name)
	}

	return nil
}

// GetTaskMeta returns task meta by given name
func GetTaskMeta(h Getter, name string) (*pb.TaskMeta, error) {
	if whetherNil(h) {
		return nil, errors.Trace(ErrInValidHandler)
	}

	taskBytes, err := h.Get(EncodeTaskMetaKey(name), nil)
	if err != nil {
		return nil, errors.Annotatef(err, "get task meta %s from leveldb", name)
	}

	task := &pb.TaskMeta{}
	err = task.Unmarshal(taskBytes)
	if err != nil {
		return nil, errors.Annotatef(err, "unmarshal binary % X", taskBytes)
	}

	return task, nil
}

// DeleteTaskMeta delete task meta from kv DB
func DeleteTaskMeta(h Deleter, name string) error {
	if whetherNil(h) {
		return errors.Trace(ErrInValidHandler)
	}

	err := h.Delete(EncodeTaskMetaKey(name), nil)
	if err != nil {
		return errors.Annotatef(err, "delete task meta %s from kv db", name)
	}

	return nil
}

// VerifyTaskMeta verify legality of take meta
func VerifyTaskMeta(task *pb.TaskMeta) error {
	if task == nil {
		return errors.Errorf("empty task")
	}

	if len(task.Name) == 0 {
		return errors.NotValidf("empty task name")
	}

	if len(task.Task) == 0 {
		return errors.NotValidf("empty task config")
	}

	if task.Stage == pb.Stage_InvalidStage {
		return errors.NotValidf("stage")
	}

	if task.Op == pb.TaskOp_InvalidOp {
		return errors.NotValidf("task operation")
	}

	return nil
}

// CloneTaskMeta returns a task meta copy
func CloneTaskMeta(task *pb.TaskMeta) *pb.TaskMeta {
	clone := new(pb.TaskMeta)
	*clone = *task
	return clone
}

// CloneTaskLog returns a task log copy
func CloneTaskLog(log *pb.TaskLog) *pb.TaskLog {
	clone := new(pb.TaskLog)
	*clone = *log
	clone.Task = CloneTaskMeta(log.Task)
	return clone
}

func whetherNil(handler interface{}) bool {
	return handler == nil || reflect.ValueOf(handler).IsNil()
}
