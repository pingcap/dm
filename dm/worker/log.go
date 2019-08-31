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
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/pb"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/helper"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
)

var (
	// GCBatchSize is batch size for gc process
	GCBatchSize = 1024
	// GCInterval is the interval to gc
	GCInterval = time.Hour
)

// dbOperator is an interface which used to do Get/Put/Delete operation on levelDB.
// It often can be an instance of leveldb.DB or leveldb.Transaction.
type dbOperator interface {
	Get(key []byte, ro *opt.ReadOptions) ([]byte, error)
	Put(key, value []byte, opts *opt.WriteOptions) error
	Delete(key []byte, wo *opt.WriteOptions) error
	Write(batch *leveldb.Batch, wo *opt.WriteOptions) error
	NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator
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
	binary.BigEndian.PutUint64(data, uint64(p.Location))

	return data, nil
}

// UnmarshalBinary implement encoding.BinaryMarshal
func (p *Pointer) UnmarshalBinary(data []byte) error {
	if len(data) != 8 {
		return terror.ErrWorkerLogPointerInvalid.Generate(data)
	}

	p.Location = int64(binary.BigEndian.Uint64(data))
	return nil
}

// LoadHandledPointer loads handled pointer value from kv DB
func LoadHandledPointer(h dbOperator) (Pointer, error) {
	var p Pointer
	if helper.IsNil(h) {
		return p, terror.ErrWorkerLogInvalidHandler.Generate()
	}

	value, err := h.Get(HandledPointerKey, nil)
	if err != nil {
		// return zero value when not found
		if err == leveldb.ErrNotFound {
			return p, nil
		}

		return p, terror.ErrWorkerLogFetchPointer.Delegate(err)
	}

	err = p.UnmarshalBinary(value)
	if err != nil {
		return p, terror.ErrWorkerLogUnmarshalPointer.Delegate(err, value)
	}

	return p, nil
}

// ClearHandledPointer clears the handled pointer in kv DB.
func ClearHandledPointer(tctx *tcontext.Context, h dbOperator) error {
	if helper.IsNil(h) {
		return terror.ErrWorkerLogInvalidHandler.Generate()
	}

	err := h.Delete(HandledPointerKey, nil)
	return terror.ErrWorkerLogClearPointer.Delegate(err)
}

var (
	defaultGCForwardLog int64 = 10000

	// TaskLogPrefix is  prefix of task log key
	TaskLogPrefix = []byte("!DM!TaskLog")
)

// DecodeTaskLogKey decodes task log key and returns its log ID
func DecodeTaskLogKey(key []byte) (int64, error) {
	if len(key) != len(TaskLogPrefix)+8 {
		return 0, terror.ErrWorkerLogTaskKeyNotValid.Generate(key)
	}

	return int64(binary.BigEndian.Uint64(key[len(TaskLogPrefix):])), nil
}

// EncodeTaskLogKey encodes log ID into a task log key
func EncodeTaskLogKey(id int64) []byte {
	key := make([]byte, 8+len(TaskLogPrefix))
	copy(key[:len(TaskLogPrefix)], TaskLogPrefix)
	binary.BigEndian.PutUint64(key[len(TaskLogPrefix):], uint64(id))

	return key
}

// Logger manage task operation logs
type Logger struct {
	endPointer     Pointer
	handledPointer Pointer

	l log.Logger
}

// Initial initials Logger
func (logger *Logger) Initial(h dbOperator) ([]*pb.TaskLog, error) {
	if helper.IsNil(h) {
		return nil, terror.ErrWorkerLogInvalidHandler.Generate()
	}

	handledPointer, err := LoadHandledPointer(h)
	if err != nil {
		return nil, err
	}

	var (
		endPointer = Pointer{
			Location: handledPointer.Location + 1,
		}
		logs = make([]*pb.TaskLog, 0, 4)
	)
	iter := h.NewIterator(util.BytesPrefix(TaskLogPrefix), nil)
	startLocation := handledPointer.Location + 1
	for ok := iter.Seek(EncodeTaskLogKey(startLocation)); ok; ok = iter.Next() {
		logBytes := iter.Value()
		opLog := &pb.TaskLog{}
		err = opLog.Unmarshal(logBytes)
		if err != nil {
			err = terror.ErrWorkerLogUnmarshalTaskKey.Delegate(err, logBytes)
			break
		}

		if opLog.Id >= endPointer.Location {
			// move to next location
			endPointer.Location = opLog.Id + 1
			logs = append(logs, opLog)
		} else {
			panic(fmt.Sprintf("out of sorted order from level db for task log key % X (log ID %d), start location %d, end location %d",
				iter.Key(), opLog.Id, startLocation, endPointer.Location))
		}
	}
	iter.Release()
	if err != nil {
		return nil, err
	}

	err = iter.Error()
	if err != nil {
		return nil, terror.ErrWorkerLogFetchLogIter.Delegate(err, handledPointer)
	}

	logger.handledPointer = handledPointer
	logger.endPointer = endPointer

	logger.l.Info("finish initialization", zap.Reflect("handle pointer", logger.handledPointer), zap.Reflect("end pointer", logger.endPointer))

	return logs, nil
}

// GetTaskLog returns task log by given log ID
func (logger *Logger) GetTaskLog(h dbOperator, id int64) (*pb.TaskLog, error) {
	if helper.IsNil(h) {
		return nil, terror.ErrWorkerLogInvalidHandler.Generate()
	}

	logBytes, err := h.Get(EncodeTaskLogKey(id), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, nil
		}
		return nil, terror.ErrWorkerLogGetTaskLog.Delegate(err, id)
	}

	opLog := &pb.TaskLog{}
	err = opLog.Unmarshal(logBytes)
	if err != nil {
		return nil, terror.ErrWorkerLogUnmarshalBinary.Delegate(err, logBytes)
	}

	return opLog, nil
}

// ForwardTo forward handled pointer to specified ID location
// not thread safe
func (logger *Logger) ForwardTo(h dbOperator, ID int64) error {
	if helper.IsNil(h) {
		return terror.ErrWorkerLogInvalidHandler.Generate()
	}

	handledPointer := Pointer{
		Location: ID,
	}

	handledPointerBytes, _ := handledPointer.MarshalBinary()

	err := h.Put(HandledPointerKey, handledPointerBytes, nil)
	if err != nil {
		return terror.ErrWorkerLogForwardPointer.Delegate(err, ID)
	}

	logger.handledPointer = handledPointer
	return nil
}

// MarkAndForwardLog marks result sucess or not in log, and forwards handledPointer
func (logger *Logger) MarkAndForwardLog(h dbOperator, opLog *pb.TaskLog) error {
	if helper.IsNil(h) {
		return terror.ErrWorkerLogInvalidHandler.Generate()
	}

	logBytes, err := opLog.Marshal()
	if err != nil {
		return terror.ErrWorkerLogMarshalTask.Delegate(err, opLog)
	}

	err = h.Put(EncodeTaskLogKey(opLog.Id), logBytes, nil)
	if err != nil {
		return terror.ErrWorkerLogSaveTask.Delegate(err, opLog.Id)
	}

	return logger.ForwardTo(h, opLog.Id)
}

// Append appends a task log
func (logger *Logger) Append(h dbOperator, opLog *pb.TaskLog) error {
	if helper.IsNil(h) {
		return terror.ErrWorkerLogInvalidHandler.Generate()
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
		return terror.ErrWorkerLogMarshalTask.Delegate(err, opLog)
	}

	err = h.Put(EncodeTaskLogKey(id), logBytes, nil)
	if err != nil {
		return terror.ErrWorkerLogSaveTask.Delegate(err, opLog)
	}

	return nil
}

// GC deletes useless log
func (logger *Logger) GC(ctx context.Context, h dbOperator) {
	ticker := time.NewTicker(GCInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			logger.l.Info("gc routine exist!")
			return
		case <-ticker.C:
			var gcID int64
			handledPointerLocaltion := atomic.LoadInt64(&logger.handledPointer.Location)
			if handledPointerLocaltion > defaultGCForwardLog {
				gcID = handledPointerLocaltion - defaultGCForwardLog
			}
			logger.doGC(h, gcID)
		}
	}
}

func (logger *Logger) doGC(h dbOperator, id int64) {
	if helper.IsNil(h) {
		logger.l.Error(terror.ErrWorkerLogInvalidHandler.Error(), zap.String("feature", "gc"))
		return
	}

	firstKey := make([]byte, 0, len(TaskLogPrefix)+8)
	endKey := EncodeTaskLogKey(id + 1)
	irange := &util.Range{
		Start: EncodeTaskLogKey(0),
	}
	iter := h.NewIterator(irange, nil)
	batch := new(leveldb.Batch)
	for iter.Next() {
		if bytes.Compare(endKey, iter.Key()) <= 0 {
			break
		}

		if len(firstKey) == 0 {
			firstKey = append(firstKey, iter.Key()...)
		}

		batch.Delete(iter.Key())
		if batch.Len() == GCBatchSize {
			err := h.Write(batch, nil)
			if err != nil {
				logger.l.Error("fail to delete keys from kv db", zap.String("feature", "gc"), zap.Binary("binary key", iter.Key()), zap.ByteString("text key", iter.Key()), log.ShortError(err))
			}
			logger.l.Info("delete range", zap.String("feature", "gc"), zap.ByteString("first key", firstKey), zap.Binary("raw first key", firstKey), zap.ByteString("end key", iter.Key()), zap.Binary("raw end key", iter.Key()))
			firstKey = firstKey[:0]
			batch.Reset()
		}
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		logger.l.Error("query logs from meta", zap.String("feature", "gc"),
			zap.ByteString("first key", firstKey), zap.Binary("raw first key", firstKey), zap.ByteString("< end key", endKey), zap.Binary("< raw end key", endKey), log.ShortError(err))
	}

	if batch.Len() > 0 {
		logger.l.Info("delete range", zap.String("feature", "gc"), zap.ByteString("first key", firstKey), zap.Binary("raw first key", firstKey), zap.ByteString("< end key", endKey), zap.Binary("< raw end key", endKey))
		err := h.Write(batch, nil)
		if err != nil {
			logger.l.Error("fail to delete keys from kv db", log.ShortError(err))
		}
	}
}

// ClearOperationLog clears the task operation log.
func ClearOperationLog(tctx *tcontext.Context, h dbOperator) error {
	return terror.Annotate(clearByPrefix(tctx, h, TaskLogPrefix), "clear task operation log")
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
func LoadTaskMetas(h dbOperator) (map[string]*pb.TaskMeta, error) {
	if helper.IsNil(h) {
		return nil, terror.ErrWorkerLogInvalidHandler.Generate()
	}

	var (
		tasks = make(map[string]*pb.TaskMeta)
		err   error
	)

	iter := h.NewIterator(util.BytesPrefix(TaskMetaPrefix), nil)
	for iter.Next() {
		taskBytes := iter.Value()
		task := &pb.TaskMeta{}
		err = task.Unmarshal(taskBytes)
		if err != nil {
			err = terror.ErrWorkerLogUnmarshalTaskMeta.Delegate(err, taskBytes)
			break
		}

		tasks[task.Name] = task
	}
	iter.Release()
	if err != nil {
		return nil, terror.ErrWorkerLogFetchTaskFromMeta.Delegate(err, TaskMetaPrefix)
	}

	err = iter.Error()
	if err != nil {
		return nil, terror.ErrWorkerLogFetchTaskFromMeta.Delegate(err, TaskMetaPrefix)
	}

	return tasks, nil
}

// SetTaskMeta saves task meta into kv db
func SetTaskMeta(h dbOperator, task *pb.TaskMeta) error {
	if helper.IsNil(h) {
		return terror.ErrWorkerLogInvalidHandler.Generate()
	}

	err := VerifyTaskMeta(task)
	if err != nil {
		return terror.Annotatef(err, "verify task meta %+v", task)
	}

	taskBytes, err := task.Marshal()
	if err != nil {
		return terror.ErrWorkerLogMarshalTask.Delegate(err, task)
	}

	err = h.Put(EncodeTaskMetaKey(task.Name), taskBytes, nil)
	if err != nil {
		return terror.ErrWorkerLogSaveTaskMeta.Delegate(err, task.Name)
	}

	return nil
}

// GetTaskMeta returns task meta by given name
func GetTaskMeta(h dbOperator, name string) (*pb.TaskMeta, error) {
	if helper.IsNil(h) {
		return nil, terror.ErrWorkerLogInvalidHandler.Generate()
	}

	taskBytes, err := h.Get(EncodeTaskMetaKey(name), nil)
	if err != nil {
		return nil, terror.ErrWorkerLogGetTaskMeta.Delegate(err, name)
	}

	task := &pb.TaskMeta{}
	err = task.Unmarshal(taskBytes)
	if err != nil {
		return nil, terror.ErrWorkerLogUnmarshalBinary.Delegate(err, taskBytes)
	}

	return task, nil
}

// DeleteTaskMeta delete task meta from kv DB
func DeleteTaskMeta(h dbOperator, name string) error {
	if helper.IsNil(h) {
		return terror.ErrWorkerLogInvalidHandler.Generate()
	}

	err := h.Delete(EncodeTaskMetaKey(name), nil)
	if err != nil {
		return terror.ErrWorkerLogDeleteTaskMeta.Delegate(err, name)
	}

	return nil
}

// ClearTaskMeta clears all task meta in kv DB.
func ClearTaskMeta(tctx *tcontext.Context, h dbOperator) error {
	return terror.Annotate(clearByPrefix(tctx, h, TaskMetaPrefix), "clear task meta")
}

// VerifyTaskMeta verify legality of take meta
func VerifyTaskMeta(task *pb.TaskMeta) error {
	if task == nil {
		return terror.ErrWorkerLogVerifyTaskMeta.New("empty task not valid")
	}

	if len(task.Name) == 0 {
		return terror.ErrWorkerLogVerifyTaskMeta.New("empty task name not valid")
	}

	if len(task.Task) == 0 {
		return terror.ErrWorkerLogVerifyTaskMeta.New("empty task config not valid")
	}

	if task.Stage == pb.Stage_InvalidStage {
		return terror.ErrWorkerLogVerifyTaskMeta.Generatef("stage %s not valid", task.Stage)
	}

	if task.Op == pb.TaskOp_InvalidOp {
		return terror.ErrWorkerLogVerifyTaskMeta.Generatef("task operation %s not valid", task.Op)
	}

	return nil
}

// CloneTaskMeta returns a task meta copy
func CloneTaskMeta(task *pb.TaskMeta) *pb.TaskMeta {
	return proto.Clone(task).(*pb.TaskMeta)
}

// CloneTaskLog returns a task log copy
func CloneTaskLog(log *pb.TaskLog) *pb.TaskLog {
	return proto.Clone(log).(*pb.TaskLog)
}

// clearByPrefix clears all keys with the specified prefix.
func clearByPrefix(tctx *tcontext.Context, h dbOperator, prefix []byte) error {
	if helper.IsNil(h) {
		return terror.ErrWorkerLogInvalidHandler.Generate()
	}

	var err error
	iter := h.NewIterator(util.BytesPrefix(prefix), nil)
	batch := new(leveldb.Batch)
	for iter.Next() {
		batch.Delete(iter.Key())
		if batch.Len() >= GCBatchSize {
			err = h.Write(batch, nil)
			if err != nil {
				iter.Release()
				return terror.ErrWorkerLogDeleteKV.Delegate(err, prefix, iter.Key())
			}
			tctx.L().Info("delete task operation log kv", zap.Binary("with prefix", prefix), zap.Binary("< key", iter.Key()))
			batch.Reset()
		}
	}
	iter.Release()
	err = iter.Error()
	if err != nil {
		return terror.ErrWorkerLogDeleteKVIter.Delegate(err, prefix)
	}

	if batch.Len() > 0 {
		err = h.Write(batch, nil)
	}
	return terror.ErrWorkerLogDeleteKV.Delegate(err, prefix, iter.Key())
}
