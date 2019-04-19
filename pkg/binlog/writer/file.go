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

package writer

import (
	"encoding/json"
	"os"
	"sync"

	"github.com/pingcap/errors"
	"github.com/siddontang/go/sync2"

	"github.com/pingcap/dm/pkg/log"
)

// FileWriter is a binlog event writer which writes binlog events to a file.
type FileWriter struct {
	cfg *FileWriterConfig

	mu     sync.RWMutex
	stage  writerStage
	offset sync2.AtomicInt64

	file *os.File
}

// FileWriterStatus represents the status of a FileWriter.
type FileWriterStatus struct {
	Stage    string `json:"stage"`
	Filename string `json:"filename"`
	Offset   int64  `json:"offset"`
}

// String implements Stringer.String.
func (s *FileWriterStatus) String() string {
	data, err := json.Marshal(s)
	if err != nil {
		log.Errorf("[FileWriterStatus] marshal status to json error %v", err)
	}
	return string(data)
}

// FileWriterConfig is the configuration used by a FileWriter.
type FileWriterConfig struct {
	Filename string
}

// NewFileWriter creates a FileWriter instance.
func NewFileWriter(cfg *FileWriterConfig) Writer {
	return &FileWriter{
		cfg: cfg,
	}
}

// Start implements Writer.Start.
func (w *FileWriter) Start() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.stage != stageNew {
		return errors.Errorf("stage %s, expect %s, already started", w.stage, stageNew)
	}

	f, err := os.OpenFile(w.cfg.Filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return errors.Annotatef(err, "open file %s", w.cfg.Filename)
	}
	w.file = f
	fs, err := f.Stat()
	if err != nil {
		return errors.Annotatef(err, "get stat for %s", f.Name())
	}
	w.offset.Set(fs.Size())

	w.stage = stagePrepared
	return nil
}

// Close implements Writer.Close.
func (w *FileWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.stage == stageClosed {
		return errors.New("already closed")
	}

	var err error
	if w.file != nil {
		err2 := w.flush() // try flush manually before close.
		if err2 != nil {
			log.Errorf("[file writer] flush buffered data error %s", err2)
		}
		err = w.file.Close()
		w.file = nil
	}

	w.stage = stageClosed
	return err
}

// Write implements Writer.Write.
func (w *FileWriter) Write(rawData []byte) error {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.stage != stagePrepared {
		return errors.Errorf("stage %s, expect %s, please start the writer first", w.stage, stagePrepared)
	}

	n, err := w.file.Write(rawData)
	w.offset.Add(int64(n))

	return errors.Annotatef(err, "data length %d", len(rawData))
}

// Flush implements Writer.Flush.
func (w *FileWriter) Flush() error {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.stage != stagePrepared {
		return errors.Errorf("stage %s, expect %s, please start the writer first", w.stage, stagePrepared)
	}

	return w.flush()
}

// Status implements Writer.Status.
func (w *FileWriter) Status() interface{} {
	w.mu.RLock()
	stage := w.stage
	w.mu.RUnlock()

	return &FileWriterStatus{
		Stage:    stage.String(),
		Filename: w.cfg.Filename,
		Offset:   w.offset.Get(),
	}
}

// flush flushes the buffered data to the disk.
func (w *FileWriter) flush() error {
	if w.file == nil {
		return errors.Errorf("file %s not opened", w.cfg.Filename)
	}
	return w.file.Sync()
}
