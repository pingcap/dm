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
	"sync"

	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/replication"

	bw "github.com/pingcap/dm/pkg/binlog/writer"
)

// FileConfig is the configuration used by the FileWriter.
type FileConfig struct {
	RelayDir string // directory to store relay log files.
}

// FileWriter implements Writer interface.
type FileWriter struct {
	cfg *FileConfig

	mu    sync.RWMutex
	stage writerStage

	out bw.Writer // underlying binlog writer
}

// NewFileWriter creates a FileWriter instances.
func NewFileWriter(cfg *FileConfig) Writer {
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

	w.stage = stageClosed
	return nil
}

// WriteEvent implements Writer.WriteEvent.
func (w *FileWriter) WriteEvent(ev *replication.BinlogEvent) (*Result, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.stage != stagePrepared {
		return nil, errors.Errorf("stage %s, expect %s, please start the writer first", w.stage, stagePrepared)
	}

	return &Result{
		Ignore: false,
	}, nil
}

// Flush implements Writer.Flush.
func (w *FileWriter) Flush() error {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.stage != stagePrepared {
		return errors.Errorf("stage %s, expect %s, please start the writer first", w.stage, stagePrepared)
	}

	return nil
}
