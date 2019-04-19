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
	"path/filepath"
	"sync"

	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go/sync2"

	bw "github.com/pingcap/dm/pkg/binlog/writer"
	"github.com/pingcap/dm/pkg/log"
)

// FileConfig is the configuration used by the FileWriter.
type FileConfig struct {
	RelayDir string // directory to store relay log files.
	Filename string // the startup relay log filename.
}

// FileWriter implements Writer interface.
type FileWriter struct {
	cfg *FileConfig

	mu    sync.RWMutex
	stage writerStage

	// underlying binlog writer,
	// it will be created/started until needed.
	out bw.Writer

	filename sync2.AtomicString // current binlog filename
	offset   sync2.AtomicInt64  // the file size may > 4GB
}

// NewFileWriter creates a FileWriter instances.
func NewFileWriter(cfg *FileConfig) Writer {
	w := &FileWriter{
		cfg: cfg,
	}
	w.filename.Set(cfg.Filename) // set the startup filename
	return w
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

	var err error
	if w.out != nil {
		err = w.out.Close()
	}

	return errors.Trace(err)
}

// WriteEvent implements Writer.WriteEvent.
func (w *FileWriter) WriteEvent(ev *replication.BinlogEvent) (*Result, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.stage != stagePrepared {
		return nil, errors.Errorf("stage %s, expect %s, please start the writer first", w.stage, stagePrepared)
	}

	switch ev.Event.(type) {
	case *replication.FormatDescriptionEvent:
		return w.handleFormatDescriptionEvent(ev)
	default:
		err := w.out.Write(ev.RawData)
		return &Result{
			Ignore: false,
		}, errors.Annotatef(err, "write event %+v", ev.Header)
	}
}

// Flush implements Writer.Flush.
func (w *FileWriter) Flush() error {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.stage != stagePrepared {
		return errors.Errorf("stage %s, expect %s, please start the writer first", w.stage, stagePrepared)
	}

	if w.out != nil {
		return w.out.Flush()
	}
	return nil
}

// handle FormatDescriptionEvent:
//   1. close the previous binlog file
//   2. open/create a new binlog file
//   3. write the binlog file header if not exists
//   4. write the FormatDescriptionEvent if not exists one
func (w *FileWriter) handleFormatDescriptionEvent(ev *replication.BinlogEvent) (*Result, error) {
	// close the previous binlog file
	if w.out != nil {
		log.Infof("[relay] closing previous underlying binlog writer with status %v", w.out.Status())
		err := w.out.Close()
		if err != nil {
			return nil, errors.Annotate(err, "close previous underlying binlog writer")
		}
	}

	// open/create a new binlog file
	filename := filepath.Join(w.cfg.RelayDir, w.filename.Get())
	outCfg := &bw.FileWriterConfig{
		Filename: filename,
	}
	out := bw.NewFileWriter(outCfg)
	err := out.Start()
	if err != nil {
		return nil, errors.Annotatef(err, "start underlying binlog writer for %s", filename)
	}
	w.out = out
	log.Infof("[relay] open underlying binlog writer with status %v", w.out.Status())

	// write the binlog file header if not exists
	exist, err := checkBinlogHeaderExist(filename)
	if err != nil {
		return nil, errors.Annotatef(err, "check binlog file header for %s", filename)
	} else if !exist {
		err = w.out.Write(replication.BinLogFileHeader)
		if err != nil {
			return nil, errors.Annotatef(err, "write binlog file header for %s", filename)
		}
	}

	// write the FormatDescriptionEvent if not exists one
	exist, err = checkFormatDescriptionEventExist(filename)
	if err != nil {
		return nil, errors.Annotatef(err, "check FormatDescriptionEvent for %s", filename)
	} else if !exist {
		err = w.out.Write(ev.RawData)
		if err != nil {
			return nil, errors.Annotatef(err, "write FormatDescriptionEvent for %s", filename)
		}
	}

	return &Result{
		Ignore: exist, // ignore if exists
	}, nil
}
