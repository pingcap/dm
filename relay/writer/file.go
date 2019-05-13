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
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	gmysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go/sync2"

	"github.com/pingcap/dm/pkg/binlog"
	"github.com/pingcap/dm/pkg/binlog/common"
	"github.com/pingcap/dm/pkg/binlog/event"
	bw "github.com/pingcap/dm/pkg/binlog/writer"
	"github.com/pingcap/dm/pkg/log"
)

// FileConfig is the configuration used by the FileWriter.
type FileConfig struct {
	RelayDir string // directory to store relay log files.
	Filename string // the startup relay log filename, if not set then a fake RotateEvent must be the first event.
}

// FileWriter implements Writer interface.
type FileWriter struct {
	cfg *FileConfig

	mu    sync.RWMutex
	stage common.Stage

	// underlying binlog writer,
	// it will be created/started until needed.
	out *bw.FileWriter

	filename sync2.AtomicString // current binlog filename
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

	if w.stage != common.StageNew {
		return errors.Errorf("stage %s, expect %s, already started", w.stage, common.StageNew)
	}
	w.stage = common.StagePrepared

	return nil
}

// Close implements Writer.Close.
func (w *FileWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.stage != common.StagePrepared {
		return errors.Errorf("stage %s, expect %s, can not close", w.stage, common.StagePrepared)
	}

	var err error
	if w.out != nil {
		err = w.out.Close()
	}

	w.stage = common.StageClosed
	return errors.Trace(err)
}

// Recover implements Writer.Recover.
func (w *FileWriter) Recover(p *parser.Parser) (*RecoverResult, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.stage != common.StagePrepared {
		return nil, errors.Errorf("stage %s, expect %s, please start the writer first", w.stage, common.StagePrepared)
	}

	return w.doRecovering(p)
}

// WriteEvent implements Writer.WriteEvent.
func (w *FileWriter) WriteEvent(ev *replication.BinlogEvent) (*Result, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.stage != common.StagePrepared {
		return nil, errors.Errorf("stage %s, expect %s, please start the writer first", w.stage, common.StagePrepared)
	}

	switch ev.Event.(type) {
	case *replication.FormatDescriptionEvent:
		return w.handleFormatDescriptionEvent(ev)
	case *replication.RotateEvent:
		return w.handleRotateEvent(ev)
	default:
		return w.handleEventDefault(ev)
	}
}

// Flush implements Writer.Flush.
func (w *FileWriter) Flush() error {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.stage != common.StagePrepared {
		return errors.Errorf("stage %s, expect %s, please start the writer first", w.stage, common.StagePrepared)
	}

	if w.out != nil {
		return w.out.Flush()
	}
	return errors.Errorf("no underlying writer opened")
}

// offset returns the current offset of the binlog file.
// it is only used for testing now.
func (w *FileWriter) offset() int64 {
	if w.out == nil {
		return 0
	}
	status := w.out.Status().(*bw.FileWriterStatus)
	return status.Offset
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

	// verify filename
	err := binlog.VerifyBinlogFilename(w.filename.Get())
	if err != nil {
		return nil, errors.Trace(err)
	}

	// open/create a new binlog file
	filename := filepath.Join(w.cfg.RelayDir, w.filename.Get())
	outCfg := &bw.FileWriterConfig{
		Filename: filename,
	}
	out := bw.NewFileWriter(outCfg)
	err = out.Start()
	if err != nil {
		return nil, errors.Annotatef(err, "start underlying binlog writer for %s", filename)
	}
	w.out = out.(*bw.FileWriter)
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
			return nil, errors.Annotatef(err, "write FormatDescriptionEvent %+v for %s", ev.Header, filename)
		}
	}

	return &Result{
		Ignore: exist, // ignore if exists
	}, nil
}

// handle RotateEvent:
//   1. update binlog filename if needed
//   2. write the RotateEvent if not fake
// NOTE: we do not create a new binlog file when received a RotateEvent,
//       instead, we create a new binlog file when received a FormatDescriptionEvent.
//       because a binlog file without any events has no meaning.
func (w *FileWriter) handleRotateEvent(ev *replication.BinlogEvent) (*Result, error) {
	rotateEv, ok := ev.Event.(*replication.RotateEvent)
	if !ok {
		return nil, errors.NotValidf("except RotateEvent, but got %+v", ev.Header)
	}

	// update binlog filename if needed
	var currFile = w.filename.Get()
	nextFile := string(rotateEv.NextLogName)
	if nextFile > currFile {
		// record the next filename, but not create it.
		// even it's a fake RotateEvent, we still need to record it,
		// because if we do not specify the filename when creating the writer (like Auto-Position),
		// we can only receive a fake RotateEvent before the FormatDescriptionEvent.
		w.filename.Set(nextFile)
	}

	// write the RotateEvent if not fake
	var ignore bool
	if ev.Header.Timestamp == 0 || ev.Header.LogPos == 0 {
		// skip fake rotate event
		ignore = true
	} else if w.out == nil {
		// if not open a binlog file yet, then non-fake RotateEvent can't be handled
		return nil, errors.Errorf("non-fake RotateEvent %+v received, but no binlog file opened", ev.Header)
	} else {
		err := w.out.Write(ev.RawData)
		if err != nil {
			return nil, errors.Annotatef(err, "write RotateEvent %+v for %s", ev.Header, filepath.Join(w.cfg.RelayDir, currFile))
		}
	}

	return &Result{
		Ignore: ignore,
	}, nil
}

// handle non-special event:
//   1. handle a potential hole if exists
//   2. handle any duplicate events if exist
//   3. write the non-duplicate event
func (w *FileWriter) handleEventDefault(ev *replication.BinlogEvent) (*Result, error) {
	// handle a potential hole
	err := w.handleFileHoleExist(ev)
	if err != nil {
		return nil, errors.Annotatef(err, "handle a potential hole in %s before %+v",
			w.filename.Get(), ev.Header)
	}

	// handle any duplicate events if exist
	result, err := w.handleDuplicateEventsExist(ev)
	if err != nil {
		return nil, errors.Annotatef(err, "handle a potential duplicate event %+v in %s",
			ev.Header, w.filename.Get())
	}
	if result.Ignore {
		// duplicate, and can ignore it. now, we assume duplicate events can all be ignored
		return result, nil
	}

	// write the non-duplicate event
	err = w.out.Write(ev.RawData)
	return &Result{
		Ignore: false,
	}, errors.Annotatef(err, "write event %+v", ev.Header)
}

// handleFileHoleExist tries to handle a potential hole after this event wrote.
// A hole exists often because some binlog events not sent by the master.
// NOTE: handle cases when file size > 4GB
func (w *FileWriter) handleFileHoleExist(ev *replication.BinlogEvent) error {
	// 1. detect whether a hole exists
	evStartPos := int64(ev.Header.LogPos - ev.Header.EventSize)
	outFs, ok := w.out.Status().(*bw.FileWriterStatus)
	if !ok {
		return errors.Errorf("invalid status type %T of the underlying writer", w.out.Status())
	}
	fileOffset := outFs.Offset
	holeSize := evStartPos - fileOffset
	if holeSize <= 0 {
		// no hole exists, but duplicate events may exists, this should be handled in another place.
		return nil
	}

	// 2. generate dummy event
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  ev.Header.ServerID,
		}
		latestPos = uint32(fileOffset)
		eventSize = uint32(holeSize)
	)
	dummyEv, err := event.GenDummyEvent(header, latestPos, eventSize)
	if err != nil {
		return errors.Annotatef(err, "generate dummy event at %d with size %d", latestPos, eventSize)
	}

	// 3. write the dummy event
	err = w.out.Write(dummyEv.RawData)
	return errors.Trace(err)
}

// handleDuplicateEventsExist tries to handle a potential duplicate event in the binlog file.
func (w *FileWriter) handleDuplicateEventsExist(ev *replication.BinlogEvent) (*Result, error) {
	filename := filepath.Join(w.cfg.RelayDir, w.filename.Get())
	duplicate, err := checkIsDuplicateEvent(filename, ev)
	if err != nil {
		return nil, errors.Annotatef(err, "check event %+v is duplicate in %s", ev, filename)
	}

	return &Result{
		Ignore: duplicate,
	}, nil
}

// doRecovering tries to recover the current binlog file.
// 1. read events from the file
// 2.
//    a. update the position with the event's position if the transaction finished
//    b. update the GTID set with the event's GTID if the transaction finished
// 3. truncate any incomplete events/transactions
// now, we think a transaction finished if we received a XIDEvent or DDL in QueryEvent
// NOTE: handle cases when file size > 4GB
func (w *FileWriter) doRecovering(p *parser.Parser) (*RecoverResult, error) {
	filename := filepath.Join(w.cfg.RelayDir, w.filename.Get())
	// get latest pos/GTID set for all completed transactions from the file
	latestPos, latestGTIDs, err := getTxnPosGTIDs(filename, p)
	if err != nil {
		return nil, errors.Annotatef(err, "get latest pos/GTID set from %s", filename)
	}

	// in most cases, we think the file is fine, so compare the size is simpler.
	fs, err := os.Stat(filename)
	if err != nil {
		return nil, errors.Annotatef(err, "get stat for %s", filename)
	}
	if fs.Size() == latestPos {
		return &RecoverResult{
			Recovered:   false, // no recovering for the file
			LatestPos:   gmysql.Position{Name: w.filename.Get(), Pos: uint32(latestPos)},
			LatestGTIDs: latestGTIDs,
		}, nil
	} else if fs.Size() < latestPos {
		return nil, errors.Errorf("latest pos %d greater than file size %d, should not happen", latestPos, fs.Size())
	}

	// truncate the file
	f, err := os.OpenFile(filename, os.O_WRONLY, 0644)
	if err != nil {
		return nil, errors.Annotatef(err, "open %s", filename)
	}
	defer f.Close()
	err = f.Truncate(latestPos)
	if err != nil {
		return nil, errors.Annotatef(err, "truncate %s to %d", filename, latestPos)
	}

	return &RecoverResult{
		Recovered:   true,
		LatestPos:   gmysql.Position{Name: w.filename.Get(), Pos: uint32(latestPos)},
		LatestGTIDs: latestGTIDs,
	}, nil
}
