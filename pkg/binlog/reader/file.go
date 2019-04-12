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

// binlog events generator for MySQL used to generate some binlog events for tests.
// Readability takes precedence over performance.

package reader

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	gmysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go/sync2"

	"github.com/pingcap/dm/pkg/gtid"
)

// FileReader is a binlog event reader which read binlog events from a file.
type FileReader struct {
	mu sync.Mutex
	wg sync.WaitGroup

	stage      sync2.AtomicInt32
	readOffset sync2.AtomicUint32
	sendOffset sync2.AtomicUint32

	parser *replication.BinlogParser
	ch     chan *replication.BinlogEvent
	ech    chan error

	ctx    context.Context
	cancel context.CancelFunc
}

// FileReaderConfig is the configuration used by a FileReader.
type FileReaderConfig struct {
	EnableRawMode bool
	Timezone      *time.Location
	ChBufferSize  int // event channel's buffer size
	EchBufferSize int // error channel's buffer size
}

// FileReaderStatus represents the status of a FileReader.
type FileReaderStatus struct {
	Stage      string `json:"stage"`
	ReadOffset uint32 `json:"read-offset"` // read event's offset in the file
	SendOffset uint32 `json:"send-offset"` // sent event's offset in the file
}

// NewFileReader creates a FileReader instance.
func NewFileReader(cfg *FileReaderConfig) Reader {
	parser := replication.NewBinlogParser()
	parser.SetVerifyChecksum(true)
	parser.SetUseDecimal(true)
	parser.SetRawMode(cfg.EnableRawMode)
	if cfg.Timezone != nil {
		parser.SetTimestampStringLocation(cfg.Timezone)
	}
	return &FileReader{
		parser: parser,
		ch:     make(chan *replication.BinlogEvent, cfg.ChBufferSize),
		ech:    make(chan error, cfg.EchBufferSize),
	}
}

// StartSyncByPos implements Reader.StartSyncByPos.
func (r *FileReader) StartSyncByPos(pos gmysql.Position) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stage.Get() != int32(stageNew) {
		return errors.Errorf("stage %s, expect %s", readerStage(r.stage.Get()), stageNew)
	}

	r.ctx, r.cancel = context.WithCancel(context.Background())
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		err := r.parser.ParseFile(pos.Name, int64(pos.Pos), r.onEvent)
		if err != nil {
			select {
			case r.ech <- err:
			case <-r.ctx.Done():
			}
		}
	}()

	r.stage.Set(int32(stagePrepared))
	return nil
}

// StartSyncByGTID implements Reader.StartSyncByGTID.
func (r *FileReader) StartSyncByGTID(gSet gtid.Set) error {
	// NOTE: may be supported later.
	return errors.NotSupportedf("read from file by GTID")
}

// Close implements Reader.Close.
func (r *FileReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stage.Get() == int32(stageClosed) {
		return errors.New("already closed")
	}

	r.parser.Stop()
	r.cancel()
	r.wg.Wait()
	r.stage.Set(int32(stageClosed))
	return nil
}

// GetEvent implements Reader.GetEvent.
func (r *FileReader) GetEvent(ctx context.Context) (*replication.BinlogEvent, error) {
	if r.stage.Get() != int32(stagePrepared) {
		return nil, errors.Errorf("stage %s, expect %s", readerStage(r.stage.Get()), stagePrepared)
	}

	select {
	case ev := <-r.ch:
		r.sendOffset.Set(ev.Header.LogPos)
		return ev, nil
	case err := <-r.ech:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.ctx.Done(): // Reader closed
		return nil, r.ctx.Err()
	}
}

// Status implements Reader.Status.
func (r *FileReader) Status() interface{} {
	return &FileReaderStatus{
		Stage:      readerStage(r.stage.Get()).String(),
		ReadOffset: r.sendOffset.Get(),
		SendOffset: r.readOffset.Get(),
	}
}

func (r *FileReader) onEvent(ev *replication.BinlogEvent) error {
	select {
	case r.ch <- ev:
		r.readOffset.Set(ev.Header.LogPos)
		return nil
	case <-r.ctx.Done():
		return r.ctx.Err()
	}
}
