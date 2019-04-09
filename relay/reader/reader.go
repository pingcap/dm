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

package reader

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go/sync2"

	br "github.com/pingcap/dm/pkg/binlog/reader"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/log"
)

// Reader reads binlog events from a upstream master server.
// A transformer should receive binlog events from this reader.
// The reader should support:
//   1. handle expected errors
//   2. do retry if possible
type Reader interface {
	// Start starts the reading process.
	Start() error

	// Close closes the reader and release the resource.
	Close() error

	// GetEvent gets the binlog event one by one, it will block if no event can be read.
	// You can pass a context (like Cancel or Timeout) to break the block.
	GetEvent(ctx context.Context) (*replication.BinlogEvent, error)
}

// Config is the configuration used by the Reader.
type Config struct {
	SyncConfig replication.BinlogSyncerConfig
	Pos        mysql.Position
	GTIDs      gtid.Set
	EnableGTID bool
	MasterID   string // the identifier for the master, used when logging.
}

// reader implements Reader interface.
type reader struct {
	cfg   *Config
	mu    sync.Mutex
	stage sync2.AtomicInt32

	in  br.Reader // the underlying reader used to read binlog events.
	out chan *replication.BinlogEvent
}

// NewReader creates a Reader instance.
func NewReader(cfg *Config) Reader {
	return &reader{
		cfg: cfg,
		in:  br.NewTCPReader(cfg.SyncConfig),
		out: make(chan *replication.BinlogEvent),
	}
}

// Start implements Reader.Start.
func (r *reader) Start() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stage.Get() != int32(stageNew) {
		return errors.Errorf("stage %s, expect %s", readerStage(r.stage.Get()), stageNew)
	}

	defer func() {
		status := r.in.Status()
		log.Infof("[relay] set up binlog reader for master %s with status %s", r.cfg.MasterID, status)
	}()

	if r.cfg.EnableGTID {
		return r.setUpReaderByGTID()
	}
	return r.setUpReaderByPos()
}

// Close implements Reader.Close.
func (r *reader) Close() error {
	if r.stage.Get() == int32(stageClosed) {
		return errors.New("already closed")
	}

	err := r.in.Close()
	r.stage.Set(int32(stageClosed))
	return errors.Trace(err)
}

// GetEvent implements Reader.GetEvent.
func (r *reader) GetEvent(ctx context.Context) (*replication.BinlogEvent, error) {
	if r.stage.Get() != int32(stagePrepared) {
		return nil, errors.Errorf("stage %s, expect %s", readerStage(r.stage.Get()), stagePrepared)
	}

	// TODO: handle errors and retry here
	return r.in.GetEvent(ctx)
}

func (r *reader) setUpReaderByGTID() error {
	gs := r.cfg.GTIDs
	log.Infof("[relay] start sync for master %s from GTID set %s", r.cfg.MasterID, gs)
	err := r.in.StartSyncByGTID(gs)
	if err != nil {
		log.Errorf("[relay] start sync for master %s from GTID set %s error %v",
			r.cfg.MasterID, gs, errors.ErrorStack(err))
		return r.setUpReaderByPos()
	}
	return nil
}

func (r *reader) setUpReaderByPos() error {
	pos := r.cfg.Pos
	log.Infof("[relay] start sync for master %s from position %s", r.cfg.MasterID, pos)
	return r.in.StartSyncByPos(pos)
}
