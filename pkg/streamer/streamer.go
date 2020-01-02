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

package streamer

import (
	"context"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"

	"github.com/pingcap/failpoint"
	"github.com/siddontang/go-mysql/replication"
	"go.uber.org/zap"
)

// TODO: maybe one day we can make a pull request to go-mysql to support LocalStreamer.

// Streamer provides the ability to get binlog event from remote server or local file.
type Streamer interface {
	// GetEvent returns binlog event
	GetEvent(ctx context.Context) (*replication.BinlogEvent, error)
}

// LocalStreamer reads and parses binlog events from local binlog file.
type LocalStreamer struct {
	ch  chan *replication.BinlogEvent
	ech chan error
	err error
}

// GetEvent gets the binlog event one by one, it will block until parser occurs some errors.
// You can pass a context (like Cancel or Timeout) to break the block.
func (s *LocalStreamer) GetEvent(ctx context.Context) (*replication.BinlogEvent, error) {
	if s.err != nil {
		return nil, terror.ErrNeedSyncAgain.Generate()
	}

	failpoint.Inject("GetEventFromLocalFailed", func(_ failpoint.Value) {
		log.L().Info("get event from local failed", zap.String("failpoint", "GetEventFromLocalFailed"))
		failpoint.Return(nil, terror.ErrSyncClosed.Generate())
	})

	select {
	case c := <-s.ch:
		return c, nil
	case s.err = <-s.ech:
		return nil, s.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *LocalStreamer) close() {
	s.closeWithError(terror.ErrSyncClosed.Generate())
}

func (s *LocalStreamer) closeWithError(err error) {
	if err == nil {
		err = terror.ErrSyncClosed.Generate()
	}
	log.L().Error("close local streamer", log.ShortError(err))
	select {
	case s.ech <- err:
	default:
	}
}

func newLocalStreamer() *LocalStreamer {
	s := new(LocalStreamer)

	s.ch = make(chan *replication.BinlogEvent, 10240)
	s.ech = make(chan error, 4)

	return s
}
