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

package syncer

import (
	"context"
	"time"

	"github.com/pingcap/dm/pkg/streamer"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"go.uber.org/zap"

	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

var (
	eventTimeout    = 1 * time.Minute
	maxEventTimeout = 1 * time.Hour
)

// BinlogType represents binlog sync type
type BinlogType uint8

// binlog sync type
const (
	RemoteBinlog BinlogType = iota + 1
	LocalBinlog
)

// StreamerProducer provides the ability to generate binlog streamer by StartSync()
// but go-mysql StartSync() returns (struct, err) rather than (interface, err)
// And we can't simplely use StartSync() method in SteamerProducer
// so use generateStreamer to wrap StartSync() method to make *BinlogSyncer and *BinlogReader in same interface
// For other implementations who implement StreamerProducer and Streamer can easily take place of Syncer.streamProducer
// For test is easy to mock
type StreamerProducer interface {
	generateStreamer(pos mysql.Position) (streamer.Streamer, error)
}

// Read local relay log
type localBinlogReader struct {
	reader *streamer.BinlogReader
}

func (l *localBinlogReader) generateStreamer(pos mysql.Position) (streamer.Streamer, error) {
	return l.reader.StartSync(pos)
}

// Read remote binlog
type remoteBinlogReader struct {
	reader     *replication.BinlogSyncer
	tctx       *tcontext.Context
	EnableGTID bool
}

func (r *remoteBinlogReader) generateStreamer(pos mysql.Position) (streamer.Streamer, error) {
	defer func() {
		lastSlaveConnectionID := r.reader.LastConnectionID()
		r.tctx.L().Info("last slave connection", zap.Uint32("connection ID", lastSlaveConnectionID))
	}()

	// FIXME: can enable GTID
	if r.EnableGTID {
		// NOTE: our (per-table based) checkpoint does not support GTID yet
		return nil, terror.ErrSyncerUnitRemoteSteamerWithGTID.Generate()
	}

	streamer, err := r.reader.StartSync(pos)
	return streamer, terror.ErrSyncerUnitRemoteSteamerStartSync.Delegate(err)
}

// StreamerController controls the streamer for read binlog, include:
// 1. reopen streamer
// 2. redirect binlog position or gtid
// 3. transfor from local streamer to remote streamer
type StreamerController struct {
	// the initital binlog type
	binlogType BinlogType

	syncCfg        replication.BinlogSyncerConfig
	localBinlogDir string
	timezone       *time.Location

	streamer         streamer.Streamer
	streamerProducer StreamerProducer // removed

	eventTimeoutCounter time.Duration

	fromDB *UpStreamConn
}

// NewStreamerController creates a new streamer controller
func NewStreamerController(tctx tcontext.Context, syncCfg replication.BinlogSyncerConfig, fromDB *UpStreamConn, binlogType BinlogType, localBinlogDir string, timezone *time.Location, beginPos mysql.Position) (*StreamerController, error) {
	var err error
	streamerController := &StreamerController{
		binlogType:     binlogType,
		syncCfg:        syncCfg,
		localBinlogDir: localBinlogDir,
		timezone:       timezone,
		fromDB:         fromDB,
	}

	streamerController.ResetReplicationSyncer(tctx)
	streamerController.streamer, err = streamerController.streamerProducer.generateStreamer(beginPos)
	return streamerController, err
}

// ResetReplicationSyncer reset the replication
func (c *StreamerController) ResetReplicationSyncer(tctx tcontext.Context) {
	// close old streamerProducer
	if c.streamerProducer != nil {
		switch t := c.streamerProducer.(type) {
		case *remoteBinlogReader:
			c.closeBinlogSyncer(tctx, t.reader)
		case *localBinlogReader:
			// TODO: close old local reader before creating a new one
		default:
			// some other producers such as mockStreamerProducer, should not re-create
			return
		}
	}
	// re-create new streamerProducer
	if c.binlogType == LocalBinlog {
		c.streamerProducer = &localBinlogReader{streamer.NewBinlogReader(&tctx, &streamer.BinlogReaderConfig{RelayDir: c.localBinlogDir, Timezone: c.timezone})}
	} else {
		c.streamerProducer = &remoteBinlogReader{replication.NewBinlogSyncer(c.syncCfg), &tctx, false}
	}
}

// RedirectStreamer redirects the streamer's begin position or gtid
func (c *StreamerController) RedirectStreamer(tctx tcontext.Context, pos mysql.Position) error {
	var err error
	tctx.L().Info("reset global streamer", zap.Stringer("position", pos))
	c.ResetReplicationSyncer(tctx)
	c.streamer, err = c.streamerProducer.generateStreamer(pos)
	return err
}

// GetEvent returns binlog event
func (c *StreamerController) GetEvent(tctx tcontext.Context, syncedPosition mysql.Position) (event *replication.BinlogEvent, err error) {
	ctx, cancel := context.WithTimeout(tctx.Context(), eventTimeout)
	defer cancel()

	for i := 0; i < maxRetryCount; i++ {
		event, err = c.streamer.GetEvent(ctx)
		if err == nil {
			return event, nil
		} else if err == context.Canceled {
			tctx.L().Info("binlog replication main routine quit(context canceled)!", zap.Stringer("last position", syncedPosition))
			return nil, nil
		} else if err == context.DeadlineExceeded {
			tctx.L().Info("deadline exceeded when fetching binlog event")
		} else {
			tctx.L().Error("get event meet error", zap.Error(err))
		}

		if c.haveUnfetchedBinlog(tctx, syncedPosition) {
			tctx.L().Info("timeout when fetching binlog event, there must be some problems with replica connection, try to re-connect")
			err = c.reopenWithRetry(tctx, syncedPosition)
			if err != nil {
				return nil, err
			}
		}
	}

	return event, err
}

func (c *StreamerController) reopenWithRetry(tctx tcontext.Context, pos mysql.Position) error {
	var err error
	for i := 0; i < maxRetryCount; i++ {
		c.streamer, err = c.reopen(tctx, pos)
		if err == nil {
			return nil
		}
		if needRetryReplicate(err) {
			tctx.L().Info("fail to retry open binlog streamer", log.ShortError(err))
			time.Sleep(retryTimeout)
			continue
		}
		break
	}
	return err
}

func (c *StreamerController) reopen(tctx tcontext.Context, pos mysql.Position) (streamer.Streamer, error) {
	tctx.L().Info("reopen", zap.Reflect("position", pos))
	if c.streamerProducer != nil {
		switch r := c.streamerProducer.(type) {
		case *remoteBinlogReader:
			err := c.closeBinlogSyncer(tctx, r.reader)
			if err != nil {
				return nil, err
			}
		case *localBinlogReader:
			// do nothing
		default:
			return nil, terror.ErrSyncerUnitReopenStreamNotSupport.Generate(r)
		}
	}

	c.streamerProducer = &remoteBinlogReader{replication.NewBinlogSyncer(c.syncCfg), &tctx, false}
	return c.streamerProducer.generateStreamer(pos)
}

func (c *StreamerController) haveUnfetchedBinlog(logtctx tcontext.Context, syncedPosition mysql.Position) bool {
	masterPos, _, err := c.getMasterStatus()
	if err != nil {
		logtctx.L().Error("fail to get master status", log.ShortError(err))
		return false
	}

	// Why 190 ?
	// +------------------+-----+----------------+-----------+-------------+-------------------------------------------------------------------+
	// | Log_name         | Pos | Event_type     | Server_id | End_log_pos | Info                                                              |
	// +------------------+-----+----------------+-----------+-------------+-------------------------------------------------------------------+
	// | mysql-bin.000002 |   4 | Format_desc    |         1 |         123 | Server ver: 5.7.18-log, Binlog ver: 4                             |
	// | mysql-bin.000002 | 123 | Previous_gtids |         1 |         194 | 00020393-1111-1111-1111-111111111111:1-7
	//
	// Currently, syncer doesn't handle Format_desc and Previous_gtids events. When binlog rotate to new file with only two events like above,
	// syncer won't save pos to 194. Actually it save pos 4 to meta file. So We got a experience value of 194 - 4 = 190.
	// If (mpos.Pos - spos.Pos) > 190, we could say that syncer is not up-to-date.
	return utils.CompareBinlogPos(masterPos, syncedPosition, 190) == 1
}

func (c *StreamerController) closeBinlogSyncer(logtctx tcontext.Context, binlogSyncer *replication.BinlogSyncer) error {
	if binlogSyncer == nil {
		return nil
	}

	lastSlaveConnectionID := binlogSyncer.LastConnectionID()
	defer binlogSyncer.Close()
	if lastSlaveConnectionID > 0 {
		err := c.fromDB.killConn(lastSlaveConnectionID)
		if err != nil {
			logtctx.L().Error("fail to kill last connection", zap.Uint32("connection ID", lastSlaveConnectionID), log.ShortError(err))
			if !utils.IsNoSuchThreadError(err) {
				return err
			}
		}
	}
	return nil
}

func (c *StreamerController) getMasterStatus() (mysql.Position, gtid.Set, error) {
	return c.fromDB.getMasterStatus(c.syncCfg.Flavor)
}

// Close closes streamer
func (c *StreamerController) Close(tctx tcontext.Context) {
	if c.streamerProducer != nil {
		switch r := c.streamerProducer.(type) {
		case *remoteBinlogReader:
			// process remote binlog reader
			c.closeBinlogSyncer(tctx, r.reader)
			c.streamerProducer = nil
		case *localBinlogReader:
			// process local binlog reader
			r.reader.Close()
		}
	}
}
