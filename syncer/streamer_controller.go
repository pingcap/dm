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
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/binlog"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/streamer"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

var (
	eventTimeout = 1 * time.Minute
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

	// position's name may contain uuid, so need remove it
	adjustedPos := binlog.AdjustPosition(pos)
	streamer, err := r.reader.StartSync(adjustedPos)
	return streamer, terror.ErrSyncerUnitRemoteSteamerStartSync.Delegate(err)
}

// StreamerController controls the streamer for read binlog, include:
// 1. reopen streamer
// 2. redirect binlog position or gtid
// 3. transfor from local streamer to remote streamer
type StreamerController struct {
	sync.RWMutex

	// the initital binlog type
	binlogType BinlogType

	syncCfg        replication.BinlogSyncerConfig
	localBinlogDir string
	timezone       *time.Location

	streamer         streamer.Streamer
	streamerProducer StreamerProducer

	// meetError means meeting error when get binlog event
	// if binlogType is local and meetError is true, then need to create remote binlog stream
	meetError bool

	fromDB *UpStreamConn

	// changed is true when change from local to remote
	changed bool

	uuidSuffix string
}

// NewStreamerController creates a new streamer controller
func NewStreamerController(tctx tcontext.Context, syncCfg replication.BinlogSyncerConfig, fromDB *UpStreamConn, binlogType BinlogType, localBinlogDir string, timezone *time.Location, beginPos mysql.Position) (*StreamerController, error) {
	streamerController := &StreamerController{
		binlogType:     binlogType,
		syncCfg:        syncCfg,
		localBinlogDir: localBinlogDir,
		timezone:       timezone,
		fromDB:         fromDB,
	}

	err := streamerController.ResetReplicationSyncer(tctx, beginPos)
	if err != nil {
		streamerController.Close(tctx)
		return nil, err
	}

	return streamerController, nil
}

// ResetReplicationSyncer reset the replication
func (c *StreamerController) ResetReplicationSyncer(tctx tcontext.Context, pos mysql.Position) (err error) {
	c.Lock()
	defer c.Unlock()

	// close old streamerProducer
	if c.streamerProducer != nil {
		switch t := c.streamerProducer.(type) {
		case *remoteBinlogReader:
			c.closeBinlogSyncer(tctx, t.reader)
		case *localBinlogReader:
			// TODO: close old local reader before creating a new one
		default:
			// some other producers such as mockStreamerProducer, should not re-create
			c.streamer, err = c.streamerProducer.generateStreamer(pos)
			return err
		}
	}

	// binlog type is local: means relay already use the server id, so need change to a new server id
	// binlog type is remote: dm-worker has more than one sub task, so need generate random server id
	randomServerID, err := utils.GetRandomServerID(tctx.Context(), c.fromDB.BaseDB.DB)
	if err != nil {
		// should never happened unless the master has too many slave
		return terror.Annotate(err, "fail to get random server id for streamer controller")
	}
	c.syncCfg.ServerID = randomServerID

	// re-create new streamerProducer
	if c.binlogType == RemoteBinlog {
		c.streamerProducer = &remoteBinlogReader{replication.NewBinlogSyncer(c.syncCfg), &tctx, false}
	} else {
		if c.meetError {
			// meetError is true means meets error when get binlog event, in this case use remote binlog as default
			c.streamerProducer = &remoteBinlogReader{replication.NewBinlogSyncer(c.syncCfg), &tctx, false}
			c.changed = true
		} else {
			c.streamerProducer = &localBinlogReader{streamer.NewBinlogReader(&tctx, &streamer.BinlogReaderConfig{RelayDir: c.localBinlogDir, Timezone: c.timezone})}
		}
	}

	c.streamer, err = c.streamerProducer.generateStreamer(pos)
	return err
}

// RedirectStreamer redirects the streamer's begin position or gtid
func (c *StreamerController) RedirectStreamer(tctx tcontext.Context, pos mysql.Position) error {
	tctx.L().Info("reset global streamer", zap.Stringer("position", pos))
	return c.ResetReplicationSyncer(tctx, pos)
}

// GetEvent returns binlog event
func (c *StreamerController) GetEvent(tctx tcontext.Context) (event *replication.BinlogEvent, err error) {
	failpoint.Inject("SyncerEventTimeout", func(val failpoint.Value) {
		if seconds, ok := val.(int); ok {
			eventTimeout = time.Duration(seconds) * time.Second
			tctx.L().Info("set fetch binlog event timeout", zap.String("failpoint", "SyncerEventTimeout"), zap.Int("value", seconds))
		}
	})
	ctx, cancel := context.WithTimeout(tctx.Context(), eventTimeout)
	event, err = c.streamer.GetEvent(ctx)
	cancel()
	if err == nil {
		switch ev := event.Event.(type) {
		case *replication.RotateEvent:
			// if is local binlog, binlog's name contain uuid information, need save it
			// if is remote binlog, need add uuid information in binlog's name
			if !c.setUUIDIfExists(tctx, string(ev.NextLogName)) {
				if len(c.uuidSuffix) != 0 {
					filename, err := binlog.ParseFilename(string(ev.NextLogName))
					if err != nil {
						return nil, terror.Annotate(err, "fail to parse binlog file name from rotate event")
					}
					ev.NextLogName = []byte(binlog.ConstructFilenameWithUUIDSuffix(filename, c.uuidSuffix))
					event.Event = ev
				}
			}
		default:
		}
		return event, nil
	}

	if err != context.Canceled {
		c.meetError = true
	}

	return nil, err
}

// ReopenWithRetry reopens streamer with retry
func (c *StreamerController) ReopenWithRetry(tctx tcontext.Context, pos mysql.Position) error {
	var err error
	for i := 0; i < maxRetryCount; i++ {
		err = c.ResetReplicationSyncer(tctx, pos)
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

// Close closes streamer
func (c *StreamerController) Close(tctx tcontext.Context) {
	c.Lock()
	defer c.Unlock()

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

func (c *StreamerController) setUUIDIfExists(tctx tcontext.Context, filename string) bool {
	_, uuidSuffix, _, err := binlog.AnalyzeFilenameWithUUIDSuffix(filename)
	if err != nil {
		// don't contain uuid in position's name
		return false
	}

	c.uuidSuffix = uuidSuffix
	return true
}

// UpdateSyncCfg updates sync config and fromDB
func (c *StreamerController) UpdateSyncCfg(syncCfg replication.BinlogSyncerConfig, fromDB *UpStreamConn) {
	c.Lock()
	c.fromDB = fromDB
	c.syncCfg = syncCfg
	c.Unlock()
}
