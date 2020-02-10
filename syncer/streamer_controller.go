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
	"strings"
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

const (
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
	initBinlogType BinlogType

	// the current binlog type
	currentBinlogType BinlogType

	syncCfg        replication.BinlogSyncerConfig
	localBinlogDir string
	timezone       *time.Location

	streamer         streamer.Streamer
	streamerProducer StreamerProducer

	// meetError means meeting error when get binlog event
	// if binlogType is local and meetError is true, then need to create remote binlog stream
	meetError bool

	fromDB *UpStreamConn

	uuidSuffix string

	closed bool

	// whether the server id is updated
	serverIDUpdated bool
}

// NewStreamerController creates a new streamer controller
func NewStreamerController(tctx *tcontext.Context, syncCfg replication.BinlogSyncerConfig, fromDB *UpStreamConn, binlogType BinlogType, localBinlogDir string, timezone *time.Location) *StreamerController {
	streamerController := &StreamerController{
		initBinlogType:    binlogType,
		currentBinlogType: binlogType,
		syncCfg:           syncCfg,
		localBinlogDir:    localBinlogDir,
		timezone:          timezone,
		fromDB:            fromDB,
		closed:            true,
	}

	return streamerController
}

// Start starts streamer controller
func (c *StreamerController) Start(tctx *tcontext.Context, pos mysql.Position) error {
	c.Lock()
	defer c.Unlock()

	c.meetError = false
	c.closed = false
	c.currentBinlogType = c.initBinlogType

	var err error
	if c.serverIDUpdated {
		err = c.resetReplicationSyncer(tctx, pos)
	} else {
		err = c.updateServerIDAndResetReplication(tctx, pos)
	}
	if err != nil {
		c.close(tctx)
		return err
	}

	return nil
}

// ResetReplicationSyncer reset the replication
func (c *StreamerController) ResetReplicationSyncer(tctx *tcontext.Context, pos mysql.Position) (err error) {
	c.Lock()
	defer c.Unlock()

	return c.resetReplicationSyncer(tctx, pos)
}

func (c *StreamerController) resetReplicationSyncer(tctx *tcontext.Context, pos mysql.Position) (err error) {
	uuidSameWithUpstream := true

	// close old streamerProducer
	if c.streamerProducer != nil {
		switch t := c.streamerProducer.(type) {
		case *remoteBinlogReader:
			c.closeBinlogSyncer(tctx, t.reader)
		case *localBinlogReader:
			// check the uuid before close
			uuidSameWithUpstream, err = c.checkUUIDSameWithUpstream(pos, t.reader.GetUUIDs())
			if err != nil {
				return err
			}
			t.reader.Close()
		default:
			// some other producers such as mockStreamerProducer, should not re-create
			c.streamer, err = c.streamerProducer.generateStreamer(pos)
			return err
		}
	}

	if c.initBinlogType == LocalBinlog && c.meetError {
		// meetError is true means meets error when get binlog event, in this case use remote binlog as default
		if !uuidSameWithUpstream {
			// if the binlog position's uuid is different from the upstream, can not switch to remote binlog
			tctx.L().Warn("may switch master in upstream, so can not switch local to remote")
		} else {
			c.currentBinlogType = RemoteBinlog
			tctx.L().Warn("meet error when read from local binlog, will switch to remote binlog")
		}
	}

	if c.currentBinlogType == RemoteBinlog {
		c.streamerProducer = &remoteBinlogReader{replication.NewBinlogSyncer(c.syncCfg), tctx, false}
	} else {
		c.streamerProducer = &localBinlogReader{streamer.NewBinlogReader(tctx, &streamer.BinlogReaderConfig{RelayDir: c.localBinlogDir, Timezone: c.timezone})}
	}

	c.streamer, err = c.streamerProducer.generateStreamer(pos)
	return err
}

// RedirectStreamer redirects the streamer's begin position or gtid
func (c *StreamerController) RedirectStreamer(tctx *tcontext.Context, pos mysql.Position) error {
	c.Lock()
	defer c.Unlock()

	tctx.L().Info("redirect streamer", zap.Stringer("position", pos))
	return c.resetReplicationSyncer(tctx, pos)
}

// GetEvent returns binlog event, should only have one thread call this function.
func (c *StreamerController) GetEvent(tctx *tcontext.Context) (event *replication.BinlogEvent, err error) {
	ctx, cancel := context.WithTimeout(tctx.Context(), eventTimeout)
	failpoint.Inject("SyncerEventTimeout", func(val failpoint.Value) {
		if seconds, ok := val.(int); ok {
			cancel()
			ctx, cancel = context.WithTimeout(tctx.Context(), time.Duration(seconds)*time.Second)
			tctx.L().Info("set fetch binlog event timeout", zap.String("failpoint", "SyncerEventTimeout"), zap.Int("value", seconds))
		}
	})

	c.RLock()
	streamer := c.streamer
	c.RUnlock()

	event, err = streamer.GetEvent(ctx)
	cancel()
	if err != nil {
		if err != context.Canceled && err != context.DeadlineExceeded {
			c.Lock()
			c.meetError = true
			c.Unlock()
		}

		return nil, err
	}

	switch ev := event.Event.(type) {
	case *replication.RotateEvent:
		// if is local binlog, binlog's name contain uuid information, need to save it
		// if is remote binlog, need to add uuid information in binlog's name
		c.Lock()
		containUUID := c.setUUIDIfExists(string(ev.NextLogName))
		uuidSuffix := c.uuidSuffix
		c.Unlock()

		if !containUUID {
			if len(uuidSuffix) != 0 {
				filename, err := binlog.ParseFilename(string(ev.NextLogName))
				if err != nil {
					return nil, terror.Annotate(err, "fail to parse binlog file name from rotate event")
				}
				ev.NextLogName = []byte(binlog.ConstructFilenameWithUUIDSuffix(filename, uuidSuffix))
				event.Event = ev
			}
		}
	default:
	}

	return event, nil
}

// ReopenWithRetry reopens streamer with retry
func (c *StreamerController) ReopenWithRetry(tctx *tcontext.Context, pos mysql.Position) error {
	c.Lock()
	defer c.Unlock()

	var err error
	for i := 0; i < maxRetryCount; i++ {
		err = c.resetReplicationSyncer(tctx, pos)
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

func (c *StreamerController) closeBinlogSyncer(logtctx *tcontext.Context, binlogSyncer *replication.BinlogSyncer) error {
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
func (c *StreamerController) Close(tctx *tcontext.Context) {
	c.Lock()
	c.close(tctx)
	c.Unlock()
}

func (c *StreamerController) close(tctx *tcontext.Context) {
	if c.closed {
		return
	}

	if c.streamerProducer != nil {
		switch r := c.streamerProducer.(type) {
		case *remoteBinlogReader:
			// process remote binlog reader
			c.closeBinlogSyncer(tctx, r.reader)
		case *localBinlogReader:
			// process local binlog reader
			r.reader.Close()
		}
		c.streamerProducer = nil
	}

	c.closed = true
}

// IsClosed returns whether streamer controller is closed
func (c *StreamerController) IsClosed() bool {
	c.RLock()
	defer c.RUnlock()

	return c.closed
}

func (c *StreamerController) setUUIDIfExists(filename string) bool {
	_, uuidSuffix, _, err := binlog.SplitFilenameWithUUIDSuffix(filename)
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

// check whether the uuid in binlog position's name is same with upstream
func (c *StreamerController) checkUUIDSameWithUpstream(pos mysql.Position, uuids []string) (bool, error) {
	_, uuidSuffix, _, err := binlog.SplitFilenameWithUUIDSuffix(pos.Name)
	if err != nil {
		// don't contain uuid in position's name
		return true, nil
	}
	uuid := utils.GetUUIDBySuffix(uuids, uuidSuffix)

	upstreamUUID, err := utils.GetServerUUID(c.fromDB.BaseDB.DB, c.syncCfg.Flavor)
	if err != nil {
		return false, terror.Annotate(err, "streamer controller check upstream uuid failed")
	}

	return uuid == upstreamUUID, nil
}

// GetBinlogType returns the binlog type used now
func (c *StreamerController) GetBinlogType() BinlogType {
	c.RLock()
	defer c.RUnlock()
	return c.currentBinlogType
}

// CanRetry returns true if can switch from local to remote and retry again
func (c *StreamerController) CanRetry() bool {
	c.RLock()
	defer c.RUnlock()

	if c.initBinlogType == LocalBinlog && c.currentBinlogType == LocalBinlog {
		return true
	}

	return false
}

func (c *StreamerController) updateServerID(tctx *tcontext.Context) error {
	randomServerID, err := utils.GetRandomServerID(tctx.Context(), c.fromDB.BaseDB.DB)
	if err != nil {
		// should never happened unless the master has too many slave
		return terror.Annotate(err, "fail to get random server id for streamer controller")
	}

	c.syncCfg.ServerID = randomServerID
	c.serverIDUpdated = true
	return nil
}

// UpdateServerIDAndResetReplication updates the server id and reset replication
func (c *StreamerController) UpdateServerIDAndResetReplication(tctx *tcontext.Context, pos mysql.Position) error {
	c.Lock()
	defer c.Unlock()

	return c.updateServerIDAndResetReplication(tctx, pos)
}

func (c *StreamerController) updateServerIDAndResetReplication(tctx *tcontext.Context, pos mysql.Position) error {
	err := c.updateServerID(tctx)
	if err != nil {
		return err
	}

	err = c.resetReplicationSyncer(tctx, pos)
	if err != nil {
		return err
	}

	return nil
}

func isDuplicateServerIDError(err error) bool {
	if err == nil {
		return false
	}

	return strings.Contains(err.Error(), "A slave with the same server_uuid/server_id as this slave has connected to the master")
}
