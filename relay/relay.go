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

package relay

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go/sync2"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/unit"
	"github.com/pingcap/dm/pkg/binlog"
	tcontext "github.com/pingcap/dm/pkg/context"
	fr "github.com/pingcap/dm/pkg/func-rollback"
	"github.com/pingcap/dm/pkg/log"
	pkgstreamer "github.com/pingcap/dm/pkg/streamer"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
	"github.com/pingcap/dm/relay/reader"
	"github.com/pingcap/dm/relay/retry"
	"github.com/pingcap/dm/relay/transformer"
	"github.com/pingcap/dm/relay/writer"
)

var (
	// used to fill RelayLogInfo
	fakeTaskName = "relay"
)

const (
	slaveReadTimeout            = 1 * time.Minute  // slave read binlog data timeout, ref: https://dev.mysql.com/doc/refman/8.0/en/replication-options-slave.html#sysvar_slave_net_timeout
	masterHeartbeatPeriod       = 30 * time.Second // master server send heartbeat period: ref: `MASTER_HEARTBEAT_PERIOD` in https://dev.mysql.com/doc/refman/8.0/en/change-master-to.html
	flushMetaInterval           = 30 * time.Second
	getMasterStatusInterval     = 30 * time.Second
	trimUUIDsInterval           = 1 * time.Hour
	showStatusConnectionTimeout = "1m"

	// dumpFlagSendAnnotateRowsEvent (BINLOG_SEND_ANNOTATE_ROWS_EVENT) request the MariaDB master to send Annotate_rows_log_event back.
	dumpFlagSendAnnotateRowsEvent uint16 = 0x02
)

// NewRelay creates an instance of Relay.
var NewRelay = NewRealRelay

// Process defines mysql-like relay log process unit
type Process interface {
	// Init initial relat log unit
	Init(ctx context.Context) (err error)
	// Process run background logic of relay log unit
	Process(ctx context.Context, pr chan pb.ProcessResult)
	// SwitchMaster switches relay's master server
	SwitchMaster(ctx context.Context, req *pb.SwitchRelayMasterRequest) error
	// Migrate  resets  binlog position
	Migrate(ctx context.Context, binlogName string, binlogPos uint32) error
	// ActiveRelayLog returns the earliest active relay log info in this operator
	ActiveRelayLog() *pkgstreamer.RelayLogInfo
	// Reload reloads config
	Reload(newCfg *Config) error
	// Update updates config
	Update(cfg *config.SubTaskConfig) error
	// Resume resumes paused relay log process unit
	Resume(ctx context.Context, pr chan pb.ProcessResult)
	// Pause pauses a running relay log process unit
	Pause()
	// Error returns error message if having one
	Error() interface{}
	// Status returns status of relay log process unit
	Status() interface{}
	// Close does some clean works
	Close()
	// IsClosed returns whether relay log process unit was closed
	IsClosed() bool
}

// Relay relays mysql binlog to local file.
type Relay struct {
	db        *sql.DB
	cfg       *Config
	syncerCfg replication.BinlogSyncerConfig

	meta   Meta
	closed sync2.AtomicBool
	sync.RWMutex

	tctx *tcontext.Context

	activeRelayLog struct {
		sync.RWMutex
		info *pkgstreamer.RelayLogInfo
	}
}

// NewRealRelay creates an instance of Relay.
func NewRealRelay(cfg *Config) Process {
	syncerCfg := replication.BinlogSyncerConfig{
		ServerID:         uint32(cfg.ServerID),
		Flavor:           cfg.Flavor,
		Host:             cfg.From.Host,
		Port:             uint16(cfg.From.Port),
		User:             cfg.From.User,
		Password:         cfg.From.Password,
		Charset:          cfg.Charset,
		UseDecimal:       true, // must set true. ref: https://github.com/pingcap/tidb-enterprise-tools/pull/272
		ReadTimeout:      slaveReadTimeout,
		HeartbeatPeriod:  masterHeartbeatPeriod,
		VerifyChecksum:   true,
		DisableRetrySync: true, // the retry of go-mysql has some problem now, we disable it and do the retry in relay first.
	}

	if !cfg.EnableGTID {
		// for rawMode(true), we only parse FormatDescriptionEvent and RotateEvent
		// if not need to support GTID mode, we can enable rawMode
		syncerCfg.RawModeEnabled = true
	}

	if cfg.Flavor == mysql.MariaDBFlavor {
		// ref: https://mariadb.com/kb/en/library/annotate_rows_log_event/#slave-option-replicate-annotate-row-events
		// ref: https://github.com/MariaDB/server/blob/bf71d263621c90cbddc7bde9bf071dae503f333f/sql/sql_repl.cc#L1809
		syncerCfg.DumpCommandFlag |= dumpFlagSendAnnotateRowsEvent
	}

	return &Relay{
		cfg:       cfg,
		syncerCfg: syncerCfg,
		meta:      NewLocalMeta(cfg.Flavor, cfg.RelayDir),
		tctx:      tcontext.Background().WithLogger(log.With(zap.String("component", "relay log"))),
	}
}

// Init implements the dm.Unit interface.
func (r *Relay) Init(ctx context.Context) (err error) {
	rollbackHolder := fr.NewRollbackHolder("relay")
	defer func() {
		if err != nil {
			rollbackHolder.RollbackReverseOrder()
		}
	}()

	cfg := r.cfg.From
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&interpolateParams=true&readTimeout=%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, showStatusConnectionTimeout)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return terror.WithScope(terror.DBErrorAdapt(err, terror.ErrDBDriverError), terror.ScopeUpstream)
	}
	r.db = db
	rollbackHolder.Add(fr.FuncRollback{Name: "close-DB", Fn: r.closeDB})

	if err2 := os.MkdirAll(r.cfg.RelayDir, 0755); err2 != nil {
		return terror.ErrRelayMkdir.Delegate(err2)
	}

	err = r.meta.Load()
	if err != nil {
		return err
	}

	if err := reportRelayLogSpaceInBackground(r.cfg.RelayDir); err != nil {
		return err
	}

	return nil
}

// Process implements the dm.Unit interface.
func (r *Relay) Process(ctx context.Context, pr chan pb.ProcessResult) {
	errs := make([]*pb.ProcessError, 0, 1)
	err := r.process(ctx)
	if err != nil && errors.Cause(err) != replication.ErrSyncClosed {
		relayExitWithErrorCounter.Inc()
		r.tctx.L().Error("process exit", zap.Error(err))
		// TODO: add specified error type instead of pb.ErrorType_UnknownError
		errs = append(errs, unit.NewProcessError(pb.ErrorType_UnknownError, err))
	}

	isCanceled := false
	if len(errs) == 0 {
		select {
		case <-ctx.Done():
			isCanceled = true
		default:
		}
	}
	pr <- pb.ProcessResult{
		IsCanceled: isCanceled,
		Errors:     errs,
	}
}

// SwitchMaster switches relay's master server
// before call this from dmctl, you must ensure that relay catches up previous master
// we can not check this automatically in this func because master already changed
// switch master server steps:
//   1. use dmctl to pause relay
//   2. ensure relay catching up current master server (use `query-status`)
//   3. switch master server for upstream
//      * change relay's master config, TODO
//      * change master behind VIP
//   4. use dmctl to switch relay's master server (use `switch-relay-master`)
//   5. use dmctl to resume relay
func (r *Relay) SwitchMaster(ctx context.Context, req *pb.SwitchRelayMasterRequest) error {
	if !r.cfg.EnableGTID {
		return terror.ErrRelaySwitchMasterNeedGTID.Generate()
	}
	return r.reSetupMeta()
}

func (r *Relay) process(parentCtx context.Context) error {
	parser2, err := utils.GetParser(r.db, false) // refine to use user config later
	if err != nil {
		return err
	}

	isNew, err := isNewServer(r.meta.UUID(), r.db, r.cfg.Flavor)
	if err != nil {
		return err
	}

	if isNew {
		// purge old relay log
		err = r.purgeRelayDir()
		if err != nil {
			return err
		}

		// re-setup meta for new server
		err = r.reSetupMeta()
		if err != nil {
			return err
		}
	} else {
		r.updateMetricsRelaySubDirIndex()
		// if not a new server, try to recover the latest relay log file.
		err = r.tryRecoverLatestFile(parser2)
		if err != nil {
			return err
		}
	}

	reader2, err := r.setUpReader()
	if err != nil {
		return err
	}
	defer func() {
		if reader2 != nil {
			err = reader2.Close()
			if err != nil {
				r.tctx.L().Error("fail to close binlog event reader", zap.Error(err))
			}
		}
	}()

	writer2, err := r.setUpWriter(parser2)
	if err != nil {
		return err
	}
	defer func() {
		err = writer2.Close()
		if err != nil {
			r.tctx.L().Error("fail to close binlog event writer", zap.Error(err))
		}
	}()

	readerRetry, err := retry.NewReaderRetry(r.cfg.ReaderRetry)
	if err != nil {
		return err
	}

	transformer2 := transformer.NewTransformer(parser2)

	go r.doIntervalOps(parentCtx)

	// handles binlog events with retry mechanism.
	// it only do the retry for some binlog reader error now.
	for {
		err := r.handleEvents(parentCtx, reader2, transformer2, writer2)
		if err == nil {
			return nil
		} else if !readerRetry.Check(parentCtx, err) {
			return err
		}

		r.tctx.L().Warn("receive retryable error for binlog reader", log.ShortError(err))
		err = reader2.Close() // close the previous reader
		if err != nil {
			r.tctx.L().Error("fail to close binlog event reader", zap.Error(err))
		}
		reader2, err = r.setUpReader() // setup a new one
		if err != nil {
			return err
		}
		r.tctx.L().Info("retrying to read binlog")
	}
}

// purgeRelayDir will clear all contents under w.cfg.RelayDir
func (r *Relay) purgeRelayDir() error {
	dir := r.cfg.RelayDir
	d, err := os.Open(dir)
	// fail to open dir, return directly
	if err != nil {
		if err == os.ErrNotExist {
			return nil
		}
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}
	r.tctx.L().Info("relay dir is purged to be ready for new relay log", zap.String("relayDir", dir))
	return nil
}

// tryRecoverLatestFile tries to recover latest relay log file with corrupt/incomplete binlog events/transactions.
func (r *Relay) tryRecoverLatestFile(parser2 *parser.Parser) error {
	var (
		uuid, latestPos = r.meta.Pos()
		_, latestGTID   = r.meta.GTID()
	)

	if latestPos.Compare(minCheckpoint) <= 0 {
		r.tctx.L().Warn("no relay log file need to recover", zap.Stringer("position", latestPos), log.WrapStringerField("gtid set", latestGTID))
		return nil
	}

	// setup a special writer to do the recovering
	cfg := &writer.FileConfig{
		RelayDir: r.meta.Dir(),
		Filename: latestPos.Name,
	}
	writer2 := writer.NewFileWriter(r.tctx, cfg, parser2)
	err := writer2.Start()
	if err != nil {
		return terror.Annotatef(err, "start recover writer for UUID %s with config %+v", uuid, cfg)
	}
	defer func() {
		err2 := writer2.Close()
		if err2 != nil {
			r.tctx.L().Error("fail to close recover writer", zap.String("UUID", uuid), zap.Reflect("config", cfg), log.ShortError(err2))
		}
	}()
	r.tctx.L().Info("started recover writer", zap.String("UUID", uuid), zap.Reflect("config", cfg))

	result, err := writer2.Recover()
	if err == nil {
		if result.Recovered {
			r.tctx.L().Warn("relay log file recovered",
				zap.Stringer("from position", latestPos), zap.Stringer("to position", result.LatestPos), log.WrapStringerField("from GTID set", latestGTID), log.WrapStringerField("to GTID set", result.LatestGTIDs))
			if err = latestGTID.Truncate(result.LatestGTIDs); err != nil {
				return err
			}
			err = r.meta.Save(result.LatestPos, latestGTID)
			if err != nil {
				return terror.Annotatef(err, "save position %s, GTID sets %v after recovered", result.LatestPos, result.LatestGTIDs)
			}
		} else if result.LatestPos.Compare(latestPos) > 0 ||
			(result.LatestGTIDs != nil && !result.LatestGTIDs.Equal(latestGTID) && result.LatestGTIDs.Contain(latestGTID)) {
			r.tctx.L().Warn("relay log file have more events",
				zap.Stringer("after position", latestPos), zap.Stringer("until position", result.LatestPos), log.WrapStringerField("after GTID set", latestGTID), log.WrapStringerField("until GTID set", result.LatestGTIDs))
		}

	}
	return terror.Annotatef(err, "recover for UUID %s with config %+v", uuid, cfg)
}

// handleEvents handles binlog events, including:
//   1. read events from upstream
//   2. transform events
//   3. write events into relay log files
//   4. update metadata if needed
func (r *Relay) handleEvents(ctx context.Context, reader2 reader.Reader, transformer2 transformer.Transformer, writer2 writer.Writer) error {
	var (
		_, lastPos  = r.meta.Pos()
		_, lastGTID = r.meta.GTID()
	)

	for {
		// 1. read events from upstream server
		readTimer := time.Now()
		rResult, err := reader2.GetEvent(ctx)
		if err != nil {
			switch errors.Cause(err) {
			case context.Canceled:
				return nil
			case replication.ErrChecksumMismatch:
				relayLogDataCorruptionCounter.Inc()
			case replication.ErrSyncClosed, replication.ErrNeedSyncAgain:
				// do nothing, but the error will be returned
			default:
				if utils.IsErrBinlogPurged(err) {
					// TODO: try auto fix GTID, and can support auto switching between upstream server later.
					cfg := r.cfg.From
					r.tctx.L().Error("the requested binlog files have purged in the master server or the master server have switched, currently DM do no support to handle this error",
						zap.String("db host", cfg.Host), zap.Int("db port", cfg.Port), zap.Stringer("last pos", lastPos), log.ShortError(err))
					// log the status for debug
					pos, gs, err2 := utils.GetMasterStatus(r.db, r.cfg.Flavor)
					if err2 == nil {
						r.tctx.L().Info("current master status", zap.Stringer("position", pos), log.WrapStringerField("GTID sets", gs))
					}
				}
				binlogReadErrorCounter.Inc()
			}
			return err
		}

		binlogReadDurationHistogram.Observe(time.Since(readTimer).Seconds())
		failpoint.Inject("BlackholeReadBinlog", func(_ failpoint.Value) {
			//r.tctx.L().Info("back hole read binlog takes effects")
			failpoint.Continue()
		})

		e := rResult.Event
		r.tctx.L().Debug("receive binlog event with header", zap.Reflect("header", e.Header))

		// 2. transform events
		transformTimer := time.Now()
		tResult := transformer2.Transform(e)
		binlogTransformDurationHistogram.Observe(time.Since(transformTimer).Seconds())
		if len(tResult.NextLogName) > 0 && tResult.NextLogName > lastPos.Name {
			lastPos = mysql.Position{
				Name: string(tResult.NextLogName),
				Pos:  uint32(tResult.LogPos),
			}
			r.tctx.L().Info("rotate event", zap.Stringer("position", lastPos))
		}
		if tResult.Ignore {
			r.tctx.L().Info("ignore event by transformer",
				zap.Reflect("header", e.Header),
				zap.String("reason", tResult.IgnoreReason))
			continue
		}

		// 3. save events into file
		writeTimer := time.Now()
		r.tctx.L().Debug("writing binlog event", zap.Reflect("header", e.Header))
		wResult, err := writer2.WriteEvent(e)
		if err != nil {
			relayLogWriteErrorCounter.Inc()
			return err
		} else if wResult.Ignore {
			r.tctx.L().Info("ignore event by writer",
				zap.Reflect("header", e.Header),
				zap.String("reason", wResult.IgnoreReason))
			r.tryUpdateActiveRelayLog(e, lastPos.Name) // even the event ignored we still need to try this update.
			continue
		}
		relayLogWriteDurationHistogram.Observe(time.Since(writeTimer).Seconds())
		r.tryUpdateActiveRelayLog(e, lastPos.Name) // wrote a event, try update the current active relay log.

		// 4. update meta and metrics
		needSavePos := tResult.CanSaveGTID
		lastPos.Pos = tResult.LogPos
		err = lastGTID.Set(tResult.GTIDSet)
		if err != nil {
			return terror.ErrRelayUpdateGTID.Delegate(err, lastGTID, tResult.GTIDSet)
		}
		if !r.cfg.EnableGTID {
			// if go-mysql set RawModeEnabled to true
			// then it will only parse FormatDescriptionEvent and RotateEvent
			// then check `e.Event.(type)` for `QueryEvent` and `XIDEvent` will never be true
			// so we need to update pos for all events
			// and also save pos for all events
			if e.Header.EventType != replication.ROTATE_EVENT {
				lastPos.Pos = e.Header.LogPos // for RotateEvent, lastPos updated to the next binlog file's position.
			}
			needSavePos = true
		}

		relayLogWriteSizeHistogram.Observe(float64(e.Header.EventSize))
		relayLogPosGauge.WithLabelValues("relay").Set(float64(lastPos.Pos))
		if index, err2 := binlog.GetFilenameIndex(lastPos.Name); err2 != nil {
			r.tctx.L().Error("parse binlog file name", zap.String("file name", lastPos.Name), log.ShortError(err2))
		} else {
			relayLogFileGauge.WithLabelValues("relay").Set(float64(index))
		}

		if needSavePos {
			err = r.meta.Save(lastPos, lastGTID)
			if err != nil {
				return terror.Annotatef(err, "save position %s, GTID sets %v into meta", lastPos, lastGTID)
			}
		}
	}
}

// tryUpdateActiveRelayLog tries to update current active relay log file.
// we should to update after received/wrote a FormatDescriptionEvent because it means switched to a new relay log file.
// NOTE: we can refactor active (writer/read) relay log mechanism later.
func (r *Relay) tryUpdateActiveRelayLog(e *replication.BinlogEvent, filename string) {
	if e.Header.EventType == replication.FORMAT_DESCRIPTION_EVENT {
		r.setActiveRelayLog(filename)
		r.tctx.L().Info("change the active relay log file", zap.String("file name", filename))
	}
}

// reSetupMeta re-setup the metadata when switching to a new upstream master server.
func (r *Relay) reSetupMeta() error {
	uuid, err := utils.GetServerUUID(r.db, r.cfg.Flavor)
	if err != nil {
		return err
	}
	err = r.meta.AddDir(uuid, nil, nil)
	if err != nil {
		return err
	}
	err = r.meta.Load()
	if err != nil {
		return err
	}

	var latestPosName, latestGTIDStr string
	if (r.cfg.EnableGTID && len(r.cfg.BinlogGTID) == 0) || (!r.cfg.EnableGTID && len(r.cfg.BinLogName) == 0) {
		latestPos, latestGTID, err2 := utils.GetMasterStatus(r.db, r.cfg.Flavor)
		if err2 != nil {
			return err2
		}
		latestPosName = latestPos.Name
		latestGTIDStr = latestGTID.String()
	}

	// try adjust meta with start pos from config
	adjusted, err := r.meta.AdjustWithStartPos(r.cfg.BinLogName, r.cfg.BinlogGTID, r.cfg.EnableGTID, latestPosName, latestGTIDStr)
	if err != nil {
		return err
	}

	if adjusted {
		_, pos := r.meta.Pos()
		_, gtid := r.meta.GTID()
		r.tctx.L().Info("adjusted meta to start pos", zap.Reflect("start pos", pos), zap.Stringer("start pos's binlog gtid", gtid))
	}

	r.updateMetricsRelaySubDirIndex()

	return nil
}

func (r *Relay) updateMetricsRelaySubDirIndex() {
	// when switching master server, update sub dir index metrics
	node := r.masterNode()
	uuidWithSuffix := r.meta.UUID() // only change after switch
	_, suffix, err := utils.ParseSuffixForUUID(uuidWithSuffix)
	if err != nil {
		r.tctx.L().Error("parse suffix for UUID", zap.String("UUID", uuidWithSuffix), zap.Error(err))
		return
	}
	relaySubDirIndex.WithLabelValues(node, uuidWithSuffix).Set(float64(suffix))
}

func (r *Relay) doIntervalOps(ctx context.Context) {
	flushTicker := time.NewTicker(flushMetaInterval)
	defer flushTicker.Stop()
	masterStatusTicker := time.NewTicker(getMasterStatusInterval)
	defer masterStatusTicker.Stop()
	trimUUIDsTicker := time.NewTicker(trimUUIDsInterval)
	defer trimUUIDsTicker.Stop()

	for {
		select {
		case <-flushTicker.C:
			if r.meta.Dirty() {
				err := r.meta.Flush()
				if err != nil {
					r.tctx.L().Error("flush meta", zap.Error(err))
				} else {
					r.tctx.L().Info("flush meta finished", zap.Stringer("meta", r.meta))
				}
			}
		case <-masterStatusTicker.C:
			pos, _, err := utils.GetMasterStatus(r.db, r.cfg.Flavor)
			if err != nil {
				r.tctx.L().Warn("get master status", zap.Error(err))
				continue
			}
			index, err := binlog.GetFilenameIndex(pos.Name)
			if err != nil {
				r.tctx.L().Error("parse binlog file name", zap.String("file name", pos.Name), log.ShortError(err))
				continue
			}
			relayLogFileGauge.WithLabelValues("master").Set(float64(index))
			relayLogPosGauge.WithLabelValues("master").Set(float64(pos.Pos))
		case <-trimUUIDsTicker.C:
			trimmed, err := r.meta.TrimUUIDs()
			if err != nil {
				r.tctx.L().Error("trim UUIDs", zap.Error(err))
			} else if len(trimmed) > 0 {
				r.tctx.L().Info("trim UUIDs", zap.String("UUIDs", strings.Join(trimmed, ";")))
			}
		case <-ctx.Done():
			return
		}
	}
}

// setUpReader setups the underlying reader used to read binlog events from the upstream master server.
func (r *Relay) setUpReader() (reader.Reader, error) {
	uuid, pos := r.meta.Pos()
	_, gs := r.meta.GTID()
	cfg := &reader.Config{
		SyncConfig: r.syncerCfg,
		Pos:        pos,
		GTIDs:      gs,
		MasterID:   r.masterNode(),
		EnableGTID: r.cfg.EnableGTID,
	}

	reader2 := reader.NewReader(cfg)
	err := reader2.Start()
	if err != nil {
		// do not log the whole config to protect the password in `SyncConfig`.
		// and other config items should already logged before or included in `err`.
		return nil, terror.Annotatef(err, "start reader for UUID %s", uuid)
	}

	r.tctx.L().Info("started underlying reader", zap.String("UUID", uuid))
	return reader2, nil
}

// setUpWriter setups the underlying writer used to writer binlog events into file or other places.
func (r *Relay) setUpWriter(parser2 *parser.Parser) (writer.Writer, error) {
	uuid, pos := r.meta.Pos()
	cfg := &writer.FileConfig{
		RelayDir: r.meta.Dir(),
		Filename: pos.Name,
	}
	writer2 := writer.NewFileWriter(r.tctx, cfg, parser2)
	err := writer2.Start()
	if err != nil {
		return nil, terror.Annotatef(err, "start writer for UUID %s with config %+v", uuid, cfg)
	}

	r.tctx.L().Info("started underlying writer", zap.String("UUID", uuid), zap.Reflect("config", cfg))
	return writer2, nil
}

func (r *Relay) masterNode() string {
	return fmt.Sprintf("%s:%d", r.cfg.From.Host, r.cfg.From.Port)
}

// IsClosed tells whether Relay unit is closed or not.
func (r *Relay) IsClosed() bool {
	return r.closed.Get()
}

// stopSync stops syncing, now it used by Close and Pause
func (r *Relay) stopSync() {
	if err := r.meta.Flush(); err != nil {
		r.tctx.L().Error("flush checkpoint", zap.Error(err))
	}
}

func (r *Relay) closeDB() {
	if r.db != nil {
		r.db.Close()
		r.db = nil
	}
}

// Close implements the dm.Unit interface.
func (r *Relay) Close() {
	r.Lock()
	defer r.Unlock()
	if r.closed.Get() {
		return
	}
	r.tctx.L().Info("relay unit is closing")

	r.stopSync()

	r.closeDB()

	r.closed.Set(true)
	r.tctx.L().Info("relay unit closed")
}

// Status implements the dm.Unit interface.
func (r *Relay) Status() interface{} {
	masterPos, masterGTID, err := utils.GetMasterStatus(r.db, r.cfg.Flavor)
	if err != nil {
		r.tctx.L().Warn("get master status", zap.Error(err))
	}

	uuid, relayPos := r.meta.Pos()
	_, relayGTIDSet := r.meta.GTID()
	rs := &pb.RelayStatus{
		MasterBinlog: masterPos.String(),
		RelaySubDir:  uuid,
		RelayBinlog:  relayPos.String(),
	}
	if masterGTID != nil { // masterGTID maybe a nil interface
		rs.MasterBinlogGtid = masterGTID.String()
	}
	if relayGTIDSet != nil {
		rs.RelayBinlogGtid = relayGTIDSet.String()
	}
	if r.cfg.EnableGTID {
		if masterGTID != nil && relayGTIDSet != nil && relayGTIDSet.Equal(masterGTID) {
			rs.RelayCatchUpMaster = true
		}
	} else {
		rs.RelayCatchUpMaster = masterPos.Compare(relayPos) == 0
	}
	return rs
}

// Error implements the dm.Unit interface.
func (r *Relay) Error() interface{} {
	return &pb.RelayError{}
}

// Type implements the dm.Unit interface.
func (r *Relay) Type() pb.UnitType {
	return pb.UnitType_Relay
}

// IsFreshTask implements Unit.IsFreshTask
func (r *Relay) IsFreshTask() (bool, error) {
	return true, nil
}

// Pause pauses the process, it can be resumed later
func (r *Relay) Pause() {
	if r.IsClosed() {
		r.tctx.L().Warn("try to pause, but already closed")
		return
	}

	r.stopSync()
}

// Resume resumes the paused process
func (r *Relay) Resume(ctx context.Context, pr chan pb.ProcessResult) {
	// do nothing now, re-process called `Process` from outer directly
}

// Update implements Unit.Update
func (r *Relay) Update(cfg *config.SubTaskConfig) error {
	// not support update configuration now
	return nil
}

// Reload updates config
func (r *Relay) Reload(newCfg *Config) error {
	r.Lock()
	defer r.Unlock()
	r.tctx.L().Info("relay unit is updating")

	// Update From
	r.cfg.From = newCfg.From

	// Update AutoFixGTID
	r.cfg.AutoFixGTID = newCfg.AutoFixGTID

	// Update Charset
	r.cfg.Charset = newCfg.Charset

	r.closeDB()
	cfg := r.cfg.From
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&interpolateParams=true&readTimeout=%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, showStatusConnectionTimeout)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return terror.WithScope(terror.DBErrorAdapt(err, terror.ErrDBDriverError), terror.ScopeUpstream)
	}
	r.db = db

	syncerCfg := replication.BinlogSyncerConfig{
		ServerID:        uint32(r.cfg.ServerID),
		Flavor:          r.cfg.Flavor,
		Host:            newCfg.From.Host,
		Port:            uint16(newCfg.From.Port),
		User:            newCfg.From.User,
		Password:        newCfg.From.Password,
		Charset:         newCfg.Charset,
		UseDecimal:      true, // must set true. ref: https://github.com/pingcap/dm/pull/272
		ReadTimeout:     slaveReadTimeout,
		HeartbeatPeriod: masterHeartbeatPeriod,
		VerifyChecksum:  true,
	}

	if !newCfg.EnableGTID {
		// for rawMode(true), we only parse FormatDescriptionEvent and RotateEvent
		// if not need to support GTID mode, we can enable rawMode
		syncerCfg.RawModeEnabled = true
	}

	r.syncerCfg = syncerCfg

	r.tctx.L().Info("relay unit is updated")

	return nil
}

// setActiveRelayLog sets or updates the current active relay log to file
func (r *Relay) setActiveRelayLog(filename string) {
	uuid := r.meta.UUID()
	_, suffix, _ := utils.ParseSuffixForUUID(uuid)
	rli := &pkgstreamer.RelayLogInfo{
		TaskName:   fakeTaskName,
		UUID:       uuid,
		UUIDSuffix: suffix,
		Filename:   filename,
	}
	r.activeRelayLog.Lock()
	r.activeRelayLog.info = rli
	r.activeRelayLog.Unlock()
}

// ActiveRelayLog returns the current active RelayLogInfo
func (r *Relay) ActiveRelayLog() *pkgstreamer.RelayLogInfo {
	r.activeRelayLog.RLock()
	defer r.activeRelayLog.RUnlock()
	return r.activeRelayLog.info
}

// Migrate reset binlog pos and name, create sub dir
func (r *Relay) Migrate(ctx context.Context, binlogName string, binlogPos uint32) error {
	r.Lock()
	defer r.Unlock()
	uuid, err := utils.GetServerUUID(r.db, r.cfg.Flavor)
	if err != nil {
		return err
	}
	err = r.meta.AddDir(uuid, &mysql.Position{Name: binlogName, Pos: binlogPos}, nil)
	if err != nil {
		return err
	}
	return nil
}
