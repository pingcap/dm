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
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go/sync2"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/unit"
	"github.com/pingcap/dm/pkg/binlog"
	binlogreader "github.com/pingcap/dm/pkg/binlog/reader"
	fr "github.com/pingcap/dm/pkg/func-rollback"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/log"
	pkgstreamer "github.com/pingcap/dm/pkg/streamer"
	"github.com/pingcap/dm/pkg/utils"
)

var (
	// logger writes log start with `[component=relay]`
	logger log.Logger

	// ErrBinlogPosGreaterThanFileSize represents binlog pos greater than file size
	ErrBinlogPosGreaterThanFileSize = errors.New("the specific position is greater than the local binlog file size")
	// ErrNoIncompleteEventFound represents no incomplete event found in relay log file
	ErrNoIncompleteEventFound = errors.New("no incomplete event found in relay log file")
	// used to fill RelayLogInfo
	fakeTaskName = "relay"
)

const (
	eventTimeout                = 1 * time.Hour
	slaveReadTimeout            = 1 * time.Minute  // slave read binlog data timeout, ref: https://dev.mysql.com/doc/refman/8.0/en/replication-options-slave.html#sysvar_slave_net_timeout
	masterHeartbeatPeriod       = 30 * time.Second // master server send heartbeat period: ref: `MASTER_HEARTBEAT_PERIOD` in https://dev.mysql.com/doc/refman/8.0/en/change-master-to.html
	flushMetaInterval           = 30 * time.Second
	getMasterStatusInterval     = 30 * time.Second
	trimUUIDsInterval           = 1 * time.Hour
	binlogHeaderSize            = 4
	showStatusConnectionTimeout = "1m"

	// dumpFlagSendAnnotateRowsEvent (BINLOG_SEND_ANNOTATE_ROWS_EVENT) request the MariaDB master to send Annotate_rows_log_event back.
	dumpFlagSendAnnotateRowsEvent uint16 = 0x02
)

func init() {
	logger = log.With(zap.String("component", "relay"))
}

// NewRelay creates an instance of Relay.
var NewRelay = NewRealRelay

// Process defines mysql-like relay log process unit
type Process interface {
	// Init initial relat log unit
	Init() (err error)
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
	reader    binlogreader.Reader

	gapSyncerCfg          replication.BinlogSyncerConfig
	meta                  Meta
	lastSlaveConnectionID uint32
	fd                    *os.File
	closed                sync2.AtomicBool
	gSetWhenSwitch        gtid.Set // GTID set when master-slave switching or the first startup
	sync.RWMutex

	activeRelayLog struct {
		sync.RWMutex
		info *pkgstreamer.RelayLogInfo
	}
}

// NewRealRelay creates an instance of Relay.
func NewRealRelay(cfg *Config) Process {
	syncerCfg := replication.BinlogSyncerConfig{
		ServerID:        uint32(cfg.ServerID),
		Flavor:          cfg.Flavor,
		Host:            cfg.From.Host,
		Port:            uint16(cfg.From.Port),
		User:            cfg.From.User,
		Password:        cfg.From.Password,
		Charset:         cfg.Charset,
		UseDecimal:      true, // must set true. ref: https://github.com/pingcap/tidb-enterprise-tools/pull/272
		ReadTimeout:     slaveReadTimeout,
		HeartbeatPeriod: masterHeartbeatPeriod,
		VerifyChecksum:  true,
	}

	// use 2**32 -1 - ServerID as new ServerID to fill the gap in relay log file
	gapSyncerCfg := syncerCfg
	gapSyncerCfg.ServerID = math.MaxUint32 - syncerCfg.ServerID
	// for gap streamer, we always use rawMode to avoid `invalid table id, no corresponding table map event` error event starting sync from a RowsEvent
	gapSyncerCfg.RawModeEnabled = true

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
		cfg:          cfg,
		syncerCfg:    syncerCfg,
		gapSyncerCfg: gapSyncerCfg,
		meta:         NewLocalMeta(cfg.Flavor, cfg.RelayDir),
	}
}

// Init implements the dm.Unit interface.
func (r *Relay) Init() (err error) {
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
		return errors.Trace(err)
	}
	r.db = db
	rollbackHolder.Add(fr.FuncRollback{Name: "close-DB", Fn: r.closeDB})

	if err2 := os.MkdirAll(r.cfg.RelayDir, 0755); err2 != nil {
		return errors.Trace(err2)
	}

	err = r.meta.Load()
	if err != nil {
		return errors.Trace(err)
	}

	if err := reportRelayLogSpaceInBackground(r.cfg.RelayDir); err != nil {
		return errors.Trace(err)
	}

	return nil
}

// Process implements the dm.Unit interface.
func (r *Relay) Process(ctx context.Context, pr chan pb.ProcessResult) {
	r.reader = binlogreader.NewTCPReader(r.syncerCfg)

	errs := make([]*pb.ProcessError, 0, 1)
	err := r.process(ctx)
	if err != nil && errors.Cause(err) != replication.ErrSyncClosed {
		relayExitWithErrorCounter.Inc()
		logger.Error("process exit", zap.Error(err))
		// TODO: add specified error type instead of pb.ErrorType_UnknownError
		errs = append(errs, unit.NewProcessError(pb.ErrorType_UnknownError, errors.ErrorStack(err)))
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
		return errors.New("can only switch relay's master server when GTID enabled")
	}
	err := r.reSetupMeta()
	return errors.Trace(err)
}

func (r *Relay) process(parentCtx context.Context) error {
	parser2, err := utils.GetParser(r.db, false) // refine to use user config later
	if err != nil {
		return errors.Trace(err)
	}

	isNew, err := r.isNewServer()
	if err != nil {
		return errors.Trace(err)
	}
	if isNew {
		// re-setup meta for new server
		err = r.reSetupMeta()
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		r.updateMetricsRelaySubDirIndex()
	}

	err = r.setUpReader()
	if err != nil {
		return errors.Trace(err)
	}

	var (
		_, lastPos   = r.meta.Pos()
		_, lastGTID  = r.meta.GTID()
		tryReSync    = true  // used to handle master-slave switch
		addRelayFlag = false // whether needing to add a LOG_EVENT_RELAY_LOG_F flag to the event

		// fill gap steps:
		// 1. record the pos after the gap
		// 2. create a special streamer to fill the gap
		// 3. catchup pos after the gap
		// 4. close the special streamer
		gapReader       binlogreader.Reader
		gapSyncStartPos *mysql.Position                     // the pos of the event starting to fill the gap
		gapSyncEndPos   *mysql.Position                     // the pos of the event after the gap
		eventFormat     *replication.FormatDescriptionEvent // latest FormatDescriptionEvent, used when re-calculate checksum
	)

	closeGapSyncer := func() {
		addRelayFlag = false // reset to false when closing the gap syncer
		if gapReader != nil {
			gapReader.Close()
			gapReader = nil
		}
		gapSyncStartPos = nil
		gapSyncEndPos = nil
	}

	defer func() {
		closeGapSyncer()
		if r.fd != nil {
			r.fd.Close()
		}
	}()

	go r.doIntervalOps(parentCtx)

	for {
		if gapReader == nil && gapSyncStartPos != nil && gapSyncEndPos != nil {
			gapReader = binlogreader.NewTCPReader(r.gapSyncerCfg)
			err = gapReader.StartSyncByPos(*gapSyncStartPos)
			if err != nil {
				return errors.Annotatef(err, "start to fill gap in relay log file from %v", gapSyncStartPos)
			}
			logger.Info("start to fill gap in relay log file", zap.Reflect("from", gapSyncStartPos))
			if r.cfg.EnableGTID {
				_, currentGtidSet := r.meta.GTID()
				if currentGtidSet.Equal(r.gSetWhenSwitch) {
					addRelayFlag = true
					logger.Info("binlog events in the gap for file already exists in previous sub directory, adding a LOG_EVENT_RELAY_LOG_F flag to them.", zap.String("file", r.fd.Name()), zap.Stringer("current GTID set", currentGtidSet))
				} else if currentGtidSet.Contain(r.gSetWhenSwitch) {
					// only after the first gap filled, currentGtidSet can be the superset of gSetWhenSwitch
					logger.Info("binlog events in the gap for file not exists in previous sub directory, no need to add a LOG_EVENT_RELAY_LOG_F flag.", zap.String("file", r.fd.Name()), zap.Stringer("current GTID set", currentGtidSet), zap.Stringer("previous sub directory end GTID set", r.gSetWhenSwitch))
				} else {
					return errors.Errorf("currnet GTID set %s is not equal or superset of previous sub directory end GTID set %s", currentGtidSet, r.gSetWhenSwitch)
				}
			}
		}

		ctx, cancel := context.WithTimeout(parentCtx, eventTimeout)
		readTimer := time.Now()
		var e *replication.BinlogEvent
		if gapReader != nil {
			e, err = gapReader.GetEvent(ctx)
		} else {
			e, err = r.reader.GetEvent(ctx)
		}
		cancel()
		binlogReadDurationHistogram.Observe(time.Since(readTimer).Seconds())

		if err != nil {
			switch errors.Cause(err) {
			case context.Canceled:
				return nil
			case context.DeadlineExceeded:
				logger.Info("deadline exceeded, no binlog event received", zap.Duration("dedaline", eventTimeout))
				continue
			case replication.ErrChecksumMismatch:
				relayLogDataCorruptionCounter.Inc()
			case replication.ErrSyncClosed, replication.ErrNeedSyncAgain:
				// do nothing
			default:
				if utils.IsErrBinlogPurged(err) {
					if gapReader != nil {
						pos, err2 := tryFindGapStartPos(err, r.fd.Name())
						if err2 == nil {
							logger.Error("try find gap start pos", log.ShortError(err2)) // log the error
							gapSyncEndPos2 := *gapSyncEndPos                             // save the endPos
							closeGapSyncer()                                             // close the previous gap streamer
							_, err2 = r.fd.Seek(int64(pos), io.SeekStart)                // seek to a valid pos
							if err2 != nil {
								return errors.Annotatef(err, "seek %s to pos %d", r.fd.Name(), pos)
							}
							err2 = r.fd.Truncate(int64(pos)) // truncate the incomplete event part
							if err2 != nil {
								return errors.Annotatef(err, "truncate %s to size %d", r.fd.Name(), pos)
							}
							logger.Info("truncate file", zap.String("file", r.fd.Name()), zap.Reflect("to size", pos))
							gapSyncStartPos = &mysql.Position{
								Name: lastPos.Name,
								Pos:  pos,
							}
							gapSyncEndPos = &gapSyncEndPos2 // reset the endPos
							logger.Info("adjust gap streamer's start pos", zap.Reflect("pos", pos))
							continue
						}
						return errors.Annotatef(err, "gap streamer")
					}
					if tryReSync && r.cfg.EnableGTID && r.cfg.AutoFixGTID {
						err = r.reSetUpReader(r.syncerCfg)
						if err != nil {
							return errors.Annotatef(err, "try auto switch with GTID")
						}
						tryReSync = false // do not support repeat try re-sync
						continue
					}
				}
				binlogReadErrorCounter.Inc()
			}
			return errors.Trace(err)
		}
		tryReSync = true

		needSavePos := false

		logger.Debug("receive binlog event", zap.Reflect("binlog header", e.Header))
		switch ev := e.Event.(type) {
		case *replication.FormatDescriptionEvent:
			// FormatDescriptionEvent is the first event in binlog, we will close old one and create a new
			eventFormat = ev // record FormatDescriptionEvent
			exist, err := r.handleFormatDescriptionEvent(lastPos.Name)
			if err != nil {
				return errors.Trace(err)
			}
			if exist {
				// exists previously, skip
				continue
			}
		case *replication.RotateEvent:
			// for RotateEvent, update binlog name
			currentPos := mysql.Position{
				Name: string(ev.NextLogName),
				Pos:  uint32(ev.Position),
			}
			if currentPos.Name > lastPos.Name {
				lastPos = currentPos
			}
			logger.Info("rotate to pos", zap.Stringer("pos", lastPos))
			if e.Header.Timestamp == 0 || e.Header.LogPos == 0 {
				// skip fake rotate event
				continue
			}
		case *replication.QueryEvent:
			// when RawModeEnabled not true, QueryEvent will be parsed
			// even for `BEGIN`, we still update pos / GTID
			lastPos.Pos = e.Header.LogPos
			lastGTID.Set(ev.GSet) // in order to call `ev.GSet`, can not combine QueryEvent and XIDEvent
			isDDL := checkIsDDL(string(ev.Query), parser2)
			if isDDL {
				needSavePos = true // need save pos for DDL
			}
		case *replication.XIDEvent:
			// when RawModeEnabled not true, XIDEvent will be parsed
			lastPos.Pos = e.Header.LogPos
			lastGTID.Set(ev.GSet)
			needSavePos = true // need save pos for XID
		case *replication.GenericEvent:
			// handle some un-parsed events
			switch e.Header.EventType {
			case replication.HEARTBEAT_EVENT:
				// skip artificial heartbeat event
				// ref: https://dev.mysql.com/doc/internals/en/heartbeat-event.html
				continue
			}
		default:
			if e.Header.Flags&0x0020 != 0 {
				// skip events with LOG_EVENT_ARTIFICIAL_F flag set
				// ref: https://dev.mysql.com/doc/internals/en/binlog-event-flag.html
				logger.Warn("skip artificial event", zap.Reflect("event header", e.Header))
				continue
			}
		}

		if gapReader == nil {
			gapDetected, fSize, err := r.detectGap(e)
			if err != nil {
				relayLogWriteErrorCounter.Inc()
				return errors.Annotatef(err, "detect relay log file gap for event %+v", e.Header)
			}
			if gapDetected {
				gapSyncStartPos = &mysql.Position{
					Name: lastPos.Name,
					Pos:  fSize,
				}
				gapSyncEndPos = &mysql.Position{
					Name: lastPos.Name,
					Pos:  e.Header.LogPos,
				}
				logger.Info("gap detected from size1 to size2", zap.Uint32("size1", fSize), zap.Uint32("size2", e.Header.LogPos-e.Header.EventSize), zap.String("file", lastPos.Name), zap.Reflect("current event", e.Header))
				continue // skip this event after the gap, it will be wrote to the file when filling the gap
			}
		} else {
			// why check gapSyncEndPos != nil?
			if gapSyncEndPos != nil && e.Header.LogPos >= gapSyncEndPos.Pos {
				// catch up, after write this event, gap will be filled
				logger.Info("fill gap reaching the end pos", zap.Stringer("end pos", gapSyncEndPos), zap.Reflect("current event", e.Header))
				closeGapSyncer()
			} else if addRelayFlag {
				// add LOG_EVENT_RELAY_LOG_F flag to events which used to fill the gap
				// ref: https://dev.mysql.com/doc/internals/en/binlog-event-flag.html
				r.addFlagToEvent(e, 0x0040, eventFormat)
			}
		}

		cmp, fSize, err := r.compareEventWithFileSize(e)
		if err != nil {
			relayLogWriteErrorCounter.Inc()
			return errors.Trace(err)
		}
		if cmp < 0 {
			logger.Warn("skip obsolete event", zap.Reflect("obsolete event", e.Header), zap.Uint32("relay file size", fSize))
			continue
		} else if cmp > 0 {
			relayLogWriteErrorCounter.Inc()
			return errors.Errorf("some events missing, current event %+v, lastPos %v, current GTID %v, relay file size %d", e.Header, lastPos, lastGTID, fSize)
		}

		if !r.cfg.EnableGTID {
			// if go-mysql set RawModeEnabled to true
			// then it will only parse FormatDescriptionEvent and RotateEvent
			// then check `e.Event.(type)` for `QueryEvent` and `XIDEvent` will never be true
			// so we need to update pos for all events
			// and also save pos for all events
			lastPos.Pos = e.Header.LogPos
			needSavePos = true
		}

		writeTimer := time.Now()
		logger.Debug("writing binlog event", zap.Reflect("event header", e.Header))
		if n, err2 := r.fd.Write(e.RawData); err2 != nil {
			relayLogWriteErrorCounter.Inc()
			return errors.Trace(err2)
		} else if n != len(e.RawData) {
			relayLogWriteErrorCounter.Inc()
			// FIXME: should we panic here? it seems unreachable
			return errors.Trace(io.ErrShortWrite)
		}

		relayLogWriteDurationHistogram.Observe(time.Since(writeTimer).Seconds())
		relayLogWriteSizeHistogram.Observe(float64(e.Header.EventSize))
		relayLogPosGauge.WithLabelValues("relay").Set(float64(lastPos.Pos))
		if index, err2 := binlog.GetFilenameIndex(lastPos.Name); err2 != nil {
			logger.Error("parse binlog file name", zap.String("file name", lastPos.Name), log.ShortError(err2))
		} else {
			relayLogFileGauge.WithLabelValues("relay").Set(float64(index))
		}

		if needSavePos {
			err = r.meta.Save(lastPos, lastGTID)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
}

// addFlagToEvent adds flag to binlog event
func (r *Relay) addFlagToEvent(e *replication.BinlogEvent, f uint16, eventFormat *replication.FormatDescriptionEvent) {
	newF := e.Header.Flags | f
	// header structure:
	// 4 byte timestamp
	// 1 byte event
	// 4 byte server-id
	// 4 byte event size
	// 4 byte log pos
	// 2 byte flags
	startIdx := 4 + 1 + 4 + 4 + 4
	binary.LittleEndian.PutUint16(e.RawData[startIdx:startIdx+2], newF)
	e.Header.Flags = newF

	// re-calculate checksum if needed
	if eventFormat == nil || eventFormat.ChecksumAlgorithm != replication.BINLOG_CHECKSUM_ALG_CRC32 {
		return
	}
	calculatedPart := e.RawData[0 : len(e.RawData)-replication.BinlogChecksumLength]
	checksum := crc32.ChecksumIEEE(calculatedPart)
	binary.LittleEndian.PutUint32(e.RawData[len(e.RawData)-replication.BinlogChecksumLength:], checksum)
}

// detectGap detects whether gap exists in relay log file
func (r *Relay) detectGap(e *replication.BinlogEvent) (bool, uint32, error) {
	if r.fd == nil {
		return false, 0, nil
	}
	fi, err := r.fd.Stat()
	if err != nil {
		return false, 0, errors.Trace(err)
	}

	size := uint32(fi.Size())
	if e.Header.LogPos-e.Header.EventSize > size {
		return true, size, nil
	}
	return false, size, nil
}

// compareEventWithFileSize compares event's start pos with relay log file size
// returns result:
//   -1: less than file size
//    0: equal file size
//    1: greater than file size
func (r *Relay) compareEventWithFileSize(e *replication.BinlogEvent) (result int, fSize uint32, err error) {
	if r.fd != nil {
		fi, err := r.fd.Stat()
		if err != nil {
			return 0, 0, errors.Annotatef(err, "compare relay log file size with %+v", e.Header)
		}
		fSize = uint32(fi.Size())
		startPos := e.Header.LogPos - e.Header.EventSize
		if startPos < fSize {
			return -1, fSize, nil
		} else if startPos > fSize {
			return 1, fSize, nil
		}
	}
	return 0, fSize, nil
}

// handleFormatDescriptionEvent tries to create new binlog file and write binlog header
func (r *Relay) handleFormatDescriptionEvent(filename string) (exist bool, err error) {
	if r.fd != nil {
		// close the previous binlog log
		r.fd.Close()
		r.fd = nil
	}

	if len(filename) == 0 {
		binlogReadErrorCounter.Inc()
		return false, errors.NotValidf("write FormatDescriptionEvent with empty binlog filename")
	}

	fullPath := path.Join(r.meta.Dir(), filename)
	fd, err := os.OpenFile(fullPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return false, errors.Annotatef(err, "file full path %s", fullPath)
	}
	r.fd = fd

	// record current active relay log file, and keep it until newer file opened
	// when current file's fd closed, we should not reset this, because it may re-open again
	r.setActiveRelayLog(filename)

	err = r.writeBinlogHeaderIfNotExists()
	if err != nil {
		return false, errors.Annotatef(err, "file full path %s", fullPath)
	}

	exist, err = r.checkFormatDescriptionEventExists(filename)
	if err != nil {
		relayLogDataCorruptionCounter.Inc()
		return false, errors.Annotatef(err, "file full path %s", fullPath)
	}

	ret, err := r.fd.Seek(0, io.SeekEnd)
	if err != nil {
		return false, errors.Annotatef(err, "file full path %s", fullPath)
	}
	logger.Info("seek to end", zap.String("file", filename), zap.Int64("end", ret))

	return exist, nil
}

// isNewServer checks whether switched to new server
func (r *Relay) isNewServer() (bool, error) {
	if len(r.meta.UUID()) == 0 {
		// no sub dir exists before
		return true, nil
	}
	uuid, err := utils.GetServerUUID(r.db, r.cfg.Flavor)
	if err != nil {
		return false, errors.Trace(err)
	}
	if strings.HasPrefix(r.meta.UUID(), uuid) {
		// same server as before
		return false, nil
	}
	return true, nil
}

func (r *Relay) reSetupMeta() error {
	uuid, err := utils.GetServerUUID(r.db, r.cfg.Flavor)
	if err != nil {
		return errors.Trace(err)
	}
	err = r.meta.AddDir(uuid, nil, nil)
	if err != nil {
		return errors.Trace(err)
	}
	err = r.meta.Load()
	if err != nil {
		return errors.Trace(err)
	}

	// try adjust meta with start pos from config
	if (r.cfg.EnableGTID && len(r.cfg.BinlogGTID) > 0) || len(r.cfg.BinLogName) > 0 {
		adjusted, err := r.meta.AdjustWithStartPos(r.cfg.BinLogName, r.cfg.BinlogGTID, r.cfg.EnableGTID)
		if err != nil {
			return errors.Trace(err)
		} else if adjusted {
			logger.Info("adjusted meta to start pos", zap.String("start pos's binlog name", r.cfg.BinLogName), zap.String("start pos's binlog gtid", r.cfg.BinlogGTID))
		}
	}

	// record GTID set when switching or the first startup
	_, r.gSetWhenSwitch = r.meta.GTID()
	logger.Info("record previous sub directory end GTID or first startup GTID", zap.Stringer("GTID", r.gSetWhenSwitch))

	r.updateMetricsRelaySubDirIndex()

	return nil
}

func (r *Relay) updateMetricsRelaySubDirIndex() {
	// when switching master server, update sub dir index metrics
	node := r.masterNode()
	uuidWithSuffix := r.meta.UUID() // only change after switch
	_, suffix, err := utils.ParseSuffixForUUID(uuidWithSuffix)
	if err != nil {
		logger.Error("parse suffix for UUID", zap.String("UUID", uuidWithSuffix), zap.Error(err))
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
					logger.Error("flush meta", zap.Error(err))
				} else {
					logger.Info("flush meta finished", zap.Stringer("meta", r.meta))
				}
			}
		case <-masterStatusTicker.C:
			pos, _, err := utils.GetMasterStatus(r.db, r.cfg.Flavor)
			if err != nil {
				logger.Warn("get master status", zap.Error(err))
				continue
			}
			index, err := binlog.GetFilenameIndex(pos.Name)
			if err != nil {
				logger.Error("parse binlog file name", zap.String("file name", pos.Name), log.ShortError(err))
				continue
			}
			relayLogFileGauge.WithLabelValues("master").Set(float64(index))
			relayLogPosGauge.WithLabelValues("master").Set(float64(pos.Pos))
		case <-trimUUIDsTicker.C:
			trimmed, err := r.meta.TrimUUIDs()
			if err != nil {
				logger.Error("trim UUIDs", zap.Error(err))
			} else if len(trimmed) > 0 {
				logger.Info("trim UUIDs", zap.String("UUIDs", strings.Join(trimmed, ";")))
			}
		case <-ctx.Done():
			return
		}
	}
}

func (r *Relay) writeBinlogHeaderIfNotExists() error {
	b := make([]byte, binlogHeaderSize)
	_, err := r.fd.Read(b)
	logger.Debug("", zap.ByteString("the first 4 bytes", b))
	if err == io.EOF || !bytes.Equal(b, replication.BinLogFileHeader) {
		_, err = r.fd.Seek(0, io.SeekStart)
		if err != nil {
			return errors.Trace(err)
		}
		logger.Info("write binlog header")
		// write binlog header fe'bin'
		if _, err = r.fd.Write(replication.BinLogFileHeader); err != nil {
			return errors.Trace(err)
		}
		// Note: it's trival to monitor the writing duration and size here. so ignore it.
	} else if err != nil {
		relayLogDataCorruptionCounter.Inc()
		return errors.Trace(err)
	}
	return nil
}

func (r *Relay) checkFormatDescriptionEventExists(filename string) (exists bool, err error) {
	eof, err2 := replication.NewBinlogParser().ParseSingleEvent(r.fd, func(e *replication.BinlogEvent) error {
		return nil
	})
	if err2 != nil {
		return false, errors.Trace(err2)
	}
	// FormatDescriptionEvent is the first event and only one FormatDescriptionEvent in a file.
	if !eof {
		logger.Info("binlog file already has Format_desc event, so ignore it", zap.String("file name", filename))
		return true, nil
	}
	return false, nil
}

func (r *Relay) setUpReader() error {
	defer func() {
		status := r.reader.Status()
		logger.Info("set up binlog reader", zap.Reflect("status", status))
	}()

	if r.cfg.EnableGTID {
		return r.setUpReaderByGTID()
	}
	return r.setUpReaderByPos()
}

func (r *Relay) setUpReaderByGTID() error {
	uuid, gs := r.meta.GTID()
	logger.Info("start sync for master", zap.String("master node", r.masterNode()), zap.String("UUID", uuid), zap.Stringer("from GTID set", gs))

	err := r.reader.StartSyncByGTID(gs)
	if err != nil {
		logger.Error("start sync in GTID mode", zap.Stringer("GTID", gs), log.ShortError(err))
		return r.setUpReaderByPos()
	}

	return nil
}

func (r *Relay) setUpReaderByPos() error {
	// if the first binlog not exists in local, we should fetch from the first position, whatever the specific position is.
	uuid, pos := r.meta.Pos()
	logger.Info("start sync for master", zap.String("master node", r.masterNode()), zap.String("UUID", uuid), zap.Stringer("from pos", pos))
	if pos.Name == "" {
		// let mysql decides
		return r.reader.StartSyncByPos(pos)
	}
	if stat, err := os.Stat(filepath.Join(r.meta.Dir(), pos.Name)); os.IsNotExist(err) {
		logger.Info("should sync from pos1 instead of pos2 because the binlog file not exists in local before and should sync from the very beginning", zap.String("pos1 name", pos.Name), zap.Int64("pos1 pos", 4), zap.String("pos2 name", pos.Name), zap.Uint32("pos2 pos", pos.Pos))
		pos.Pos = 4
	} else if err != nil {
		return errors.Trace(err)
	} else {
		if stat.Size() > int64(pos.Pos) {
			// it means binlog file already exists, and the local binlog file already contains the specific position
			//  so we can just fetch from the biggest position, that's the stat.Size()
			//
			// NOTE: is it possible the data from pos.Pos to stat.Size corrupt
			logger.Info("binlog file already contains position, so we should sync from the biggest position", zap.String("binlog file", pos.Name), zap.Uint32("position", pos.Pos), zap.Int64("biggest position", stat.Size()))
			pos.Pos = uint32(stat.Size())
			err := r.meta.Save(pos, nil)
			if err != nil {
				return errors.Trace(err)
			}
		} else if stat.Size() < int64(pos.Pos) {
			// in such case, we should stop immediately and check
			return errors.Annotatef(ErrBinlogPosGreaterThanFileSize, "%s size=%d, specific pos=%d", pos.Name, stat.Size(), pos.Pos)
		}
	}

	return r.reader.StartSyncByPos(pos)
}

// reSyncBinlog re-tries sync binlog when master-slave switched
func (r *Relay) reSyncBinlog(cfg replication.BinlogSyncerConfig) error {
	err := r.retrySyncGTIDs()
	if err != nil {
		return errors.Trace(err)
	}
	return r.reSetUpReader(cfg)
}

// retrySyncGTIDs try to auto fix GTID set
// assume that reset master before switching to new master, and only the new master would write
// it's a weak function to try best to fix GTID set while switching master/slave
func (r *Relay) retrySyncGTIDs() error {
	// TODO: now we don't implement quering GTID from MariaDB, implement it later
	if r.cfg.Flavor != mysql.MySQLFlavor {
		return nil
	}
	_, oldGTIDSet := r.meta.GTID()
	logger.Info("start retry sync with old GTID", zap.Stringer("gtid set", oldGTIDSet))

	_, newGTIDSet, err := utils.GetMasterStatus(r.db, r.cfg.Flavor)
	if err != nil {
		return errors.Annotatef(err, "get master status")
	}
	logger.Info("", zap.Reflect("new master GTID set", newGTIDSet))

	masterUUID, err := utils.GetServerUUID(r.db, r.cfg.Flavor)
	if err != nil {
		return errors.Annotatef(err, "get master UUID")
	}
	logger.Info("", zap.String("master UUID", masterUUID))

	oldGTIDSet.Replace(newGTIDSet, []interface{}{masterUUID})

	// add sub relay dir for new master server
	// save and flush meta for new master server
	err = r.meta.AddDir(masterUUID, nil, oldGTIDSet)
	if err != nil {
		return errors.Annotatef(err, "add sub relay directory for master server %s", masterUUID)
	}

	r.updateMetricsRelaySubDirIndex()

	return nil
}

func (r *Relay) reSetUpReader(cfg replication.BinlogSyncerConfig) error {
	if r.reader != nil {
		err := r.reader.Close()
		r.reader = nil
		if err != nil {
			return errors.Trace(err)
		}
	}
	return r.setUpReader()
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
	if r.reader != nil {
		r.reader.Close()
		r.reader = nil
	}
	if r.fd != nil {
		r.fd.Close()
		r.fd = nil
	}
	if err := r.meta.Flush(); err != nil {
		logger.Error("flush checkpoint", zap.Error(err))
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
	logger.Info("relay unit is closing")

	r.stopSync()

	r.closeDB()

	r.closed.Set(true)
	logger.Info("relay unit closed")
}

// Status implements the dm.Unit interface.
func (r *Relay) Status() interface{} {
	masterPos, masterGTID, err := utils.GetMasterStatus(r.db, r.cfg.Flavor)
	if err != nil {
		logger.Warn("get master status", zap.Error(err))
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
		logger.Warn("try to pause, but already closed")
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
	logger.Info("relay unit is updating")

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
		return errors.Trace(err)
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

	logger.Info("relay unit is updated")

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
		return errors.Trace(err)
	}
	err = r.meta.AddDir(uuid, &mysql.Position{Name: binlogName, Pos: binlogPos}, nil)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

/*********** dummy relay log process unit *************/

// DummyRelay is a dummy relay
type DummyRelay struct {
	initErr error

	processResult pb.ProcessResult
	errorInfo     *pb.RelayError
	reloadErr     error
}

// NewDummyRelay creates an instance of dummy Relay.
func NewDummyRelay(cfg *Config) Process {
	return &DummyRelay{}
}

// Init implements Process interface
func (d *DummyRelay) Init() error {
	return d.initErr
}

// InjectInitError injects init error
func (d *DummyRelay) InjectInitError(err error) {
	d.initErr = err
}

// Process implements Process interface
func (d *DummyRelay) Process(ctx context.Context, pr chan pb.ProcessResult) {
	select {
	case <-ctx.Done():
		pr <- d.processResult
	}
}

// InjectProcessResult injects process result
func (d *DummyRelay) InjectProcessResult(result pb.ProcessResult) {
	d.processResult = result
}

// SwitchMaster implements Process interface
func (d *DummyRelay) SwitchMaster(ctx context.Context, req *pb.SwitchRelayMasterRequest) error {
	return nil
}

// Migrate implements Process interface
func (d *DummyRelay) Migrate(ctx context.Context, binlogName string, binlogPos uint32) error {
	return nil
}

// ActiveRelayLog implements Process interface
func (d *DummyRelay) ActiveRelayLog() *pkgstreamer.RelayLogInfo {
	return nil
}

// Reload implements Process interface
func (d *DummyRelay) Reload(newCfg *Config) error {
	return d.reloadErr
}

// InjectReloadError injects reload error
func (d *DummyRelay) InjectReloadError(err error) {
	d.reloadErr = err
}

// Update implements Process interface
func (d *DummyRelay) Update(cfg *config.SubTaskConfig) error {
	return nil
}

// Resume implements Process interface
func (d *DummyRelay) Resume(ctx context.Context, pr chan pb.ProcessResult) {}

// Pause implements Process interface
func (d *DummyRelay) Pause() {}

// Error implements Process interface
func (d *DummyRelay) Error() interface{} {
	return d.errorInfo
}

// Status implements Process interface
func (d *DummyRelay) Status() interface{} {
	return &pb.RelayStatus{
		Stage: pb.Stage_New,
	}
}

// Close implements Process interface
func (d *DummyRelay) Close() {}

// IsClosed implements Process interface
func (d *DummyRelay) IsClosed() bool { return false }
