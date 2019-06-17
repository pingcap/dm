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
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go/sync2"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/unit"
	"github.com/pingcap/dm/pkg/binlog"
	fr "github.com/pingcap/dm/pkg/func-rollback"
	"github.com/pingcap/dm/pkg/log"
	pkgstreamer "github.com/pingcap/dm/pkg/streamer"
	"github.com/pingcap/dm/pkg/utils"
	"github.com/pingcap/dm/relay/reader"
	"github.com/pingcap/dm/relay/transformer"
	"github.com/pingcap/dm/relay/writer"
)

var (
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

	meta   Meta
	closed sync2.AtomicBool
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
	errs := make([]*pb.ProcessError, 0, 1)
	err := r.process(ctx)
	if err != nil && errors.Cause(err) != replication.ErrSyncClosed {
		relayExitWithErrorCounter.Inc()
		log.Errorf("[relay] process exit with error %v", errors.ErrorStack(err))
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

	// TODO: do recover before reading from upstream.
	reader2, err := r.setUpReader()
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		err = reader2.Close()
		if err != nil {
			log.Errorf("[relay] close binlog event reader error %v", err)
		}
	}()

	writer2, err := r.setUpWriter(parser2)
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		err = writer2.Close()
		if err != nil {
			log.Errorf("[relay] close binlog event writer error %v", err)
		}
	}()

	transformer2 := transformer.NewTransformer(parser2)

	go r.doIntervalOps(parentCtx)

	return errors.Trace(r.handleEvents(parentCtx, reader2, transformer2, writer2))
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
		// 1. reader events from upstream server
		ctx2, cancel2 := context.WithTimeout(ctx, eventTimeout)
		readTimer := time.Now()
		rResult, err := reader2.GetEvent(ctx2)
		cancel2()
		binlogReadDurationHistogram.Observe(time.Since(readTimer).Seconds())

		if err != nil {
			if rResult.ErrIgnorable {
				log.Infof("[relay] get ignorable error %v when reading binlog event", err)
				return nil
			} else if rResult.ErrRetryable {
				log.Infof("[relay] get retryable error %v when reading binlog event", err)
				continue
			}
			switch err {
			case replication.ErrChecksumMismatch:
				relayLogDataCorruptionCounter.Inc()
			case replication.ErrSyncClosed, replication.ErrNeedSyncAgain:
				// do nothing, but the error will be returned
			default:
				if utils.IsErrBinlogPurged(err) {
					// TODO: try auto fix GTID, and can support auto switching between upstream server later.
				}
				binlogReadErrorCounter.Inc()
			}
			return errors.Trace(err)
		}
		e := rResult.Event
		log.Debugf("[relay] receive binlog event with header %+v", e.Header)

		// 2. transform events
		tResult := transformer2.Transform(e)
		if len(tResult.NextLogName) > 0 && tResult.NextLogName > lastPos.Name {
			lastPos = mysql.Position{
				Name: string(tResult.NextLogName),
				Pos:  uint32(tResult.LogPos),
			}
			log.Infof("[relay] rotate to %s", lastPos.String())
		}
		if tResult.Ignore {
			log.Infof("[relay] ignore event %+v by transformer", e.Header)
			continue
		}

		// 3. save events into file
		writeTimer := time.Now()
		log.Debugf("[relay] writing binlog event with header %+v", e.Header)
		wResult, err := writer2.WriteEvent(e)
		if err != nil {
			relayLogWriteErrorCounter.Inc()
			return errors.Trace(err)
		} else if wResult.Ignore {
			log.Infof("[relay] ignore event %+v by writer", e.Header)
			continue
		}
		relayLogWriteDurationHistogram.Observe(time.Since(writeTimer).Seconds())

		// 4. update meta and metrics
		needSavePos := tResult.CanSaveGTID
		lastPos.Pos = tResult.LogPos
		err = lastGTID.Set(tResult.GTIDSet)
		if err != nil {
			log.Errorf("[relay] update last GTID set to %v error %v", tResult.GTIDSet, err)
		}
		if !r.cfg.EnableGTID {
			// if go-mysql set RawModeEnabled to true
			// then it will only parse FormatDescriptionEvent and RotateEvent
			// then check `e.Event.(type)` for `QueryEvent` and `XIDEvent` will never be true
			// so we need to update pos for all events
			// and also save pos for all events
			if e.Header.EventType != replication.ROTATE_EVENT {
				lastPos.Pos = e.Header.LogPos // makfor RotateEvent, lastPos updated to the next binlog file's position.
			}
			needSavePos = true
		}

		relayLogWriteSizeHistogram.Observe(float64(e.Header.EventSize))
		relayLogPosGauge.WithLabelValues("relay").Set(float64(lastPos.Pos))
		if index, err2 := binlog.GetFilenameIndex(lastPos.Name); err2 != nil {
			log.Errorf("[relay] parse binlog file name %s err %v", lastPos.Name, err2)
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
			log.Infof("[relay] adjusted meta to start pos with binlog-name (%s), binlog-gtid (%s)", r.cfg.BinLogName, r.cfg.BinlogGTID)
		}
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
		log.Errorf("parse suffix for UUID %s error %v", uuidWithSuffix, errors.Trace(err))
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
					log.Errorf("[relay] flush meta error %v", errors.ErrorStack(err))
				} else {
					log.Infof("[relay] flush meta finished, %s", r.meta.String())
				}
			}
		case <-masterStatusTicker.C:
			pos, _, err := utils.GetMasterStatus(r.db, r.cfg.Flavor)
			if err != nil {
				log.Warnf("[relay] get master status error %v", errors.ErrorStack(err))
				continue
			}
			index, err := binlog.GetFilenameIndex(pos.Name)
			if err != nil {
				log.Errorf("[relay] parse binlog file name %s error %v", pos.Name, err)
				continue
			}
			relayLogFileGauge.WithLabelValues("master").Set(float64(index))
			relayLogPosGauge.WithLabelValues("master").Set(float64(pos.Pos))
		case <-trimUUIDsTicker.C:
			trimmed, err := r.meta.TrimUUIDs()
			if err != nil {
				log.Errorf("[relay] trim UUIDs error %s", errors.ErrorStack(err))
			} else if len(trimmed) > 0 {
				log.Infof("[relay] trim UUIDs %s", strings.Join(trimmed, ";"))
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
		return nil, errors.Annotatef(err, "start reader for UUID %s", uuid)
	}

	log.Infof("[relay] started underlying reader for UUID %s ", uuid)
	return reader2, nil
}

// setUpWriter setups the underlying writer used to writer binlog events into file or other places.
func (r *Relay) setUpWriter(parser2 *parser.Parser) (writer.Writer, error) {
	uuid, pos := r.meta.Pos()
	cfg := &writer.FileConfig{
		RelayDir: r.meta.Dir(),
		Filename: pos.Name,
	}
	writer2 := writer.NewFileWriter(cfg, parser2)
	err := writer2.Start()
	if err != nil {
		return nil, errors.Annotatef(err, "start writer for UUID %s with config %+v", uuid, cfg)
	}

	log.Infof("[relay] started underlying writer for UUID %s with config %+v", uuid, cfg)
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
		log.Errorf("[relay] flush checkpoint error %v", errors.ErrorStack(err))
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
	log.Info("[relay] relay unit is closing")

	r.stopSync()

	r.closeDB()

	r.closed.Set(true)
	log.Info("[relay] relay unit closed")
}

// Status implements the dm.Unit interface.
func (r *Relay) Status() interface{} {
	masterPos, masterGTID, err := utils.GetMasterStatus(r.db, r.cfg.Flavor)
	if err != nil {
		log.Warnf("[relay] get master status %v", errors.ErrorStack(err))
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
		log.Warn("[relay] try to pause, but already closed")
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
	log.Info("[relay] relay unit is updating")

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

	log.Info("[relay] relay unit is updated")

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
