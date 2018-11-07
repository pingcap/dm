package relay

import (
	"bytes"
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

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-enterprise-tools/dm/config"
	"github.com/pingcap/tidb-enterprise-tools/dm/pb"
	"github.com/pingcap/tidb-enterprise-tools/dm/unit"
	pkgstreamer "github.com/pingcap/tidb-enterprise-tools/pkg/streamer"
	"github.com/pingcap/tidb-enterprise-tools/pkg/utils"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go/sync2"
	"golang.org/x/net/context"
)

// errors used by relay
var (
	ErrBinlogPosGreaterThanFileSize = errors.New("the specific position is greater than the local binlog file size")
	// for MariaDB, UUID set as `gtid_domain_id` + domainServerIDSeparator + `server_id`
	domainServerIDSeparator = "-"
)

const (
	eventTimeout                = 1 * time.Hour
	slaveReadTimeout            = 1 * time.Minute  // slave read binlog data timeout, ref: https://dev.mysql.com/doc/refman/8.0/en/replication-options-slave.html#sysvar_slave_net_timeout
	masterHeartbeatPeriod       = 30 * time.Second // master server send heartbeat period: ref: `MASTER_HEARTBEAT_PERIOD` in https://dev.mysql.com/doc/refman/8.0/en/change-master-to.html
	flushMetaInterval           = 30 * time.Second
	getMasterStatusInterval     = 30 * time.Second
	binlogHeaderSize            = 4
	showStatusConnectionTimeout = "1m"
)

// Relay relays mysql binlog to local file.
type Relay struct {
	db                    *sql.DB
	cfg                   *Config
	syncer                *replication.BinlogSyncer
	syncerCfg             replication.BinlogSyncerConfig
	gapSyncerCfg          replication.BinlogSyncerConfig
	meta                  Meta
	lastSlaveConnectionID uint32
	fd                    *os.File
	closed                sync2.AtomicBool
	sync.RWMutex
}

// NewRelay creates an instance of Relay.
func NewRelay(cfg *Config) *Relay {
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

	if !cfg.EnableGTID {
		// for rawMode(true), we only parse FormatDescriptionEvent and RotateEvent
		// if not need to support GTID mode, we can enable rawMode
		syncerCfg.RawModeEnabled = true
	}
	return &Relay{
		cfg:          cfg,
		syncerCfg:    syncerCfg,
		gapSyncerCfg: gapSyncerCfg,
		meta:         NewLocalMeta(cfg.Flavor, cfg.RelayDir),
	}
}

// Init implements the dm.Unit interface.
func (r *Relay) Init() error {
	cfg := r.cfg.From
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&interpolateParams=true&readTimeout=%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, showStatusConnectionTimeout)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return errors.Trace(err)
	}
	r.db = db

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
	r.syncer = replication.NewBinlogSyncer(r.syncerCfg)

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

	streamer, err := r.getBinlogStreamer()
	if err != nil {
		return errors.Trace(err)
	}

	var (
		_, lastPos  = r.meta.Pos()
		_, lastGTID = r.meta.GTID()
		tryReSync   = true // used to handle master-slave switch

		// fill gap steps:
		// 1. record the pos after the gap
		// 2. create a special streamer to fill the gap
		// 3. catchup pos after the gap
		// 4. close the special streamer
		gapSyncer     *replication.BinlogSyncer           // syncer used to fill the gap in relay log file
		gapStreamer   *replication.BinlogStreamer         // streamer used to fill the gap in relay log file
		gapSyncEndPos *mysql.Position                     // the pos of the event after the gap
		eventFormat   *replication.FormatDescriptionEvent // latest FormatDescriptionEvent, used when re-calculate checksum
	)

	closeGapSyncer := func() {
		if gapSyncer != nil {
			gapSyncer.Close()
			gapSyncer = nil
		}
		gapStreamer = nil
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
		if gapStreamer == nil && gapSyncEndPos != nil {
			gapSyncer = replication.NewBinlogSyncer(r.gapSyncerCfg)
			gapStreamer, err = gapSyncer.StartSync(lastPos)
			if err != nil {
				return errors.Annotatef(err, "start to fill gap in relay log file from %v", lastPos)
			}
			log.Infof("[relay] start to fill gap in relay log file from %v", lastPos)
		}

		ctx, cancel := context.WithTimeout(parentCtx, eventTimeout)
		readTimer := time.Now()
		var e *replication.BinlogEvent
		if gapStreamer != nil {
			e, err = gapStreamer.GetEvent(ctx)
		} else {
			e, err = streamer.GetEvent(ctx)
		}
		cancel()
		binlogReadDurationHistogram.Observe(time.Since(readTimer).Seconds())

		if err != nil {
			switch errors.Cause(err) {
			case context.Canceled:
				return nil
			case context.DeadlineExceeded:
				log.Infof("[relay] deadline %s exceeded, no binlog event received", eventTimeout)
				continue
			case replication.ErrChecksumMismatch:
				relayLogDataCorruptionCounter.Inc()
			case replication.ErrSyncClosed, replication.ErrNeedSyncAgain:
				// do nothing
			default:
				if utils.IsErrBinlogPurged(err) {
					if gapStreamer != nil {
						return errors.Annotatef(err, "gap streamer")
					}
					if tryReSync && r.cfg.EnableGTID && r.cfg.AutoFixGTID {
						streamer, err = r.reSyncBinlog(r.syncerCfg)
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

		log.Debugf("[relay] receive binlog event with header %+v", e.Header)
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
			if currentPos.Compare(lastPos) == 1 {
				lastPos = currentPos
			}
			log.Infof("[relay] rotate to %s", lastPos.String())
			if e.Header.Timestamp == 0 || e.Header.LogPos == 0 {
				// skip fake rotate event
				continue
			}
		case *replication.QueryEvent:
			// when RawModeEnabled not true, QueryEvent will be parsed
			// even for `BEGIN`, we still update pos / GTID
			lastPos.Pos = e.Header.LogPos
			lastGTID.Set(ev.GSet) // in order to call `ev.GSet`, can not combine QueryEvent and XIDEvent
		case *replication.XIDEvent:
			// when RawModeEnabled not true, XIDEvent will be parsed
			lastPos.Pos = e.Header.LogPos
			lastGTID.Set(ev.GSet)
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
				log.Warnf("[relay] skip artificial event %+v", e.Header)
				continue
			}
		}

		if gapStreamer == nil {
			gapDetected, fSize, err := r.detectGap(e)
			if err != nil {
				relayLogWriteErrorCounter.Inc()
				return errors.Annotatef(err, "detect relay log file gap for event %+v", e.Header)
			}
			if gapDetected {
				gapSyncEndPos = &mysql.Position{
					Name: lastPos.Name,
					Pos:  e.Header.LogPos,
				}
				lastPos.Pos = fSize // reset lastPos
				log.Infof("[relay] gap detected from %d to %d in %s", fSize, e.Header.LogPos-e.Header.EventSize, lastPos.Name)
				continue // skip this event after the gap
			}
		} else {
			// why check gapSyncEndPos != nil?
			if gapSyncEndPos != nil && e.Header.LogPos >= gapSyncEndPos.Pos {
				// catch up, after write this event, gap will be filled
				log.Infof("[relay] fill gap reaching the end pos %v", gapSyncEndPos.String())
				closeGapSyncer()
			} else {
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
			log.Warnf("[relay] skip obsolete event %+v (with relay file size %d)", e.Header, fSize)
			continue
		} else if cmp > 0 {
			relayLogWriteErrorCounter.Inc()
			return errors.Errorf("some events missing, current event %+v, lastPos %v, current GTID %v, relay file size %d", e.Header, lastPos, lastGTID, fSize)
		}

		if !r.cfg.EnableGTID {
			// not need support GTID mode (rawMode enabled), update pos for all events
			lastPos.Pos = e.Header.LogPos
		}

		writeTimer := time.Now()
		log.Debugf("[relay] writing binlog event with header %+v", e.Header)
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
		if index, err := pkgstreamer.GetBinlogFileIndex(lastPos.Name); err != nil {
			log.Errorf("[relay] parse binlog file name %s err %v", lastPos.Name, err)
		} else {
			relayLogFileGauge.WithLabelValues("relay").Set(index)
		}

		err = r.meta.Save(lastPos, lastGTID)
		if err != nil {
			return errors.Trace(err)
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
	log.Infof("[relay] %s seek to end (%d)", filename, ret)

	return exist, nil
}

// isNewServer checks whether switched to new server
func (r *Relay) isNewServer() (bool, error) {
	if len(r.meta.UUID()) == 0 {
		// no sub dir exists before
		return true, nil
	}
	uuid, err := r.getServerUUID()
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
	uuid, err := r.getServerUUID()
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

// getServerUUID gets master server's UUID
// for MySQL，UUID is `server_uuid` system variable
// for MariaDB, UUID is `gtid_domain_id` joined `server_id` with domainServerIDSeparator
func (r *Relay) getServerUUID() (string, error) {
	if r.cfg.Flavor == mysql.MariaDBFlavor {
		domainID, err := utils.GetMariaDBGtidDomainID(r.db)
		if err != nil {
			return "", errors.Trace(err)
		}
		serverID, err := utils.GetServerID(r.db)
		if err != nil {
			return "", errors.Trace(err)
		}
		return fmt.Sprintf("%d%s%d", domainID, domainServerIDSeparator, serverID), nil
	}
	return utils.GetServerUUID(r.db)
}

func (r *Relay) doIntervalOps(ctx context.Context) {
	flushTicker := time.NewTicker(flushMetaInterval)
	defer flushTicker.Stop()
	masterStatusTicker := time.NewTicker(getMasterStatusInterval)
	defer masterStatusTicker.Stop()
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
			index, err := pkgstreamer.GetBinlogFileIndex(pos.Name)
			if err != nil {
				log.Errorf("[relay] parse binlog file name %s error %v", pos.Name, err)
				continue
			}
			relayLogFileGauge.WithLabelValues("master").Set(index)
			relayLogPosGauge.WithLabelValues("master").Set(float64(pos.Pos))
		case <-ctx.Done():
			return
		}
	}
}

func (r *Relay) writeBinlogHeaderIfNotExists() error {
	b := make([]byte, binlogHeaderSize)
	_, err := r.fd.Read(b)
	log.Debugf("[relay] the first 4 bytes are %v", b)
	if err == io.EOF || !bytes.Equal(b, replication.BinLogFileHeader) {
		_, err = r.fd.Seek(0, io.SeekStart)
		if err != nil {
			return errors.Trace(err)
		}
		log.Info("[relay] write binlog header")
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
		log.Infof("[relay] binlog file %s already has Format_desc event, so ignore it", filename)
		return true, nil
	}
	return false, nil
}

// NOTE: now, no online master-slave switching supported
// when switching, user must Pause relay, update config, then Resume
// so, will call `getBinlogStreamer` again on new master
func (r *Relay) getBinlogStreamer() (*replication.BinlogStreamer, error) {
	defer func() {
		r.lastSlaveConnectionID = r.syncer.LastConnectionID()
		log.Infof("[relay] last slave connection id %d", r.lastSlaveConnectionID)
	}()
	if r.cfg.EnableGTID {
		return r.startSyncByGTID()
	}
	return r.startSyncByPos()
}

func (r *Relay) startSyncByGTID() (*replication.BinlogStreamer, error) {
	uuid, gs := r.meta.GTID()
	log.Infof("[relay] start sync for master(%s, %s) from GTID set %s", r.masterNode(), uuid, gs)

	streamer, err := r.syncer.StartSyncGTID(gs.Origin())
	if err != nil {
		log.Errorf("[relay] start sync in GTID mode from %s error %v", gs.String(), err)
		return r.startSyncByPos()
	}

	return streamer, errors.Trace(err)
}

// TODO: exception handling.
// e.g.
// 1.relay connects to a difference MySQL
// 2. upstream MySQL does a pure restart (removes all its' data, and then restart)

func (r *Relay) startSyncByPos() (*replication.BinlogStreamer, error) {
	// if the first binlog not exists in local, we should fetch from the first position, whatever the specific position is.
	uuid, pos := r.meta.Pos()
	log.Infof("[relay] start sync for master (%s, %s) from %s", r.masterNode(), uuid, pos.String())
	if pos.Name == "" {
		// let mysql decides
		return r.syncer.StartSync(pos)
	}
	if stat, err := os.Stat(filepath.Join(r.meta.Dir(), pos.Name)); os.IsNotExist(err) {
		log.Infof("[relay] should sync from %s:4 instead of %s:%d because the binlog file not exists in local before and should sync from the very beginning", pos.Name, pos.Name, pos.Pos)
		pos.Pos = 4
	} else if err != nil {
		return nil, errors.Trace(err)
	} else {
		if stat.Size() > int64(pos.Pos) {
			// it means binlog file already exists, and the local binlog file already contains the specific position
			//  so we can just fetch from the biggest position, that's the stat.Size()
			//
			// NOTE: is it possible the data from pos.Pos to stat.Size corrupt
			log.Infof("[relay] the binlog file %s already contains position %d, so we should sync from %d", pos.Name, pos.Pos, stat.Size())
			pos.Pos = uint32(stat.Size())
			err := r.meta.Save(pos, nil)
			if err != nil {
				return nil, errors.Trace(err)
			}
		} else if stat.Size() < int64(pos.Pos) {
			// in such case, we should stop immediately and check
			return nil, errors.Annotatef(ErrBinlogPosGreaterThanFileSize, "%s size=%d, specific pos=%d", pos.Name, stat.Size(), pos.Pos)
		}
	}

	streamer, err := r.syncer.StartSync(pos)
	return streamer, errors.Trace(err)
}

// reSyncBinlog re-tries sync binlog when master-slave switched
func (r *Relay) reSyncBinlog(cfg replication.BinlogSyncerConfig) (*replication.BinlogStreamer, error) {
	err := r.retrySyncGTIDs()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return r.reopenStreamer(cfg)
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
	log.Infof("[relay] start retry sync with old GTID %s", oldGTIDSet.String())

	_, newGTIDSet, err := utils.GetMasterStatus(r.db, r.cfg.Flavor)
	if err != nil {
		return errors.Annotatef(err, "get master status")
	}
	log.Infof("[relay] new master GTID set %v", newGTIDSet)

	masterUUID, err := r.getServerUUID()
	if err != nil {
		return errors.Annotatef(err, "get master UUID")
	}
	log.Infof("master UUID %s", masterUUID)

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

// reopenStreamer reopen a new streamer
func (r *Relay) reopenStreamer(cfg replication.BinlogSyncerConfig) (*replication.BinlogStreamer, error) {
	if r.syncer != nil {
		err := r.closeBinlogSyncer(r.syncer)
		r.syncer = nil
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	r.syncer = replication.NewBinlogSyncer(cfg)
	return r.getBinlogStreamer()
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
	if r.syncer != nil {
		r.closeBinlogSyncer(r.syncer)
		r.syncer = nil
	}
	if r.fd != nil {
		r.fd.Close()
		r.fd = nil
	}
	if err := r.meta.Flush(); err != nil {
		log.Errorf("[relay] flush checkpoint error %v", errors.ErrorStack(err))
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

	if r.db != nil {
		r.db.Close()
		r.db = nil
	}

	r.closed.Set(true)
	log.Info("[relay] relay unit closed")
}

func (r *Relay) closeBinlogSyncer(syncer *replication.BinlogSyncer) error {
	if syncer == nil {
		return nil
	}

	defer syncer.Close()
	lastSlaveConnectionID := syncer.LastConnectionID()
	if lastSlaveConnectionID > 0 {
		err := utils.KillConn(r.db, lastSlaveConnectionID)
		if err != nil {
			if !utils.IsNoSuchThreadError(err) {
				return errors.Annotatef(err, "connection ID %d", lastSlaveConnectionID)
			}
		}
	}
	return nil
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
		if rs.MasterBinlogGtid == rs.RelayBinlogGtid {
			rs.RelayCatchUpMaster = true
		}
	} else {
		rs.RelayCatchUpMaster = masterPos.Compare(relayPos) == 0
	}
	return rs
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

	r.db.Close()
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
		UseDecimal:      true, // must set true. ref: https://github.com/pingcap/tidb-enterprise-tools/pull/272
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
