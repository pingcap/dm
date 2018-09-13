package relay

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-enterprise-tools/dm/config"
	"github.com/pingcap/tidb-enterprise-tools/dm/pb"
	"github.com/pingcap/tidb-enterprise-tools/dm/unit"
	"github.com/pingcap/tidb-enterprise-tools/pkg/utils"

	// TODO: unify syncer/loader/relay checkpoint
	"github.com/pingcap/tidb-enterprise-tools/syncer"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go/sync2"
	"golang.org/x/net/context"
)

// TODO: refine gtid-mode.
// if support gtid mode fully, we should not use raw mode, because we need to parse all binlog event.

// errors used by relay
var (
	ErrBinlogPosGreaterThanFileSize = errors.New("the specific position is greater than the local binlog file size")
)

const (
	eventTimeout                = 1 * time.Hour
	binlogHeaderSize            = 4
	showStatusConnectionTimeout = "1m"
)

// Relay relays mysql binlog to local file.
type Relay struct {
	db                    *sql.DB
	cfg                   *Config
	syncer                *replication.BinlogSyncer
	syncerCfg             replication.BinlogSyncerConfig
	meta                  syncer.Meta
	lastSlaveConnectionID uint32
	fd                    *os.File
	closed                sync2.AtomicBool
	sync.RWMutex
}

// NewRelay creates an instance of Relay.
func NewRelay(cfg *Config) *Relay {
	syncerCfg := replication.BinlogSyncerConfig{
		ServerID:       uint32(cfg.ServerID),
		Flavor:         cfg.Flavor,
		Host:           cfg.From.Host,
		Port:           uint16(cfg.From.Port),
		User:           cfg.From.User,
		Password:       cfg.From.Password,
		Charset:        cfg.Charset,
		UseDecimal:     true, // must set true. ref: https://github.com/pingcap/tidb-enterprise-tools/pull/272
		RawModeEnabled: true, // for rawMode(true), we only parse FormatDescriptionEvent and RotateEvent
		VerifyChecksum: true,
		// TODO: other config ?
	}
	meta := syncer.NewLocalMeta(path.Join(cfg.RelayDir, cfg.MetaFile), cfg.Flavor)
	binlogSyncer := replication.NewBinlogSyncer(syncerCfg)
	return &Relay{
		cfg:       cfg,
		syncer:    binlogSyncer,
		syncerCfg: syncerCfg,
		meta:      meta,
	}
}

// Init implements the dm.Unit interface.
func (r *Relay) Init() error {
	cfg := r.cfg.From
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8&interpolateParams=true&readTimeout=%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, showStatusConnectionTimeout)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return errors.Trace(err)
	}
	r.db = db

	if err := r.meta.Load(); err != nil {
		return errors.Trace(err)
	}
	if err := os.MkdirAll(r.cfg.RelayDir, 0755); err != nil {
		return errors.Trace(err)
	}
	go func() {
		for {
			time.Sleep(time.Second * 30)
			err := r.meta.Flush()
			if err != nil {
				log.Errorf("[relay] flush checkpoint error %v", errors.ErrorStack(err))
			}
		}
	}()
	return nil
}

// Process implements the dm.Unit interface.
func (r *Relay) Process(ctx context.Context, pr chan pb.ProcessResult) {
	errs := make([]*pb.ProcessError, 0, 1)
	err := r.process(ctx)
	if err != nil && errors.Cause(err) != replication.ErrSyncClosed {
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

func (r *Relay) process(parentCtx context.Context) error {
	streamer, err := r.getBinlogStreamer()
	if err != nil {
		return errors.Trace(err)
	}

	var (
		filename string
		offset   uint32
	)
	defer func() {
		if r.fd != nil {
			r.fd.Close()
		}
	}()

	for {
		ctx, cancel := context.WithTimeout(parentCtx, eventTimeout)
		e, err := streamer.GetEvent(ctx)
		cancel()

		if err == context.DeadlineExceeded {
			log.Infof("after %s deadline exceeded", eventTimeout)
			continue
		}
		if err != nil {
			return errors.Trace(err)
		}

		offset = e.Header.LogPos
		log.Debugf("header %v", e.Header)

		if e.Header.EventType == replication.ROTATE_EVENT {
			rotateEvent := e.Event.(*replication.RotateEvent)
			filename = string(rotateEvent.NextLogName)

			if e.Header.Timestamp == 0 || offset == 0 {
				// fake rotate event
				continue
			}
		} else if e.Header.EventType == replication.FORMAT_DESCRIPTION_EVENT {
			// FormateDescriptionEvent is the first event in binlog, we will close old one and create a new

			if r.fd != nil {
				r.fd.Close()
			}

			if len(filename) == 0 {
				return errors.Errorf("empty binlog filename for FormateDescriptionEvent")
			}

			r.fd, err = os.OpenFile(path.Join(r.cfg.RelayDir, filename), os.O_CREATE|os.O_RDWR, 0644)
			if err != nil {
				return errors.Trace(err)
			}

			err = r.writeBinlogHeaderIfNotExists()
			if err != nil {
				return errors.Trace(err)
			}

			exists, err := r.checkFormatDescriptionEventExists(filename)
			if err != nil {
				return errors.Trace(err)
			}

			ret, err := r.fd.Seek(0, io.SeekEnd)
			if err != nil {
				return errors.Trace(err)
			}
			log.Infof("%s seek to end %d", filename, ret)

			if exists {
				continue
			}
		}

		log.Debugf("write %v", e.Header)
		if n, err2 := r.fd.Write(e.RawData); err2 != nil {
			return errors.Trace(err2)
		} else if n != len(e.RawData) {
			// FIXME: should we panic here? it seems unreachable
			return errors.Trace(io.ErrShortWrite)
		}

		// we don't need to save gtid for local checkpoint.
		err = r.meta.Save(mysql.Position{
			Name: filename,
			Pos:  offset,
		}, nil, false)
		if err != nil {
			return errors.Trace(err)
		}
	}
}

func (r *Relay) writeBinlogHeaderIfNotExists() error {
	b := make([]byte, 4)
	_, err := r.fd.Read(b)
	log.Debugf("the first 4 bytes are %v", b)
	if err == io.EOF || !bytes.Equal(b, replication.BinLogFileHeader) {
		_, err = r.fd.Seek(0, io.SeekStart)
		if err != nil {
			return errors.Trace(err)
		}
		log.Info("write binlog header")
		// write binlog header fe'bin'
		if _, err = r.fd.Write(replication.BinLogFileHeader); err != nil {
			return errors.Trace(err)
		}
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
	// FormateDescriptionEvent is the first event and only one FormateDescriptionEvent in a file.
	if !eof {
		log.Infof("binlog file %s already has Format_desc event, so ignore it", filename)
		return true, nil
	}
	return false, nil
}

// TODO: master-slave switch ?
// TODO: reopen?
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
	gs, err := r.meta.GTID()
	if err != nil {
		return nil, errors.Trace(err)
	}

	streamer, err := r.syncer.StartSyncGTID(gs.Origin())
	if err != nil {
		log.Errorf("start sync in gtid mode error %v", err)
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
	pos := r.meta.Pos()
	if pos.Name == "" {
		// let mysql decides
		return r.syncer.StartSync(pos)
	}
	if stat, err := os.Stat(filepath.Join(r.cfg.RelayDir, pos.Name)); os.IsNotExist(err) {
		log.Infof("we should sync from %s:4 instead of %s:%d because the binlog file not exists in local before and should sync from the very beginning", pos.Name, pos.Name, pos.Pos)
		pos.Pos = 4
	} else if err != nil {
		return nil, errors.Trace(err)
	} else {
		if stat.Size() > int64(pos.Pos) {
			// it means binlog file already exists, and the local binlog file already contains the specific position
			//  so we can just fetch from the biggest position, that's the stat.Size()
			log.Infof("the binlog file %s already contains position %d, so we should sync from %d", pos.Name, pos.Pos, stat.Size())
			pos.Pos = uint32(stat.Size())
			err := r.meta.Save(mysql.Position{
				Name: pos.Name,
				Pos:  pos.Pos,
			}, nil, false)
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

// IsClosed tells whether Relay unit is closed or not.
func (r *Relay) IsClosed() bool {
	return r.closed.Get()
}

// Close implements the dm.Unit interface.
func (r *Relay) Close() {
	r.Lock()
	defer r.Unlock()
	if r.closed.Get() {
		return
	}
	log.Info("relay closing")
	if r.syncer != nil {
		r.syncer.Close()
	}
	if r.fd != nil {
		r.fd.Close()
	}
	if r.db != nil {
		r.db.Close()
	}
	if err := r.meta.Flush(); err != nil {
		log.Errorf("[relay] flush checkpoint error %v", errors.ErrorStack(err))
	}
	r.closed.Set(true)
	log.Info("relay closed")
}

// Status implements the dm.Unit interface.
func (r *Relay) Status() interface{} {
	masterPos, masterGTID, err := utils.GetMasterStatus(r.db, r.cfg.Flavor)
	if err != nil {
		log.Warnf("[relay] get master status %v", errors.ErrorStack(err))
	}

	relayPos := r.meta.Pos()
	relayGTIDSet, err := r.meta.GTID()
	if err != nil {
		log.Warnf("[relay] get gtid err %v", errors.ErrorStack(err))
	}
	return &pb.RelayStatus{
		MasterBinlog:     masterPos.String(),
		MasterBinlogGtid: masterGTID.String(),
		RelayBinlog:      relayPos.String(),
		RelayBinlogGtid:  relayGTIDSet.String(),
	}
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
	// Note: will not implemented
}

// Resume resumes the paused process
func (r *Relay) Resume(ctx context.Context, pr chan pb.ProcessResult) {
	// Note: will not implementted
}

// Update implements Unit.Update
func (r *Relay) Update(cfg *config.SubTaskConfig) error {
	// not support update configuration now
	return nil
}
