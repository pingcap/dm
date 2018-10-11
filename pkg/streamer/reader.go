package streamer

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"golang.org/x/net/context"

	"github.com/pingcap/tidb-enterprise-tools/pkg/utils"
)

// errors used by reader
var (
	ErrReaderRunning          = errors.New("binlog reader is already running")
	ErrBinlogFileNotSpecified = errors.New("binlog file must be specified")
)

// BinlogReaderConfig is the configuration for BinlogReader
type BinlogReaderConfig struct {
	BinlogDir string
}

// BinlogReader is a binlog reader.
type BinlogReader struct {
	cfg     *BinlogReaderConfig
	parser  *replication.BinlogParser
	watcher *fsnotify.Watcher
	running bool
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
}

// NewBinlogReader creates a new BinlogReader
func NewBinlogReader(cfg *BinlogReaderConfig) *BinlogReader {
	ctx, cancel := context.WithCancel(context.Background())
	parser := replication.NewBinlogParser()
	parser.SetVerifyChecksum(true)
	// useDecimal must set true.  ref: https://github.com/pingcap/tidb-enterprise-tools/pull/272
	parser.SetUseDecimal(true)
	return &BinlogReader{
		cfg:    cfg,
		parser: parser,
		ctx:    ctx,
		cancel: cancel,
	}
}

// StartSync start syncon
// TODO:  thread-safe?
func (r *BinlogReader) StartSync(pos mysql.Position) (Streamer, error) {
	if pos.Name == "" {
		return nil, ErrBinlogFileNotSpecified
	}
	if r.running {
		return nil, ErrReaderRunning
	}

	r.closeWatcher()
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, errors.Trace(err)
	}
	r.watcher = watcher

	r.running = true

	s := newLocalStreamer()

	updatePosition := func(event *replication.BinlogEvent) {
		log.Debugf("event %v", event.Header)
		switch event.Header.EventType {
		case replication.ROTATE_EVENT:
			rotateEvent := event.Event.(*replication.RotateEvent)
			currentPos := mysql.Position{
				Name: string(rotateEvent.NextLogName),
				Pos:  uint32(rotateEvent.Position),
			}
			if currentPos.Name > pos.Name {
				pos = currentPos // need update Name and Pos
			}
			log.Infof("rotate event to %v", pos)
		default:
			log.Debugf("original pos %v, current pos %v", pos.Pos, event.Header.LogPos)
			if pos.Pos < event.Header.LogPos {
				pos.Pos = event.Header.LogPos
			}
		}

	}

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		for {
			select {
			case <-r.ctx.Done():
				return
			default:
				log.Debugf("onstream read from pos %v", pos)
				if err := r.onStream(s, pos, updatePosition); err != nil {
					log.Errorf("streaming error %v", errors.ErrorStack(err))
					return
				}
			}
		}
	}()

	return s, nil
}

func (r *BinlogReader) onStream(s *LocalStreamer, pos mysql.Position, updatePos func(event *replication.BinlogEvent)) error {
	defer func() {
		if e := recover(); e != nil {
			s.closeWithError(fmt.Errorf("Err: %v\n Stack: %s", e, string(debug.Stack())))
		}
	}()

	files, err := collectBinlogFiles(r.cfg.BinlogDir, pos.Name)
	if err != nil {
		s.closeWithError(err)
		return errors.Trace(err)
	}

	var (
		offset       int64
		serverID     uint32
		lastFilePath string    // last relay log file path
		lastFilePos  = pos.Pos // last parsed pos for relay log file
	)

	onEventFunc := func(e *replication.BinlogEvent) error {
		//TODO: put the implementaion of updatepos here?
		updatePos(e)

		serverID = e.Header.ServerID // record server_id
		if _, ok := e.Event.(*replication.FormatDescriptionEvent); !ok {
			// ignore FORMAT_DESCRIPTION event, because go-mysql will send this fake event
			lastFilePos = e.Header.LogPos
		}

		select {
		case s.ch <- e:
		case <-r.ctx.Done():
			return nil
		}
		return nil
	}

	for i, file := range files {
		select {
		case <-r.ctx.Done():
			return nil
		default:
		}

		if i == 0 {
			offset = int64(pos.Pos)
		} else {
			offset = 4 // start read from pos 4
			if serverID > 0 {
				// serverID got, send a fake ROTATE_EVENT before parse binlog file
				// ref: https://github.com/mysql/mysql-server/blob/4f1d7cf5fcb11a3f84cff27e37100d7295e7d5ca/sql/rpl_binlog_sender.cc#L248
				e, err2 := utils.GenFakeRotateEvent(file, uint64(offset), serverID)
				if err2 != nil {
					return errors.Trace(err2)
				}
				err2 = onEventFunc(e)
				if err2 != nil {
					return errors.Trace(err2)
				}
			}
		}
		fullpath := filepath.Join(r.cfg.BinlogDir, file)
		log.Debugf("parse file %s from offset %d", fullpath, offset)
		if i == len(files)-1 {
			lastFilePath = fullpath
		}

		err = r.parser.ParseFile(fullpath, offset, onEventFunc)
		if i == len(files)-1 && errors.Cause(err) == io.EOF {
			log.Warnf("parse binlog file %s from offset %d got EOF %s", fullpath, offset, errors.ErrorStack(err))
			break // wait for re-parse
		} else if err != nil {
			log.Errorf("parse binlog file %s from offset %d error %s", fullpath, offset, errors.ErrorStack(err))
			s.closeWithError(err)
			return errors.Trace(err)
		}
	}

	// watch dir for whether file count changed (new file generated)
	err = r.watcher.Add(r.cfg.BinlogDir)
	if err != nil {
		return errors.Annotatef(err, "add watch for relay log dir %s", r.cfg.BinlogDir)
	}
	defer r.watcher.Remove(r.cfg.BinlogDir)

	if len(lastFilePath) > 0 {
		// check relay log whether updated since the last ParseFile returned
		fi, err := os.Stat(lastFilePath)
		if err != nil {
			return errors.Annotatef(err, "get stat for relay log %s", lastFilePath)
		}
		if uint32(fi.Size()) > lastFilePos {
			log.Infof("[relay] relay log file size has changed from %d to %d", lastFilePos, fi.Size())
			return nil // already updated, we need to parse it again
		}
	}

	select {
	case <-r.ctx.Done():
		return nil
	case err, ok := <-r.watcher.Errors:
		if !ok {
			return errors.Errorf("watcher's errors chan for relay log dir %s closed", r.cfg.BinlogDir)
		}
		return errors.Annotatef(err, "relay log dir %s", r.cfg.BinlogDir)
	case event, ok := <-r.watcher.Events:
		if !ok {
			return errors.Errorf("watcher's events chan for relay log dir %s closed", r.cfg.BinlogDir)
		}
		log.Debugf("watcher receive event %+v", event)
	}

	log.Debugf("[stream] onStream exits")
	return nil
}

// Close closes BinlogReader.
func (r *BinlogReader) Close() error {
	log.Info("binlog reader closing")
	r.running = false
	r.cancel()
	r.parser.Stop()
	r.wg.Wait()
	r.closeWatcher()
	log.Info("binlog reader closed")
	return nil
}

func (r *BinlogReader) closeWatcher() {
	if r.watcher != nil {
		r.watcher.Close()
		r.watcher = nil
	}
}
