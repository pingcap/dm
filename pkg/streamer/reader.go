package streamer

import (
	"fmt"
	"path/filepath"
	"runtime/debug"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"golang.org/x/net/context"
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
	r.running = true

	s := newLocalStreamer()

	updatePosition := func(event *replication.BinlogEvent) {
		log.Debugf("event %v", event.Header)
		switch event.Header.EventType {
		case replication.ROTATE_EVENT:
			rotateEvent := event.Event.(*replication.RotateEvent)
			filename := string(rotateEvent.NextLogName)
			pos.Name = filename
			log.Debugf("rotate event to %s", filename)
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
				log.Infof("onstream read from pos %v", pos)
				if err := r.onStream(s, pos, updatePosition); err != nil {
					log.Errorf("streaming error %v", errors.ErrorStack(err))
					return
				}
				time.Sleep(time.Second * 1)
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

	onEventFunc := func(e *replication.BinlogEvent) error {
		//TODO: put the implementaion of updatepos here?
		updatePos(e)
		select {
		case s.ch <- e:
		case <-r.ctx.Done():
			return nil
		}
		return nil
	}

	var offset int64
	firstFile := parseBinlogFile(pos.Name)
	for _, file := range files {
		select {
		case <-r.ctx.Done():
			return nil
		default:
		}
		parsed := parseBinlogFile(file)
		if !parsed.BiggerOrEqualThan(firstFile) {
			log.Debugf("ignore older binlog file %s", file)
			continue
		}

		if parsed.Equal(firstFile) {
			offset = int64(pos.Pos)
		} else {
			offset = 0
		}
		fullpath := filepath.Join(r.cfg.BinlogDir, file)
		log.Infof("parse file %s from offset %d", fullpath, offset)
		if err := r.parser.ParseFile(fullpath, offset, onEventFunc); err != nil {
			log.Errorf("parse binlog file %s from offset %d error %s", fullpath, offset, errors.ErrorStack(err))
			s.closeWithError(err)
			return errors.Trace(err)
		}
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
	log.Info("binlog reader closed")
	return nil

}
