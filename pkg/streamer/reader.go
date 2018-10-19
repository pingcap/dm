package streamer

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime/debug"
	"strings"
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

	// in order to differ binlog pos from multi (switched) masters, we added a UUID-suffix field into binlogPos.Name
	// and we also need support: with UUIDSuffix's pos should always > without UUIDSuffix's pos, so we can update from @without to @with automatically
	// conversion: originalPos.NamePrefix + posUUIDSuffixSeparator + UUIDSuffix + baseSeqSeparator + originalPos.NameSuffix => convertedPos.Name
	// UUIDSuffix is the suffix of sub relay directory name, and when new sub directory created, UUIDSuffix is incremented
	// eg. mysql-bin.000003 in c6ae5afe-c7a3-11e8-a19d-0242ac130006.000002 => mysql-bin|000002.000003
	// where `000002` in `c6ae5afe-c7a3-11e8-a19d-0242ac130006.000002` is the UUIDSuffix
	posUUIDSuffixSeparator = "|"
)

// BinlogReaderConfig is the configuration for BinlogReader
type BinlogReaderConfig struct {
	RelayDir string
}

// BinlogReader is a binlog reader.
type BinlogReader struct {
	cfg     *BinlogReaderConfig
	parser  *replication.BinlogParser
	watcher *fsnotify.Watcher

	indexPath string   // relay server-uuid index file path
	uuids     []string // master UUIDs (relay sub dir)

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
		cfg:       cfg,
		parser:    parser,
		indexPath: path.Join(cfg.RelayDir, utils.UUIDIndexFilename),
		ctx:       ctx,
		cancel:    cancel,
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

	// create fs notify watcher
	r.closeWatcher()
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, errors.Trace(err)
	}
	r.watcher = watcher

	// watch meta index file
	err = watcher.Add(r.indexPath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer watcher.Remove(r.indexPath)

	// load and update UUID list
	err = r.updateUUIDs()
	if err != nil {
		return nil, errors.Trace(err)
	}

	r.running = true

	s := newLocalStreamer()

	updatePosition := func(event *replication.BinlogEvent) {
		log.Debugf("[streamer] read event %v", event.Header)
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
			log.Infof("[streamer] rotate binlog to %v", pos)
		default:
			log.Debugf("[streamer] original pos %v, current pos %v", pos.Pos, event.Header.LogPos)
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
				log.Debugf("[streamer] start read from pos %v", pos)
				if err := r.onStream(s, pos, updatePosition); err != nil {
					s.closeWithError(err)
					log.Errorf("[streamer] streaming error %v", errors.ErrorStack(err))
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

	uuidWithSuffix, uuidSuffix, realPos, err := r.extractPos(pos)
	if err != nil {
		return errors.Trace(err)
	}

	pos = realPos // use realPos to do syncing
	binlogDir := path.Join(r.cfg.RelayDir, uuidWithSuffix)

	files, err := collectBinlogFiles(binlogDir, pos.Name)
	if err != nil {
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

		serverID = e.Header.ServerID // record server_id

		switch e.Header.EventType {
		case replication.FORMAT_DESCRIPTION_EVENT:
			// ignore FORMAT_DESCRIPTION event, because go-mysql will send this fake event
		case replication.ROTATE_EVENT:
			// add master UUID suffix to pos.Name
			env := e.Event.(*replication.RotateEvent)
			parsed, _ := parseBinlogFile(string(env.NextLogName))
			nameWithSuffix := fmt.Sprintf("%s%s%s%s%s", parsed.baseName, posUUIDSuffixSeparator, uuidSuffix, baseSeqSeparator, parsed.seq)
			env.NextLogName = []byte(nameWithSuffix)

			if e.Header.Timestamp != 0 && offset != 0 {
				// not fake rotate event, update file pos
				lastFilePos = e.Header.LogPos
			}
		default:
			// update file pos
			lastFilePos = e.Header.LogPos
		}

		updatePos(e)

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
		}

		// always send a fake ROTATE_EVENT before parse binlog file
		// ref: https://github.com/mysql/mysql-server/blob/4f1d7cf5fcb11a3f84cff27e37100d7295e7d5ca/sql/rpl_binlog_sender.cc#L248
		e, err2 := utils.GenFakeRotateEvent(file, uint64(offset), serverID)
		if err2 != nil {
			return errors.Trace(err2)
		}
		err2 = onEventFunc(e)
		if err2 != nil {
			return errors.Trace(err2)
		}
		fullpath := filepath.Join(binlogDir, file)
		log.Debugf("[streamer] parse file %s from offset %d", fullpath, offset)
		if i == len(files)-1 {
			lastFilePath = fullpath
		}

		err = r.parser.ParseFile(fullpath, offset, onEventFunc)
		if i == len(files)-1 && err != nil && strings.Contains(err.Error(), "err EOF") {
			// NOTE: go-mysql returned err not includes caused err, but as message, ref: parser.go `parseSingleEvent`
			log.Warnf("[streamer] parse binlog file %s from offset %d got EOF %s", fullpath, offset, errors.ErrorStack(err))
			break // wait for re-parse
		} else if err != nil {
			log.Errorf("[streamer] parse binlog file %s from offset %d error %s", fullpath, offset, errors.ErrorStack(err))
			return errors.Trace(err)
		}
	}

	// parse current sub dir finished, try switch to next
	nextUUID, nextUUIDSuffix := r.getNextUUID(uuidWithSuffix)
	if nextUUID != "" {
		// next UUID (master relay sub dir) exist
		nextBinlogName, err2 := r.getFirstBinlogName(nextUUID)
		if err2 != nil {
			return errors.Trace(err2)
		}

		// check relay log whether updated since the last ParseFile returned
		fi, err := os.Stat(lastFilePath)
		if err != nil {
			return errors.Annotatef(err, "get stat for relay log %s", lastFilePath)
		}
		if uint32(fi.Size()) > lastFilePos {
			log.Infof("[streamer] relay log file size has changed from %d to %d", lastFilePos, fi.Size())
			return nil // already updated, we need to parse it again
		}

		// switch to next sub dir by sending fake ROTATE event
		log.Infof("[streamer] try switch relay log to %s/%s", nextUUID, nextBinlogName)
		uuidSuffix = nextUUIDSuffix // update uuidSuffix, which will be added to binlog name
		e, err2 := utils.GenFakeRotateEvent(nextBinlogName, 4, 0)
		if err2 != nil {
			return errors.Trace(err2)
		}
		err2 = onEventFunc(e)
		if err2 != nil {
			return errors.Trace(err2)
		}
		return nil // no need to watch anymore
	}

	// watch dir for whether file count changed (new file generated)
	err = r.watcher.Add(binlogDir)
	if err != nil {
		return errors.Annotatef(err, "add watch for relay log dir %s", binlogDir)
	}
	defer r.watcher.Remove(binlogDir)

	if len(lastFilePath) > 0 {
		// check relay log whether updated since the last ParseFile returned
		fi, err := os.Stat(lastFilePath)
		if err != nil {
			return errors.Annotatef(err, "get stat for relay log %s", lastFilePath)
		}
		if uint32(fi.Size()) > lastFilePos {
			log.Infof("[streamer] relay log file size has changed from %d to %d", lastFilePos, fi.Size())
			return nil // already updated, we need to parse it again
		}
	}

	select {
	case <-r.ctx.Done():
		return nil
	case err, ok := <-r.watcher.Errors:
		if !ok {
			return errors.Errorf("watcher's errors chan for relay log dir %s closed", binlogDir)
		}
		return errors.Annotatef(err, "relay log dir %s", binlogDir)
	case event, ok := <-r.watcher.Events:
		if !ok {
			return errors.Errorf("watcher's events chan for relay log dir %s closed", binlogDir)
		}
		log.Debugf("[streamer] watcher receive event %+v", event)

		// TODO zxc: refine to choose different re-parse strategy according to event.Name
		if event.Name == r.indexPath {
			// index file changed, update again
			r.updateUUIDs()
		}
		log.Debugf("[streamer] watcher receive event %+v", event)
	}

	log.Debugf("[streamer] onStream exits")
	return nil
}

// updateUUIDs re-parses UUID index file and updates UUID list
func (r *BinlogReader) updateUUIDs() error {
	uuids, err := utils.ParseUUIDIndex(r.indexPath)
	if err != nil {
		return errors.Trace(err)
	}
	r.uuids = uuids
	log.Infof("[streamer] update relay UUIDs to %v", uuids)
	return nil
}

// extractPos extracts (uuidWithSuffix, uuidSuffix, originalPos) from input pos (originalPos or convertedPos)
func (r *BinlogReader) extractPos(pos mysql.Position) (uuidWithSuffix string, uuidSuffix string, realPos mysql.Position, err error) {
	if len(r.uuids) == 0 {
		return "", "", pos, errors.NotFoundf("relay sub dir with index file %s", r.indexPath)
	}

	parsed, _ := parseBinlogFile(pos.Name)
	sepIdx := strings.Index(parsed.baseName, posUUIDSuffixSeparator)
	if sepIdx > 0 && sepIdx+len(posUUIDSuffixSeparator) < len(parsed.baseName) {
		realBaseName, masterUUIDSuffix := parsed.baseName[:sepIdx], parsed.baseName[sepIdx+len(posUUIDSuffixSeparator):]
		uuid := utils.GetUUIDBySuffix(r.uuids, masterUUIDSuffix)

		if len(uuid) > 0 {
			// valid UUID found
			uuidWithSuffix = uuid
			uuidSuffix = masterUUIDSuffix
			realPos = mysql.Position{
				Name: constructBinlogFilename(realBaseName, parsed.seq),
				Pos:  pos.Pos,
			}
		} else {
			err = errors.NotFoundf("UUID suffix %s with UUIDs %v", masterUUIDSuffix, r.uuids)
		}
		return
	}

	// use the latest
	var suffixInt = 0
	uuid := r.uuids[len(r.uuids)-1]
	_, suffixInt, err = utils.ParseSuffixForUUID(uuid)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	uuidWithSuffix = uuid
	uuidSuffix = utils.SuffixIntToStr(suffixInt)
	realPos = pos // pos is realPos
	return
}

func (r *BinlogReader) getNextUUID(uuid string) (string, string) {
	for i := len(r.uuids) - 2; i >= 0; i-- {
		if r.uuids[i] == uuid {
			nextUUID := r.uuids[i+1]
			_, suffixInt, _ := utils.ParseSuffixForUUID(nextUUID)
			return nextUUID, utils.SuffixIntToStr(suffixInt)
		}
	}
	return "", ""
}

func (r *BinlogReader) getFirstBinlogName(uuid string) (string, error) {
	fpath := path.Join(r.cfg.RelayDir, uuid)
	files, err := readDir(fpath)
	if err != nil {
		return "", errors.Annotatef(err, "get binlog file for dir %s", fpath)
	}

	for _, f := range files {
		if f == utils.MetaFilename {
			log.Debugf("[streamer] skip meta file %s", f)
			continue
		}

		_, err := parseBinlogFile(f)
		if err != nil {
			return "", errors.NotValidf("binlog file %s", f)
		}
		return f, nil
	}

	return "", errors.NotFoundf("binlog files in dir %s", fpath)
}

// Close closes BinlogReader.
func (r *BinlogReader) Close() error {
	log.Info("[streamer] binlog reader closing")
	r.running = false
	r.cancel()
	r.parser.Stop()
	r.wg.Wait()
	r.closeWatcher()
	log.Info("[streamer] binlog reader closed")
	return nil
}

func (r *BinlogReader) closeWatcher() {
	if r.watcher != nil {
		r.watcher.Close()
		r.watcher = nil
	}
}
