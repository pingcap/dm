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

package streamer

import (
	"context"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"

	"github.com/pingcap/dm/pkg/binlog"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/utils"
)

// errors used by reader
var (
	ErrReaderRunning          = errors.New("binlog reader is already running")
	ErrBinlogFileNotSpecified = errors.New("binlog file must be specified")

	// polling interval for watcher
	watcherInterval = 100 * time.Millisecond
)

// BinlogReaderConfig is the configuration for BinlogReader
type BinlogReaderConfig struct {
	RelayDir string
	Timezone *time.Location
}

// BinlogReader is a binlog reader.
type BinlogReader struct {
	cfg    *BinlogReaderConfig
	parser *replication.BinlogParser

	indexPath string   // relay server-uuid index file path
	uuids     []string // master UUIDs (relay sub dir)

	latestServerID uint32 // latest server ID, got from relay log

	running bool
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc

	logger log.Logger
}

// NewBinlogReader creates a new BinlogReader
func NewBinlogReader(logger log.Logger, cfg *BinlogReaderConfig) *BinlogReader {
	ctx, cancel := context.WithCancel(context.Background())
	parser := replication.NewBinlogParser()
	parser.SetVerifyChecksum(true)
	// useDecimal must set true.  ref: https://github.com/pingcap/tidb-enterprise-tools/pull/272
	parser.SetUseDecimal(true)
	if cfg.Timezone != nil {
		parser.SetTimestampStringLocation(cfg.Timezone)
	}
	return &BinlogReader{
		cfg:       cfg,
		parser:    parser,
		indexPath: path.Join(cfg.RelayDir, utils.UUIDIndexFilename),
		ctx:       ctx,
		cancel:    cancel,
		logger:    logger,
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

	// load and update UUID list
	// NOTE: if want to support auto master-slave switching, then needing to re-load UUIDs when parsing.
	err := r.updateUUIDs()
	if err != nil {
		return nil, errors.Trace(err)
	}

	r.latestServerID = 0
	r.running = true
	s := newLocalStreamer()

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.logger.Info("start reading", zap.Stringer("position", pos))
		err = r.parseRelay(r.ctx, s, pos)
		if errors.Cause(err) == r.ctx.Err() {
			r.logger.Warn("parse relay finished", log.ShortError(err))
		} else if err != nil {
			s.closeWithError(err)
			r.logger.Error("parse relay stopped", zap.Error(err))
		}
	}()

	return s, nil
}

// parseRelay parses relay root directory, it support master-slave switch (switching to next sub directory)
func (r *BinlogReader) parseRelay(ctx context.Context, s *LocalStreamer, pos mysql.Position) error {
	var (
		needSwitch     bool
		nextUUID       string
		nextBinlogName string
		err            error
	)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		needSwitch, nextUUID, nextBinlogName, err = r.parseDirAsPossible(ctx, s, pos)
		if err != nil {
			return errors.Trace(err)
		}
		if !needSwitch {
			return errors.NotSupportedf("parse for previous sub relay directory finished, but no next sub directory need to switch")
		}

		_, suffixInt, err2 := utils.ParseSuffixForUUID(nextUUID)
		if err2 != nil {
			return errors.Annotatef(err2, "parse suffix for UUID %s", nextUUID)
		}
		uuidSuffix := utils.SuffixIntToStr(suffixInt)

		parsed, err2 := binlog.ParseFilename(nextBinlogName)
		if err2 != nil {
			return errors.Annotatef(err2, "parse binlog file name %s", nextBinlogName)
		}

		// update pos, so can switch to next sub directory
		pos.Name = binlog.ConstructFilenameWithUUIDSuffix(parsed, uuidSuffix)
		pos.Pos = 4 // start from pos 4 for next sub directory / file
		r.logger.Info("switching to next realy sub directory", zap.String("next uuid", nextUUID), zap.Stringer("position", pos))
	}
}

// parseDirAsPossible parses relay sub directory as far as possible
func (r *BinlogReader) parseDirAsPossible(ctx context.Context, s *LocalStreamer, pos mysql.Position) (needSwitch bool, nextUUID string, nextBinlogName string, err error) {
	currentUUID, _, realPos, err := binlog.ExtractPos(pos, r.uuids)
	if err != nil {
		return false, "", "", errors.Annotatef(err, "parse relay dir with pos %v", pos)
	}
	pos = realPos         // use realPos to do syncing
	var firstParse = true // the first parse time for the relay log file
	var dir = path.Join(r.cfg.RelayDir, currentUUID)
	r.logger.Info("start to parse relay log files in sub directory", zap.String("directory", dir), zap.Stringer("position", pos))

	for {
		select {
		case <-ctx.Done():
			return false, "", "", ctx.Err()
		default:
		}
		files, err := CollectBinlogFilesCmp(dir, pos.Name, FileCmpBiggerEqual)
		if err != nil {
			return false, "", "", errors.Annotatef(err, "parse relay dir %s with pos %s", dir, pos)
		} else if len(files) == 0 {
			return false, "", "", errors.Errorf("no relay log files in dir %s match pos %s", dir, pos)
		}

		r.logger.Debug("start read relay log files", zap.Strings("files", files), zap.String("directory", dir), zap.Stringer("position", pos))

		var (
			latestPos  int64
			latestName string
			offset     = int64(pos.Pos)
		)
		for i, relayLogFile := range files {
			select {
			case <-ctx.Done():
				return false, "", "", ctx.Err()
			default:
			}
			if i == 0 {
				if !strings.HasSuffix(relayLogFile, pos.Name) {
					return false, "", "", errors.Errorf("the first relay log %s not match the start pos %v", relayLogFile, pos)
				}
			} else {
				offset = 4        // for other relay log file, start parse from 4
				firstParse = true // new relay log file need to parse
			}
			needSwitch, latestPos, nextUUID, nextBinlogName, err = r.parseFileAsPossible(ctx, s, relayLogFile, offset, dir, firstParse, currentUUID, i == len(files)-1)
			firstParse = false // already parsed
			if err != nil {
				return false, "", "", errors.Annotatef(err, "parse relay log file %s from offset %d in dir %s", relayLogFile, offset, dir)
			}
			if needSwitch {
				// need switch to next relay sub directory
				return true, nextUUID, nextBinlogName, nil
			}
			latestName = relayLogFile // record the latest file name
		}

		// update pos, so can re-collect files from the latest file and re start parse from latest pos
		pos.Pos = uint32(latestPos)
		pos.Name = latestName
	}
}

// parseFileAsPossible parses single relay log file as far as possible
func (r *BinlogReader) parseFileAsPossible(ctx context.Context, s *LocalStreamer, relayLogFile string, offset int64, relayLogDir string, firstParse bool, currentUUID string, possibleLast bool) (needSwitch bool, latestPos int64, nextUUID string, nextBinlogName string, err error) {
	var (
		needReParse bool
	)
	latestPos = offset
	r.logger.Debug("start to parse relay log file", zap.String("file", relayLogFile), zap.Int64("position", latestPos), zap.String("directory", relayLogDir))

	for {
		select {
		case <-ctx.Done():
			return false, 0, "", "", ctx.Err()
		default:
		}
		needSwitch, needReParse, latestPos, nextUUID, nextBinlogName, err = r.parseFile(ctx, s, relayLogFile, latestPos, relayLogDir, firstParse, currentUUID, possibleLast)
		firstParse = false // set to false to handle the `continue` below
		if err != nil {
			return false, 0, "", "", errors.Annotatef(err, "parse relay log file %s from offset %d in dir %s", relayLogFile, latestPos, relayLogDir)
		}
		if needReParse {
			r.logger.Debug("continue to re-parse relay log file", zap.String("file", relayLogFile), zap.String("directory", relayLogDir))
			continue // should continue to parse this file
		}
		return needSwitch, latestPos, nextUUID, nextBinlogName, nil
	}
}

// parseFile parses single relay log file from specified offset
func (r *BinlogReader) parseFile(
	ctx context.Context, s *LocalStreamer, relayLogFile string, offset int64,
	relayLogDir string, firstParse bool, currentUUID string, possibleLast bool) (
	needSwitch, needReParse bool, latestPos int64, nextUUID string, nextBinlogName string, err error) {
	_, suffixInt, err := utils.ParseSuffixForUUID(currentUUID)
	if err != nil {
		return false, false, 0, "", "", errors.Trace(err)
	}

	uuidSuffix := utils.SuffixIntToStr(suffixInt) // current UUID's suffix, which will be added to binlog name
	latestPos = offset                            // set to argument passed in

	onEventFunc := func(e *replication.BinlogEvent) error {
		r.logger.Debug("read event", zap.Reflect("header", e.Header))
		if e.Header.Flags&0x0040 != 0 {
			// now LOG_EVENT_RELAY_LOG_F is only used for events which used to fill the gap in relay log file when switching the master server
			r.logger.Debug("skip event created by relay writer", zap.Reflect("header", e.Header))
			return nil
		}

		r.latestServerID = e.Header.ServerID // record server_id

		switch e.Header.EventType {
		case replication.FORMAT_DESCRIPTION_EVENT:
			// ignore FORMAT_DESCRIPTION event, because go-mysql will send this fake event
		case replication.ROTATE_EVENT:
			// add master UUID suffix to pos.Name
			env := e.Event.(*replication.RotateEvent)
			parsed, _ := binlog.ParseFilename(string(env.NextLogName))
			nameWithSuffix := binlog.ConstructFilenameWithUUIDSuffix(parsed, uuidSuffix)
			env.NextLogName = []byte(nameWithSuffix)

			if e.Header.Timestamp != 0 && e.Header.LogPos != 0 {
				// not fake rotate event, update file pos
				latestPos = int64(e.Header.LogPos)
			} else {
				r.logger.Debug("skip fake rotate event %+v", zap.Reflect("header", e.Header))
			}

			// currently, we do not switch to the next relay log file when we receive the RotateEvent,
			// because that next relay log file may not exists at this time,
			// and we *try* to switch to the next when `needReParse` is false.
			// so this `currentPos` only used for log now.
			currentPos := mysql.Position{
				Name: string(env.NextLogName),
				Pos:  uint32(env.Position),
			}
			r.logger.Info("rotate binlog", zap.Stringer("position", currentPos))
		default:
			// update file pos
			latestPos = int64(e.Header.LogPos)
		}

		select {
		case s.ch <- e:
		case <-ctx.Done():
		}
		return nil
	}

	fullPath := filepath.Join(relayLogDir, relayLogFile)

	if firstParse {
		// if the file is the first time to parse, send a fake ROTATE_EVENT before parse binlog file
		// ref: https://github.com/mysql/mysql-server/blob/4f1d7cf5fcb11a3f84cff27e37100d7295e7d5ca/sql/rpl_binlog_sender.cc#L248
		e, err2 := utils.GenFakeRotateEvent(relayLogFile, uint64(offset), r.latestServerID)
		if err2 != nil {
			return false, false, 0, "", "", errors.Annotatef(err2, "generate fake RotateEvent for (%s: %d)", relayLogFile, offset)
		}
		err2 = onEventFunc(e)
		if err2 != nil {
			return false, false, 0, "", "", errors.Annotatef(err2, "send event %+v", e.Header)
		}
		r.logger.Info("start parse relay log file", zap.String("file", fullPath), zap.Int64("offset", offset))
	} else {
		r.logger.Debug("start parse relay log file", zap.String("file", fullPath), zap.Int64("offset", offset))
	}

	// use parser.ParseFile directly now, if needed we can change to use FileReader.
	err = r.parser.ParseFile(fullPath, offset, onEventFunc)
	if err != nil {
		if possibleLast && isIgnorableParseError(err) {
			r.logger.Warn("fail to parse relay log file, meet some ignorable error", zap.String("file", fullPath), zap.Int64("offset", offset), zap.Error(err))
		} else {
			r.logger.Error("parse relay log file", zap.String("file", fullPath), zap.Int64("offset", offset), zap.Error(err))
			return false, false, 0, "", "", errors.Annotatef(err, "relay log file %s", fullPath)
		}
	}
	r.logger.Debug("parse relay log file", zap.String("file", fullPath), zap.Int64("offset", latestPos))

	if !possibleLast {
		// there are more relay log files in current sub directory, continue to re-collect them
		r.logger.Info("more relay log files need to parse", zap.String("directory", relayLogDir))
		return false, false, latestPos, "", "", nil
	}

	needSwitch, needReParse, nextUUID, nextBinlogName, err = needSwitchSubDir(r.cfg.RelayDir, currentUUID, fullPath, latestPos, r.uuids)
	if err != nil {
		return false, false, 0, "", "", errors.Trace(err)
	} else if needReParse {
		// need to re-parse the current relay log file
		return false, true, latestPos, "", "", nil
	} else if needSwitch {
		// need to switch to next relay sub directory
		return true, false, 0, nextUUID, nextBinlogName, nil
	}

	updatedPath, err := relaySubDirUpdated(ctx, watcherInterval, relayLogDir, fullPath, relayLogFile, latestPos)
	if err != nil {
		return false, false, 0, "", "", errors.Trace(err)
	}

	if strings.HasSuffix(updatedPath, relayLogFile) {
		// current relay log file updated, need to re-parse it
		return false, true, latestPos, "", "", nil
	}

	// need parse next relay log file or re-collect files
	return false, false, latestPos, "", "", nil
}

// updateUUIDs re-parses UUID index file and updates UUID list
func (r *BinlogReader) updateUUIDs() error {
	uuids, err := utils.ParseUUIDIndex(r.indexPath)
	if err != nil {
		return errors.Annotatef(err, "index file path %s", r.indexPath)
	}
	oldUUIDs := r.uuids
	r.uuids = uuids
	r.logger.Info("update relay UUIDs", zap.Strings("old uuids", oldUUIDs), zap.Strings("uuids", uuids))
	return nil
}

// Close closes BinlogReader.
func (r *BinlogReader) Close() error {
	r.logger.Info("binlog reader closing")
	r.running = false
	r.cancel()
	r.parser.Stop()
	r.wg.Wait()
	r.logger.Info("binlog reader closed")
	return nil
}
