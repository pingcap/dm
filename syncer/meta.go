// Copyright 2016 PingCAP, Inc.
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
	"bytes"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go/ioutil2"
)

var (
	maxSaveTime = 30 * time.Second
)

// Meta is the binlog meta information from sync source.
// When syncer restarts, we should reload meta info to guarantee continuous transmission.
type Meta interface {
	// Load loads meta information.
	Load() error

	// Save saves meta information.
	Save(pos mysql.Position, gtid GTIDSet, force bool) error

	// Flush write meta information
	Flush() error

	// Check checks whether we should save meta.
	Check() bool

	// Pos gets position information.
	Pos() mysql.Position

	// GTID() returns gtid information.
	GTID() (GTIDSet, error)
}

// LocalMeta is local meta struct.
type LocalMeta struct {
	sync.RWMutex

	flavor string
	gset   GTIDSet

	filename string
	saveTime time.Time

	BinLogName string `toml:"binlog-name" json:"binlog-name"`
	BinLogPos  uint32 `toml:"binlog-pos" json:"binlog-pos"`
	BinlogGTID string `toml:"binlog-gtid" json:"binlog-gtid"`
}

// NewLocalMeta creates a new LocalMeta.
func NewLocalMeta(filename, flavor string) *LocalMeta {
	return &LocalMeta{filename: filename, BinLogPos: 4, flavor: flavor}
}

// Load implements Meta.Load interface.
func (lm *LocalMeta) Load() error {
	// initialize gset
	var err error
	lm.gset, err = parserGTID(lm.flavor, "")
	if err != nil {
		return errors.Trace(err)
	}

	file, err := os.Open(lm.filename)
	if err != nil && !os.IsNotExist(errors.Cause(err)) {
		return errors.Trace(err)
	}

	if os.IsNotExist(errors.Cause(err)) {
		return nil
	}
	defer file.Close()

	_, err = toml.DecodeReader(file, lm)
	if err != nil {
		return errors.Trace(err)
	}

	lm.gset, err = parserGTID(lm.flavor, lm.BinlogGTID)
	return errors.Trace(err)
}

// Save implements Meta.Save interface.
func (lm *LocalMeta) Save(pos mysql.Position, gs GTIDSet, force bool) error {
	lm.Lock()
	defer lm.Unlock()

	lm.BinLogName = pos.Name
	lm.BinLogPos = pos.Pos
	lm.BinlogGTID = gs.String()
	lm.gset = gs

	if force {
		return lm.Flush()
	}
	return nil
}

// Flush implements Meta.Flush interface.
func (lm *LocalMeta) Flush() error {
	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)
	err := e.Encode(lm)
	if err != nil {
		log.Errorf("save meta info to file %s failed: %v", lm.filename, errors.ErrorStack(err))
		return errors.Trace(err)
	}

	err = ioutil2.WriteFileAtomic(lm.filename, buf.Bytes(), 0644)
	if err != nil {
		log.Errorf("save meta info to file %s failed: %v", lm.filename, errors.ErrorStack(err))
		return errors.Trace(err)
	}

	lm.saveTime = time.Now()
	log.Infof("save position to file, binlog-name:%s binlog-pos:%d binlog-gtid:%v", lm.BinLogName, lm.BinLogPos, lm.BinlogGTID)

	return nil
}

// Pos implements Meta.Pos interface.
func (lm *LocalMeta) Pos() mysql.Position {
	lm.RLock()
	defer lm.RUnlock()

	return mysql.Position{Name: lm.BinLogName, Pos: lm.BinLogPos}
}

// GTID implements Meta.GTID interface
func (lm *LocalMeta) GTID() (GTIDSet, error) {
	lm.RLock()
	defer lm.RUnlock()

	return lm.gset.clone(), nil
}

// Check implements Meta.Check interface.
func (lm *LocalMeta) Check() bool {
	lm.RLock()
	defer lm.RUnlock()

	if time.Since(lm.saveTime) >= maxSaveTime {
		return true
	}

	return false
}

func (lm *LocalMeta) String() string {
	pos := lm.Pos()
	gs, _ := lm.GTID()
	return fmt.Sprintf("syncer-binlog = %v, syncer-binlog-gtid = %v", pos, gs)
}
