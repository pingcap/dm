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

package worker

import (
	"io/ioutil"
	"path"
	"path/filepath"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/syndtr/goleveldb/leveldb"

	"github.com/pingcap/dm/dm/pb"
)

type testUpgrade struct{}

var _ = Suite(&testUpgrade{})

func (t *testUpgrade) TestIntervalVersion(c *C) {
	var (
		ver internalVersion
		no  uint64
	)
	c.Assert(ver.no, Equals, no)
	c.Assert(ver.toUint64(), Equals, no)
	c.Assert(ver.String(), Equals, "0")

	// from, to, string
	no = 12345
	ver.fromUint64(no)
	c.Assert(ver.no, Equals, no)
	c.Assert(ver.toUint64(), Equals, no)
	c.Assert(ver.String(), Equals, "12345")

	// compare
	c.Assert(ver.compare(newInternalVersion(12344)), Equals, 1)
	c.Assert(ver.compare(newInternalVersion(12345)), Equals, 0)
	c.Assert(ver.compare(newInternalVersion(12346)), Equals, -1)

	// marshal
	data12345 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x39}
	dataMar, err := ver.MarshalBinary()
	c.Assert(err, IsNil)
	c.Assert(dataMar, DeepEquals, data12345)

	// unmarshal
	data6789 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1A, 0x85}
	err = ver.UnmarshalBinary(data6789)
	c.Assert(err, IsNil)
	c.Assert(ver.no, Equals, uint64(6789))

	// invalid data length
	dataInvalid := data6789[:len(data6789)-1]
	err = ver.UnmarshalBinary(dataInvalid)
	c.Assert(err, ErrorMatches, ".*binary data.*")
}

func (t *testUpgrade) openTestDB(c *C, dbDir string) *leveldb.DB {
	if dbDir == "" {
		dbDir = path.Join(c.MkDir(), "kv")
	}
	db, err := openDB(dbDir, defaultKVConfig)
	if err != nil {
		c.Fatalf("fail to open leveldb %v", err)
	}
	return db
}

func (t *testUpgrade) TestLoadSaveInternalVersion(c *C) {
	var (
		db      *leveldb.DB
		ver1234 = newInternalVersion(1234)
	)

	// load with nil DB
	_, err := loadInternalVersion(nil)
	c.Assert(errors.Cause(err), Equals, ErrInValidHandler)
	_, err = loadInternalVersion(db)
	c.Assert(errors.Cause(err), Equals, ErrInValidHandler)

	// save with nil DB
	err = saveInternalVersion(nil, ver1234)
	c.Assert(errors.Cause(err), Equals, ErrInValidHandler)
	err = saveInternalVersion(db, ver1234)
	c.Assert(errors.Cause(err), Equals, ErrInValidHandler)

	// open DB
	db = t.openTestDB(c, "")
	defer db.Close()

	// load but no data exist
	verLoad, err := loadInternalVersion(db)
	c.Assert(err, IsNil)
	c.Assert(verLoad.no, DeepEquals, defaultPreviousWorkerVersion)

	// save into DB
	err = saveInternalVersion(db, ver1234)
	c.Assert(err, IsNil)

	// load back
	verLoad, err = loadInternalVersion(db)
	c.Assert(err, IsNil)
	c.Assert(verLoad, DeepEquals, ver1234)
}

func (t *testUpgrade) TestTryUpgrade(c *C) {
	// DB directory not exists, no need to upgrade
	dbDir := "./path-not-exists"
	err := tryUpgrade(dbDir)
	c.Assert(err, IsNil)

	// DB directory is a file path, invalid
	tDir := c.MkDir()
	dbDir = filepath.Join(tDir, "file-not-dir")
	err = ioutil.WriteFile(dbDir, nil, 0600)
	c.Assert(err, IsNil)
	err = tryUpgrade(dbDir)
	c.Assert(err, ErrorMatches, ".*directory.*for DB.*")

	// valid DB directory
	dbDir = tDir

	// previousVer == currentVer, no need to upgrade
	prevVer := newInternalVersion(currentWorkerVersion)
	t.verifyUpgrade(c, dbDir,
		func() {
			t.saveVerToDB(c, dbDir, prevVer)
		}, func() {
			currVer := t.loadVerFromDB(c, dbDir)
			c.Assert(currVer, DeepEquals, prevVer)
		})

	// previousVer > currentVer, no need to upgrade, and can not automatic downgrade now
	prevVer = newInternalVersion(currentWorkerVersion + 1)
	t.verifyUpgrade(c, dbDir,
		func() {
			t.saveVerToDB(c, dbDir, prevVer)
		}, func() {
			currVer := t.loadVerFromDB(c, dbDir)
			c.Assert(currVer, DeepEquals, prevVer)
		})
}

func (t *testUpgrade) TestUpgradeToVer1(c *C) {
	dbDir := c.MkDir()
	t.verifyUpgrade(c, dbDir,
		func() {
			t.prepareBeforeUpgradeVer1(c, dbDir)
		}, func() {
			t.verifyAfterUpgradeVer1(c, dbDir)
		})
}

func (t *testUpgrade) prepareBeforeUpgradeVer1(c *C, dbDir string) {
	db := t.openTestDB(c, dbDir)
	defer db.Close()

	// 1. add some operation log into levelDB and set handled pointer
	logger := new(Logger)
	c.Assert(logger.MarkAndForwardLog(db, &pb.TaskLog{
		Id:   100,
		Task: testTask1Meta,
	}), IsNil)
	c.Assert(logger.MarkAndForwardLog(db, &pb.TaskLog{
		Id:   200,
		Task: testTask2Meta,
	}), IsNil)
	c.Assert(logger.MarkAndForwardLog(db, &pb.TaskLog{
		Id:   300,
		Task: testTask3Meta,
	}), IsNil)
	c.Assert(logger.handledPointer.Location, Equals, int64(300))
	c.Assert(logger.endPointer.Location, Equals, int64(0))

	// 2. add some task meta into levelDB
	c.Assert(SetTaskMeta(db, testTask1Meta), IsNil)
	c.Assert(SetTaskMeta(db, testTask2Meta), IsNil)
	t1, err := GetTaskMeta(db, "task1")
	c.Assert(err, IsNil)
	c.Assert(t1, DeepEquals, testTask1Meta)
	t2, err := GetTaskMeta(db, "task2")
	c.Assert(err, IsNil)
	c.Assert(t2, DeepEquals, testTask2Meta)
}

func (t *testUpgrade) verifyAfterUpgradeVer1(c *C, dbDir string) {
	db := t.openTestDB(c, dbDir)
	defer db.Close()

	// 1. verify operation log and handled pointer
	logger := new(Logger)
	logs, err := logger.Initial(db)
	c.Assert(err, IsNil)
	c.Assert(logs, HasLen, 0)
	c.Assert(logger.handledPointer.Location, Equals, int64(0))
	c.Assert(logger.endPointer.Location, Equals, int64(1))

	// 2. verify task meta
	_, err = GetTaskMeta(db, "task1")
	c.Assert(errors.Cause(err), Equals, leveldb.ErrNotFound)
	_, err = GetTaskMeta(db, "task2")
	c.Assert(errors.Cause(err), Equals, leveldb.ErrNotFound)
}

func (t *testUpgrade) saveVerToDB(c *C, dbDir string, ver internalVersion) {
	db := t.openTestDB(c, dbDir)
	defer db.Close()
	err := saveInternalVersion(db, ver)
	c.Assert(err, IsNil)
}

func (t *testUpgrade) loadVerFromDB(c *C, dbDir string) internalVersion {
	db := t.openTestDB(c, dbDir)
	defer db.Close()
	ver, err := loadInternalVersion(db)
	c.Assert(err, IsNil)
	return ver
}

func (t *testUpgrade) verifyUpgrade(c *C, dir string, before func(), after func()) {
	before()
	err := tryUpgrade(dir)
	c.Assert(err, IsNil)
	after()
}
