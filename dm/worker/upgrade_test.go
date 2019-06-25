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
	"path"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/syndtr/goleveldb/leveldb"
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

func (t *testUpgrade) openTestDB(c *C) *leveldb.DB {
	dir := c.MkDir()
	dbDir := path.Join(dir, "kv")
	db, err := openDB(dbDir, defaultKVConfig)
	if err != nil {
		c.Fatalf("fail to open leveldb %v", err)
	}
	return db
}

func (t *testUpgrade) TestLoadSaveInternalVersion(c *C) {
	var (
		db      *leveldb.DB
		ver1234 internalVersion
	)
	ver1234.fromUint64(1234)

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
	db = t.openTestDB(c)
	defer db.Close()

	// load but no data exist
	_, err = loadInternalVersion(db)
	c.Assert(errors.Cause(err), Equals, leveldb.ErrNotFound)

	// save into DB
	err = saveInternalVersion(db, ver1234)
	c.Assert(err, IsNil)

	// load back
	verLoad, err := loadInternalVersion(db)
	c.Assert(err, IsNil)
	c.Assert(verLoad, DeepEquals, ver1234)
}
