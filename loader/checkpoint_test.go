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

package loader

import (
	"os"
	"strconv"

	. "github.com/pingcap/check"

	"github.com/pingcap/dm/dm/config"
	tcontext "github.com/pingcap/dm/pkg/context"
)

var _ = Suite(&testCheckPointSuite{})

type testCheckPointSuite struct {
	cfg *config.SubTaskConfig
}

func (t *testCheckPointSuite) SetUpSuite(c *C) {
	host := os.Getenv("MYSQL_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port, _ := strconv.Atoi(os.Getenv("MYSQL_PORT"))
	if port == 0 {
		port = 3306
	}
	user := os.Getenv("MYSQL_USER")
	if user == "" {
		user = "root"
	}
	pswd := os.Getenv("MYSQL_PSWD")

	t.cfg = &config.SubTaskConfig{
		To: config.DBConfig{
			Host:     host,
			User:     user,
			Password: pswd,
			Port:     port,
		},
		MetaSchema: "test",
	}
	t.cfg.To.Adjust()
}

func (t *testCheckPointSuite) TearDownSuite(c *C) {
}

// test checkpoint's db operation.
func (t *testCheckPointSuite) TestForDB(c *C) {
	cases := []struct {
		filename string
		endPos   int64
	}{
		{"db1.tbl1.sql", 123},
		{"db1.tbl2.sql", 456},
		{"db1.tbl3.sql", 789},
	}

	allFiles := map[string]Tables2DataFiles{
		"db1": {
			"tbl1": {cases[0].filename},
			"tbl2": {cases[1].filename},
			"tbl3": {cases[2].filename},
		},
	}

	id := "test_for_db"
	tctx := tcontext.Background()
	cp, err := newRemoteCheckPoint(tctx, t.cfg, id)
	c.Assert(err, IsNil)
	defer cp.Close()

	c.Assert(cp.Clear(tctx), IsNil)

	// no checkpoint exist
	err = cp.Load(tctx)
	c.Assert(err, IsNil)

	infos := cp.GetAllRestoringFileInfo()
	c.Assert(len(infos), Equals, 0)

	count, err := cp.Count(tctx)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 0)

	c.Assert(cp.IsTableCreated("db1", ""), IsFalse)
	c.Assert(cp.IsTableCreated("db1", "tbl1"), IsFalse)
	c.Assert(cp.CalcProgress(allFiles), IsNil)
	c.Assert(cp.IsTableFinished("db1", "tbl1"), IsFalse)

	// insert default checkpoints
	for _, cs := range cases {
		err = cp.Init(tctx, cs.filename, cs.endPos)
		c.Assert(err, IsNil)
	}

	c.Assert(cp.IsTableCreated("db1", ""), IsTrue)
	c.Assert(cp.IsTableCreated("db1", "tbl1"), IsTrue)
	c.Assert(cp.CalcProgress(allFiles), IsNil)
	c.Assert(cp.IsTableFinished("db1", "tbl1"), IsFalse)

	info := cp.GetRestoringFileInfo("db1", "tbl1")
	c.Assert(info, HasLen, 1)
	c.Assert(info[cases[0].filename], DeepEquals, []int64{0, cases[0].endPos})

	err = cp.Load(tctx)
	c.Assert(err, IsNil)

	infos = cp.GetAllRestoringFileInfo()
	c.Assert(len(infos), Equals, len(cases))
	for _, cs := range cases {
		info, ok := infos[cs.filename]
		c.Assert(ok, IsTrue)
		c.Assert(len(info), Equals, 2)
		c.Assert(info[0], Equals, int64(0))
		c.Assert(info[1], Equals, cs.endPos)
	}

	count, err = cp.Count(tctx)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, len(cases))

	// update checkpoints
	db, conns, err := createConns(tctx, t.cfg, 1)
	c.Assert(err, IsNil)
	conn := conns[0]
	defer func() {
		err = db.Close()
		c.Assert(err, IsNil)
	}()
	for _, cs := range cases {
		sql2 := cp.GenSQL(cs.filename, cs.endPos)
		err = conn.executeSQL(tctx, []string{sql2})
		c.Assert(err, IsNil)
	}

	err = cp.Load(tctx)
	c.Assert(err, IsNil)

	c.Assert(cp.IsTableCreated("db1", ""), IsTrue)
	c.Assert(cp.IsTableCreated("db1", "tbl1"), IsTrue)
	c.Assert(cp.CalcProgress(allFiles), IsNil)
	c.Assert(cp.IsTableFinished("db1", "tbl1"), IsTrue)

	info = cp.GetRestoringFileInfo("db1", "tbl1")
	c.Assert(info, HasLen, 1)
	c.Assert(info[cases[0].filename], DeepEquals, []int64{cases[0].endPos, cases[0].endPos})

	infos = cp.GetAllRestoringFileInfo()
	c.Assert(len(infos), Equals, len(cases))
	for _, cs := range cases {
		info, ok := infos[cs.filename]
		c.Assert(ok, IsTrue)
		c.Assert(len(info), Equals, 2)
		c.Assert(info[0], Equals, cs.endPos)
		c.Assert(info[1], Equals, cs.endPos)
	}

	count, err = cp.Count(tctx)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, len(cases))

	// clear all
	c.Assert(cp.Clear(tctx), IsNil)

	// no checkpoint exist
	err = cp.Load(tctx)
	c.Assert(err, IsNil)

	c.Assert(cp.IsTableCreated("db1", ""), IsFalse)
	c.Assert(cp.IsTableCreated("db1", "tbl1"), IsFalse)
	c.Assert(cp.CalcProgress(allFiles), IsNil)
	c.Assert(cp.IsTableFinished("db1", "tbl1"), IsFalse)

	infos = cp.GetAllRestoringFileInfo()
	c.Assert(len(infos), Equals, 0)

	// obtain count again
	count, err = cp.Count(tctx)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 0)
}

func (t *testCheckPointSuite) TestDeepCopy(c *C) {
	cp := RemoteCheckPoint{}
	cp.restoringFiles.pos = make(map[string]map[string]FilePosSet)
	cp.restoringFiles.pos["db"] = make(map[string]FilePosSet)
	cp.restoringFiles.pos["db"]["table"] = make(map[string][]int64)
	cp.restoringFiles.pos["db"]["table"]["file"] = []int64{0, 100}

	ret := cp.GetRestoringFileInfo("db", "table")
	cp.restoringFiles.pos["db"]["table"]["file"][0] = 10
	cp.restoringFiles.pos["db"]["table"]["file2"] = []int64{0, 100}
	c.Assert(ret, DeepEquals, map[string][]int64{"file": {0, 100}})

	ret = cp.GetAllRestoringFileInfo()
	cp.restoringFiles.pos["db"]["table"]["file"][0] = 20
	cp.restoringFiles.pos["db"]["table"]["file3"] = []int64{0, 100}
	c.Assert(ret, DeepEquals, map[string][]int64{"file": {10, 100}, "file2": {0, 100}})
}
