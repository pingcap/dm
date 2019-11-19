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

package mydumper

import (
	"database/sql"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/conn"
)

var _ = Suite(&testMydumperSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testMydumperSuite struct {
	cfg                     *config.SubTaskConfig
	origApplyNewBaseDB      func(config config.DBConfig) (*conn.BaseDB, error)
	origFetchTargetDoTables func(*sql.DB, *filter.Filter, *router.Table) (map[string][]*filter.Table, error)
}

func (m *testMydumperSuite) SetUpSuite(c *C) {
	m.cfg = &config.SubTaskConfig{
		From: config.DBConfig{
			Host:     "127.0.0.1",
			User:     "root",
			Password: "123",
			Port:     3306,
		},
		MydumperConfig: config.MydumperConfig{
			MydumperPath:  "./bin/mydumper",
			Threads:       4,
			SkipTzUTC:     true,
			ChunkFilesize: 64,
		},
		LoaderConfig: config.LoaderConfig{
			Dir: "./dumped_data",
		},
	}

	m.origApplyNewBaseDB = applyNewBaseDB
	m.origFetchTargetDoTables = fetchTargetDoTables
	applyNewBaseDB = func(config config.DBConfig) (*conn.BaseDB, error) {
		return &conn.BaseDB{}, nil
	}
	fetchTargetDoTables = func(db *sql.DB, bw *filter.Filter, router *router.Table) (map[string][]*filter.Table, error) {
		mapper := make(map[string][]*filter.Table)
		mapper["mockDatabase"] = append(mapper["mockDatabase"], &filter.Table{
			Schema: "mockDatabase",
			Name:   "mockTable1",
		})
		mapper["mockDatabase"] = append(mapper["mockDatabase"], &filter.Table{
			Schema: "mockDatabase",
			Name:   "mockTable2",
		})
		return mapper, nil
	}
}

func (m *testMydumperSuite) TearDownSuite(c *C) {
	applyNewBaseDB = m.origApplyNewBaseDB
	fetchTargetDoTables = m.origFetchTargetDoTables
}

func generateArgsAndCompare(c *C, m *testMydumperSuite, expectedExtraArgs, extraArgs string) {
	expected := strings.Fields("--host 127.0.0.1 --port 3306 --user root " +
		"--outputdir ./dumped_data --threads 4 --chunk-filesize 64 --skip-tz-utc " +
		expectedExtraArgs + " --password 123")
	m.cfg.MydumperConfig.ExtraArgs = extraArgs

	mydumper := NewMydumper(m.cfg)
	args, err := mydumper.constructArgs()
	c.Assert(err, IsNil)
	c.Assert(args, DeepEquals, expected)
}

func testThroughGivenArgs(c *C, m *testMydumperSuite, arg, index string) {
	quotedIndex := "'" + index + "'" // add quotes for constructArgs
	generateArgsAndCompare(c, m, arg+" "+index, arg+" "+quotedIndex)
	generateArgsAndCompare(c, m, arg+"="+index, arg+"="+index)
}

func (m *testMydumperSuite) TestShouldNotGenerateExtraArgs(c *C) {
	// -x, --regex
	index := "^(?!(mysql|information_schema|performance_schema))"
	testThroughGivenArgs(c, m, "-x", index)
	testThroughGivenArgs(c, m, "--regex", index)
	// -T, --tables-list
	index = "testDatabase.testTable"
	testThroughGivenArgs(c, m, "-T", index)
	testThroughGivenArgs(c, m, "--tables-list", index)
	// -B, --database
	index = "testDatabase"
	testThroughGivenArgs(c, m, "-B", index)
	testThroughGivenArgs(c, m, "--database", index)
}

func (m *testMydumperSuite) TestShouldGenerateExtraArgs(c *C) {
	expectedMockResult := "--tables-list mockDatabase.mockTable1,mockDatabase.mockTable2"
	// empty extraArgs
	generateArgsAndCompare(c, m, expectedMockResult, "")
	// extraArgs doesn't contains -T/-B/-x args
	statement := "--statement-size=100"
	generateArgsAndCompare(c, m, statement+" "+expectedMockResult, statement)
}
