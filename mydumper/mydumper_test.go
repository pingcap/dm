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
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/baseconn"
	"github.com/pingcap/dm/pkg/retry"
)

var _ = Suite(&testMydumperSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testMydumperSuite struct {
	cfg             *config.SubTaskConfig
	origNewBaseConn func(string, retry.Strategy, *baseconn.RawDBConfig) (*baseconn.BaseConn, error)
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

	m.origNewBaseConn = newBaseConn
	newBaseConn = func(dbDSN string, strategy retry.Strategy, rawDBCfg *baseconn.RawDBConfig) (*baseconn.BaseConn, error) {
		return &baseconn.BaseConn{}, nil
	}
	c.Assert(failpoint.Enable("github.com/pingcap/dm/pkg/utils/mockGenerateExtraArgs", "return(true)"), IsNil)
}

func (m *testMydumperSuite) TearDownSuite(c *C) {
	newBaseConn = m.origNewBaseConn
	failpoint.Disable("github.com/pingcap/dm/pkg/utils/mockGenerateExtraArgs")
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
