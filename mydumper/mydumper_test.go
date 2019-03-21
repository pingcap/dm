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

	"github.com/pingcap/dm/dm/config"
)

var _ = Suite(&testMydumperSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testMydumperSuite struct {
	cfg *config.SubTaskConfig
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
}

func (m *testMydumperSuite) TestArgs(c *C) {
	expected := strings.Fields("--host 127.0.0.1 --port 3306 --user root " +
		"--outputdir ./dumped_data --threads 4 --chunk-filesize 64 --skip-tz-utc " +
		"--regex ^(?!(mysql|information_schema|performance_schema)) " +
		"--password 123")
	m.cfg.MydumperConfig.ExtraArgs = "--regex '^(?!(mysql|information_schema|performance_schema))'"
	mydumper := NewMydumper(m.cfg)
	args := mydumper.constructArgs()
	c.Assert(args, DeepEquals, expected)
}
