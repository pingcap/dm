package tracer

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

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/pingcap/check"
)

var (
	defaultConfigFile = "./dm-tracer.toml"
)

type testTracer struct{}

var _ = check.Suite(&testTracer{})

func (t *testTracer) TestConfig(c *check.C) {
	var (
		err        error
		cfg        = &Config{}
		tracerAddr = ":8263"
		cases      = []struct {
			args     []string
			hasError bool
			errorReg string
		}{
			{
				[]string{"-V"},
				true,
				flag.ErrHelp.Error(),
			},
			{
				[]string{"invalid"},
				true,
				"'invalid' is an invalid flag",
			},
			{
				[]string{"--config=./dm-tracer.toml"},
				false,
				"",
			},
		}
	)

	err = cfg.configFromFile(defaultConfigFile)
	c.Assert(err, check.IsNil)
	c.Assert(cfg.TracerAddr, check.Equals, tracerAddr)
	c.Assert(cfg.Enable, check.IsTrue)

	for _, tc := range cases {
		cfg = NewConfig()
		err = cfg.Parse(tc.args)
		if tc.hasError {
			c.Assert(err, check.ErrorMatches, tc.errorReg)
		} else {
			c.Assert(cfg.TracerAddr, check.Equals, tracerAddr)
			c.Assert(cfg.String(), check.Matches, fmt.Sprintf("{.*tracer-addr\":\"%s\".*}", tracerAddr))
		}
	}
}

func (t *testTracer) TestInvalidConfig(c *check.C) {
	var (
		f   *os.File
		err error
		cfg = NewConfig()
	)
	f, err = ioutil.TempFile(".", "test_invalid_config.toml")
	c.Assert(err, check.IsNil)
	defer os.Remove(f.Name())

	// test invalid config file content
	f.Write([]byte("invalid toml file"))
	err = cfg.Parse([]string{fmt.Sprintf("-config=%s", f.Name())})
	c.Assert(err, check.NotNil)
}
