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

package master

import (
	"encoding/base64"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"

	capturer "github.com/kami-zh/go-capturer"
	"github.com/pingcap/check"
)

var (
	defaultConfigFile = "./dm-master.toml"
)

func (t *testMaster) TestPrintSampleConfig(c *check.C) {
	var (
		buf    []byte
		err    error
		encode string
		out    string
	)

	defer func() {
		SampleConfigFile = ""
	}()

	// test valid sample config
	out = capturer.CaptureStdout(func() {
		buf, err = ioutil.ReadFile(defaultConfigFile)
		c.Assert(err, check.IsNil)
		encode = base64.StdEncoding.EncodeToString(buf)
		SampleConfigFile = encode

		cfg := NewConfig()
		err = cfg.Parse([]string{"-print-sample-config"})
		c.Assert(err, check.ErrorMatches, flag.ErrHelp.Error())
	})
	c.Assert(strings.TrimSpace(out), check.Equals, strings.TrimSpace(string(buf)))

	// test invalid base64 encoded sample config
	out = capturer.CaptureStdout(func() {
		SampleConfigFile = "invalid base64 encode string"

		cfg := NewConfig()
		err = cfg.Parse([]string{"-print-sample-config"})
		c.Assert(err, check.ErrorMatches, flag.ErrHelp.Error())
	})
	c.Assert(strings.TrimSpace(out), check.Matches, "base64 decode config error:.*")
}

func (t *testMaster) TestConfig(c *check.C) {
	var (
		err        error
		cfg        = &Config{}
		masterAddr = ":8261"
		deployMap  = map[string]string{
			"mysql-replica-01": "172.16.10.72:8262",
			"mysql-replica-02": "172.16.10.73:8262",
		}
		cases = []struct {
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
				[]string{"-print-sample-config"},
				true,
				flag.ErrHelp.Error(),
			},
			{
				[]string{"invalid"},
				true,
				"'invalid' is an invalid flag",
			},
			{
				[]string{"--config=./dm-master.toml"},
				false,
				"",
			},
		}
	)

	cfg.ConfigFile = defaultConfigFile
	err = cfg.Reload()
	c.Assert(err, check.IsNil)
	c.Assert(cfg.MasterAddr, check.Equals, masterAddr)
	c.Assert(cfg.DeployMap, check.DeepEquals, deployMap)

	for _, tc := range cases {
		cfg = NewConfig()
		err = cfg.Parse(tc.args)
		if tc.hasError {
			c.Assert(err, check.ErrorMatches, tc.errorReg)
		} else {
			c.Assert(cfg.MasterAddr, check.Equals, masterAddr)
			c.Assert(cfg.DeployMap, check.DeepEquals, deployMap)
			c.Assert(cfg.String(), check.Matches, fmt.Sprintf("{.*master-addr\":\"%s\".*}", masterAddr))
		}
	}
}

func (t *testMaster) TestUpdateConfig(c *check.C) {
	var (
		err        error
		content    []byte
		newContent []byte
		newCfgPath string
	)
	newCfgPath = path.Join(c.MkDir(), "test_config.toml")
	c.Assert(err, check.IsNil)

	cfg := &Config{}
	cfg.configFromFile(defaultConfigFile)

	content, err = ioutil.ReadFile(defaultConfigFile)
	c.Assert(err, check.IsNil)

	// update config to a new file
	cfg.ConfigFile = newCfgPath
	err = cfg.UpdateConfigFile(string(content))
	c.Assert(err, check.IsNil)
	newContent, err = ioutil.ReadFile(newCfgPath)
	c.Assert(err, check.IsNil)
	c.Assert(newContent, check.DeepEquals, content)

	// update config to the default config file
	cfg.ConfigFile = ""
	err = cfg.UpdateConfigFile(string(content))
	c.Assert(err, check.IsNil)
	newContent, err = ioutil.ReadFile(defaultConfigFile)
	c.Assert(err, check.IsNil)
	c.Assert(newContent, check.DeepEquals, content)
}

func (t *testMaster) TestInvalidConfig(c *check.C) {
	var (
		f   *os.File
		err error
		cfg = NewConfig()
	)
	f, err = ioutil.TempFile(".", "test_invalid_config.toml")
	c.Assert(err, check.IsNil)
	defer os.Remove(f.Name())

	// test config Verify failed
	f.Write([]byte(`
master-addr = ":8261"

[[deploy]]
dm-worker = "172.16.10.72:8262"`))
	err = cfg.configFromFile(f.Name())
	c.Assert(err, check.IsNil)
	err = cfg.adjust()
	c.Assert(err, check.ErrorMatches, "user should specify valid relation between source\\(mysql/mariadb\\) and dm-worker.*")

	// test invalid config file content
	f.Write([]byte("invalid toml file"))
	err = cfg.Parse([]string{fmt.Sprintf("-config=%s", f.Name())})
	c.Assert(err, check.NotNil)
	cfg.ConfigFile = f.Name()
	err = cfg.Reload()
	c.Assert(err, check.NotNil)
}
