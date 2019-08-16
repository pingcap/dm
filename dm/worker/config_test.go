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
	"fmt"
	"io/ioutil"
	"path"
	"strings"

	. "github.com/pingcap/check"

	"github.com/pingcap/dm/dm/config"
)

func (t *testServer) TestConfig(c *C) {
	cfg := NewConfig()

	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml", "-relay-dir=./xx"}), IsNil)
	c.Assert(cfg.RelayDir, Equals, "./xx")
	c.Assert(cfg.ServerID, Equals, 101)

	dir := c.MkDir()
	cfg.ConfigFile = path.Join(dir, "dm-worker.toml")

	// test clone
	clone1 := cfg.Clone()
	c.Assert(cfg, DeepEquals, clone1)
	clone1.ServerID = 100
	c.Assert(cfg.ServerID, Equals, 101)

	// test format
	c.Assert(cfg.String(), Matches, `.*"server-id":101.*`)
	tomlStr, err := clone1.Toml()
	c.Assert(err, IsNil)
	c.Assert(tomlStr, Matches, `(.|\n)*server-id = 100(.|\n)*`)
	originCfgStr, err := cfg.Toml()
	c.Assert(err, IsNil)
	c.Assert(originCfgStr, Matches, `(.|\n)*server-id = 101(.|\n)*`)

	// test update config file and reload
	c.Assert(cfg.UpdateConfigFile(tomlStr), IsNil)
	cfg.Reload()
	c.Assert(err, IsNil)
	c.Assert(cfg.ServerID, Equals, 100)
	c.Assert(cfg.UpdateConfigFile(originCfgStr), IsNil)
	cfg.Reload()
	c.Assert(err, IsNil)
	c.Assert(cfg.ServerID, Equals, 101)

	// test decrypt password
	clone1.From.Password = "1234"
	clone1.ServerID = 101
	clone2, err := cfg.DecryptPassword()
	c.Assert(err, IsNil)
	c.Assert(clone2, DeepEquals, clone1)

	cfg.From.Password = "xxx"
	_, err = cfg.DecryptPassword()
	c.Assert(err, NotNil)

	cfg.From.Password = ""
	clone3, err := cfg.DecryptPassword()
	c.Assert(err, IsNil)
	c.Assert(clone3, DeepEquals, cfg)

	// test invalid config
	dir2 := c.MkDir()
	configFile := path.Join(dir2, "dm-worker-invalid.toml")
	configContent := []byte(`
worker-addr = ":8262"
aaa = "xxx"
`)
	err = ioutil.WriteFile(configFile, configContent, 0644)
	c.Assert(err, IsNil)
	err = cfg.configFromFile(configFile)
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, ".*worker config contains unknown configuration options: aaa")
}

func (t *testServer) TestConfigVerify(c *C) {
	newConfig := func() *Config {
		cfg := NewConfig()
		c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml", "-relay-dir=./xx"}), IsNil)
		return cfg
	}
	testCases := []struct {
		genFunc     func() *Config
		errorFormat string
	}{
		{
			func() *Config {
				return newConfig()
			},
			"",
		},
		{
			func() *Config {
				cfg := newConfig()
				cfg.SourceID = ""
				return cfg
			},
			".*dm-worker should bind a non-empty source ID which represents a MySQL/MariaDB instance or a replica group.*",
		},
		{
			func() *Config {
				cfg := newConfig()
				cfg.SourceID = "source-id-length-more-than-thirty-two"
				return cfg
			},
			fmt.Sprintf(".*the length of source ID .* is more than max allowed value %d", config.MaxSourceIDLength),
		},
		{
			func() *Config {
				cfg := newConfig()
				cfg.RelayBinLogName = "mysql-binlog"
				return cfg
			},
			".*not valid.*",
		},
		{
			func() *Config {
				cfg := newConfig()
				cfg.RelayBinlogGTID = "9afe121c-40c2-11e9-9ec7-0242ac110002:1-rtc"
				return cfg
			},
			".*relay-binlog-gtid 9afe121c-40c2-11e9-9ec7-0242ac110002:1-rtc:.*",
		},
		{
			func() *Config {
				cfg := newConfig()
				cfg.From.Password = "not-encrypt"
				return cfg
			},
			"*decode base64 encoded password.*",
		},
	}

	for _, tc := range testCases {
		cfg := tc.genFunc()
		err := cfg.verify()
		if tc.errorFormat != "" {
			c.Assert(err, NotNil)
			lines := strings.Split(err.Error(), "\n")
			c.Assert(lines[0], Matches, tc.errorFormat)
		} else {
			c.Assert(err, IsNil)
		}
	}

}
