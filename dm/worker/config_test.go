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
)

func (t *testServer) TestConfig(c *C) {
	cfg := NewConfig()

	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), IsNil)
	c.Assert(cfg.SourceID, Equals, "mysql-replica-01")
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
	clone3, err := cfg.DecryptPassword()
	c.Assert(err, NotNil)

	cfg.From.Password = ""
	clone3, err = cfg.DecryptPassword()
	c.Assert(clone3, DeepEquals, cfg)
}
