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
	"strings"

	. "github.com/pingcap/check"
)

func (t *testServer) TestConfig(c *C) {
	cfg := NewConfig()

	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), IsNil)
	c.Assert(cfg.SourceID, Equals, "mysql-replica-01")

	dir := c.MkDir()
	cfg.ConfigFile = path.Join(dir, "dm-worker.toml")

	// test clone
	clone1 := cfg.Clone()
	c.Assert(cfg, DeepEquals, clone1)
	clone1.SourceID = "xxx"
	c.Assert(cfg.SourceID, Equals, "mysql-replica-01")

	// test format
	c.Assert(strings.Contains(cfg.String(), "mysql-replica-01"), IsTrue)
	tomlStr, err := clone1.Toml()
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(tomlStr, "xxx"), IsTrue)
	originCfgStr, err := cfg.Toml()
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(tomlStr, "mysql-replica-01"), IsTrue)

	// test update config file and reload
	c.Assert(cfg.UpdateConfigFile(tomlStr), IsNil)
	cfg.Reload()
	c.Assert(err, IsNil)
	c.Assert(cfg.SourceID, Equals, "xxx")
	c.Assert(cfg.UpdateConfigFile(originCfgStr), IsNil)
	cfg.Reload()
	c.Assert(err, IsNil)
	c.Assert(cfg.SourceID, Equals, "mysql-replica-01")

	// test decrypt password
	clone1.From.Password = "1234"
	clone1.SourceID = "mysql-replica-01"
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
