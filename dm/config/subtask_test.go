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

package config

import (
	. "github.com/pingcap/check"
)

func (t *testConfig) TestSubTask(c *C) {
	cfg := &SubTaskConfig{
		Name:            "test-task",
		SourceID:        "mysql-instance-01",
		OnlineDDLScheme: "pt",
		Timezone:        "Asia/Shanghai",
		From: DBConfig{
			Host:     "127.0.0.1",
			Port:     3306,
			User:     "root",
			Password: "Up8156jArvIPymkVC+5LxkAT6rek",
		},
		To: DBConfig{
			Host:     "127.0.0.1",
			Port:     4306,
			User:     "root",
			Password: "",
		},
	}
	cfg.From.Adjust()
	cfg.To.Adjust()

	clone1, err := cfg.Clone()
	c.Assert(err, IsNil)
	c.Assert(cfg, DeepEquals, clone1)

	clone1.From.Password = "1234"
	clone2, err := cfg.DecryptPassword()
	c.Assert(err, IsNil)
	c.Assert(clone2, DeepEquals, clone1)

	cfg.From.Password = "xxx"
	_, err = cfg.DecryptPassword()
	c.Assert(err, NotNil)
	err = cfg.Adjust(true)
	c.Assert(err, NotNil)
	err = cfg.Adjust(false)
	c.Assert(err, IsNil)

	cfg.From.Password = ""
	clone3, err := cfg.DecryptPassword()
	c.Assert(err, IsNil)
	c.Assert(clone3, DeepEquals, cfg)

	err = cfg.Adjust(true)
	c.Assert(err, IsNil)
}

func (t *testConfig) TestSubTaskAdjustFail(c *C) {
	newSubTaskConfig := func() *SubTaskConfig {
		return &SubTaskConfig{
			Name:            "test-task",
			SourceID:        "mysql-instance-01",
			OnlineDDLScheme: "pt",
			Timezone:        "Asia/Shanghai",
			From: DBConfig{
				Host:     "127.0.0.1",
				Port:     3306,
				User:     "root",
				Password: "Up8156jArvIPymkVC+5LxkAT6rek",
			},
			To: DBConfig{
				Host:     "127.0.0.1",
				Port:     4306,
				User:     "root",
				Password: "",
			},
		}
	}
	testCases := []struct {
		genFunc     func() *SubTaskConfig
		errorFormat string
	}{
		{
			func() *SubTaskConfig {
				cfg := newSubTaskConfig()
				cfg.Name = ""
				return cfg
			},
			"\\[.*\\] task name should not be empty",
		},
		{
			func() *SubTaskConfig {
				cfg := newSubTaskConfig()
				cfg.SourceID = ""
				return cfg
			},
			"\\[.*\\] empty source-id not valid",
		},
		{
			func() *SubTaskConfig {
				cfg := newSubTaskConfig()
				cfg.SourceID = "source-id-length-more-than-thirty-two"
				return cfg
			},
			"\\[.*\\] too long source-id not valid",
		},
		{
			func() *SubTaskConfig {
				cfg := newSubTaskConfig()
				cfg.OnlineDDLScheme = "rtc"
				return cfg
			},
			"\\[.*\\] online scheme rtc not supported",
		},
		{
			func() *SubTaskConfig {
				cfg := newSubTaskConfig()
				cfg.Timezone = "my-house"
				return cfg
			},
			"\\[.*\\] invalid timezone string: my-house:.*",
		},
	}

	for _, tc := range testCases {
		cfg := tc.genFunc()
		err := cfg.Adjust(true)
		c.Assert(err, ErrorMatches, tc.errorFormat)
	}
}
