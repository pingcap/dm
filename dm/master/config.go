// Copyright 2018 PingCAP, Inc.
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
	"encoding/json"
	"flag"
	"fmt"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-enterprise-tools/pkg/utils"
)

// NewConfig creates a config for dm-master
func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("dm-master", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.BoolVar(&cfg.printVersion, "V", false, "prints version and exit")
	fs.StringVar(&cfg.ConfigFile, "config", "", "path to config file")
	fs.StringVar(&cfg.MasterAddr, "master-addr", "", "master API server addr")
	fs.StringVar(&cfg.StatusAddr, "status-addr", "", "status addr")
	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.LogFile, "log-file", "", "log file path")
	fs.StringVar(&cfg.LogRotate, "log-rotate", "day", "log file rotate type, hour/day")

	return cfg
}

// DeployMapper defines dm-worker's deploy mapper info
// source-db-host:ip -> dm-worker-host:ip
// NOTE: we can refine it when new deployment function is available
type DeployMapper struct {
	MySQL  string `toml:"mysql" json:"mysql"`
	Worker string `toml:"worker" json:"worker"`
}

// Config is the configuration for dm-master
type Config struct {
	*flag.FlagSet `json:"-"`

	LogLevel  string `toml:"log-level" json:"log-level"`
	LogFile   string `toml:"log-file" json:"log-file"`
	LogRotate string `toml:"log-rotate" json:"log-rotate"`

	MasterAddr string `toml:"master-addr" json:"master-addr"`
	StatusAddr string `toml:"status-addr" json:"status-addr"`

	Deploy    []*DeployMapper   `toml:"deploy" json:"-"`
	DeployMap map[string]string `json:"deploy"`

	ConfigFile string `json:"config-file"`

	printVersion bool
}

func (c *Config) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		log.Errorf("[dm-master] marshal config to json error %v", err)
	}
	return string(cfg)
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	if c.printVersion {
		fmt.Println(utils.GetRawInfo())
		return flag.ErrHelp
	}

	// Load config file if specified.
	if c.ConfigFile != "" {
		err = c.configFromFile(c.ConfigFile)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// Parse again to replace with command line options.
	err = c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	if len(c.FlagSet.Args()) != 0 {
		return errors.Errorf("'%s' is an invalid flag", c.FlagSet.Arg(0))
	}

	return c.adjust()
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	return errors.Trace(err)
}

// adjust adjusts configs
func (c *Config) adjust() error {
	c.DeployMap = make(map[string]string)
	for _, item := range c.Deploy {
		c.DeployMap[item.MySQL] = item.Worker
	}
	return nil
}
