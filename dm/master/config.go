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
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"

	"github.com/BurntSushi/toml"
	"go.uber.org/zap"
)

// SampleConfigFile is sample config file of dm-master
// later we can read it from dm/master/dm-master.toml
// and assign it to SampleConfigFile while we build dm-master
var SampleConfigFile string

var defaultRPCTimeout = "30s"

// NewConfig creates a config for dm-master
func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("dm-master", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.BoolVar(&cfg.printVersion, "V", false, "prints version and exit")
	fs.BoolVar(&cfg.printSampleConfig, "print-sample-config", false, "print sample config file of dm-worker")
	fs.StringVar(&cfg.ConfigFile, "config", "", "path to config file")
	fs.StringVar(&cfg.MasterAddr, "master-addr", "", "master API server and status addr")
	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.LogFile, "log-file", "", "log file path")
	fs.StringVar(&cfg.LogFormat, "log-format", "text", `the format of the log, "text" or "json"`)
	//fs.StringVar(&cfg.LogRotate, "log-rotate", "day", "log file rotate type, hour/day")

	return cfg
}

// DeployMapper defines dm-worker's deploy mapper info: source id -> dm-worker ${host:ip}
type DeployMapper struct {
	MySQL  string `toml:"mysql-instance" json:"mysql-instance"` //  deprecated, use source-id instead
	Source string `toml:"source-id" json:"source-id"`           // represents a MySQL/MariaDB instance or a replica group
	Worker string `toml:"dm-worker" json:"dm-worker"`
}

// Verify verifies deploy configuration
func (d *DeployMapper) Verify() error {
	if d.MySQL == "" && d.Source == "" {
		return terror.ErrMasterDeployMapperVerify.Generate(d)
	}

	return nil
}

// Config is the configuration for dm-master
type Config struct {
	*flag.FlagSet `json:"-"`

	LogLevel  string `toml:"log-level" json:"log-level"`
	LogFile   string `toml:"log-file" json:"log-file"`
	LogFormat string `toml:"log-format" json:"log-format"`
	LogRotate string `toml:"log-rotate" json:"log-rotate"`

	RPCTimeoutStr string        `toml:"rpc-timeout" json:"rpc-timeout"`
	RPCTimeout    time.Duration `json:"-"`
	RPCRateLimit  float64       `toml:"rpc-rate-limit" json:"rpc-rate-limit"`
	RPCRateBurst  int           `toml:"rpc-rate-burst" json:"rpc-rate-burst"`

	MasterAddr string `toml:"master-addr" json:"master-addr"`

	Deploy    []*DeployMapper   `toml:"deploy" json:"-"`
	DeployMap map[string]string `json:"deploy"`

	ConfigFile string `json:"config-file"`

	printVersion      bool
	printSampleConfig bool
}

func (c *Config) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		log.L().Error("marshal to json", zap.Reflect("master config", c), log.ShortError(err))
	}
	return string(cfg)
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.FlagSet.Parse(arguments)
	if err != nil {
		return terror.ErrMasterConfigParseFlagSet.Delegate(err)
	}

	if c.printVersion {
		fmt.Println(utils.GetRawInfo())
		return flag.ErrHelp
	}

	if c.printSampleConfig {
		if strings.TrimSpace(SampleConfigFile) == "" {
			fmt.Println("sample config file of dm-master is empty")
		} else {
			rawConfig, err2 := base64.StdEncoding.DecodeString(SampleConfigFile)
			if err2 != nil {
				fmt.Println("base64 decode config error:", err2)
			} else {
				fmt.Println(string(rawConfig))
			}
		}
		return flag.ErrHelp
	}

	// Load config file if specified.
	if c.ConfigFile != "" {
		err = c.configFromFile(c.ConfigFile)
		if err != nil {
			return err
		}
	}

	// Parse again to replace with command line options.
	err = c.FlagSet.Parse(arguments)
	if err != nil {
		return terror.ErrMasterConfigParseFlagSet.Delegate(err)
	}

	if len(c.FlagSet.Args()) != 0 {
		return terror.ErrMasterConfigInvalidFlag.Generate(c.FlagSet.Arg(0))
	}

	return c.adjust()
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) error {
	metaData, err := toml.DecodeFile(path, c)
	if err != nil {
		return terror.ErrMasterConfigTomlTransform.Delegate(err)
	}
	undecoded := metaData.Undecoded()
	if len(undecoded) > 0 {
		var undecodedItems []string
		for _, item := range undecoded {
			undecodedItems = append(undecodedItems, item.String())
		}
		return terror.ErrMasterConfigUnknownItem.Generate(strings.Join(undecodedItems, ","))
	}
	return nil
}

// adjust adjusts configs
func (c *Config) adjust() error {
	c.DeployMap = make(map[string]string)
	for _, item := range c.Deploy {
		if err := item.Verify(); err != nil {
			return err
		}

		// compatible with mysql instance which is deprecated
		if item.Source == "" {
			item.Source = item.MySQL
		}

		c.DeployMap[item.Source] = item.Worker
	}

	if c.RPCTimeoutStr == "" {
		c.RPCTimeoutStr = defaultRPCTimeout
	}
	timeout, err := time.ParseDuration(c.RPCTimeoutStr)
	if err != nil {
		return terror.ErrMasterConfigTimeoutParse.Delegate(err)
	}
	c.RPCTimeout = timeout

	// for backward compatibility
	if c.RPCRateLimit <= 0 {
		log.L().Warn("invalid rpc-rate-limit, default value used", zap.Float64("specified rpc-rate-limit", c.RPCRateLimit), zap.Float64("default rpc-rate-limit", DefaultRate))
		c.RPCRateLimit = DefaultRate
	}
	if c.RPCRateBurst <= 0 {
		log.L().Warn("invalid rpc-rate-burst, default value use", zap.Int("specified rpc-rate-burst", c.RPCRateBurst), zap.Int("default rpc-rate-burst", DefaultBurst))
		c.RPCRateBurst = DefaultBurst
	}

	return nil
}

// UpdateConfigFile write config to local file
// if ConfigFile is nil, it will write to dm-master.toml
func (c *Config) UpdateConfigFile(content string) error {
	if c.ConfigFile != "" {
		err := ioutil.WriteFile(c.ConfigFile, []byte(content), 0666)
		return terror.ErrMasterConfigUpdateCfgFile.Delegate(err)
	}
	c.ConfigFile = "dm-master.toml"
	err := ioutil.WriteFile(c.ConfigFile, []byte(content), 0666)
	return terror.ErrMasterConfigUpdateCfgFile.Delegate(err)
}

// Reload load config from local file
func (c *Config) Reload() error {
	if c.ConfigFile != "" {
		err := c.configFromFile(c.ConfigFile)
		if err != nil {
			return err
		}
	}

	return c.adjust()
}
