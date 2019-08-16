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

package tracer

import (
	"encoding/json"
	"flag"
	"fmt"

	"github.com/BurntSushi/toml"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

// NewConfig creates a new base config for tracer.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.flagSet = flag.NewFlagSet("tracer", flag.ContinueOnError)
	fs := cfg.flagSet

	fs.BoolVar(&cfg.printVersion, "V", false, "prints version and exit")
	fs.StringVar(&cfg.ConfigFile, "config", "", "path to config file")
	fs.BoolVar(&cfg.Enable, "enable", false, "whether to enable tracer")
	fs.StringVar(&cfg.TracerAddr, "tracer-addr", ":8263", "tracer API server and status addr")
	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.LogFile, "log-file", "log/dm-tracer.log", "log file path")

	return cfg
}

// Config is the configuration.
type Config struct {
	flagSet *flag.FlagSet

	LogLevel   string `toml:"log-level" json:"log-level"`
	LogFile    string `toml:"log-file" json:"log-file"`
	TracerAddr string `toml:"tracer-addr" json:"tracer-addr"`
	Enable     bool   `toml:"enable" json:"enable"`
	Checksum   bool   `toml:"checksum" json:"checksum"`

	ConfigFile string `json:"config-file"`

	printVersion bool
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.flagSet.Parse(arguments)
	if err != nil {
		return terror.ErrTracerParseFlagSet.Delegate(err)
	}

	if c.printVersion {
		fmt.Println(utils.GetRawInfo())
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
	err = c.flagSet.Parse(arguments)
	if err != nil {
		return terror.ErrTracerParseFlagSet.Delegate(err)
	}

	if len(c.flagSet.Args()) != 0 {
		return terror.ErrTracerConfigInvalidFlag.Generate(c.flagSet.Arg(0))
	}

	return nil
}

// String returns format string of Config
func (c *Config) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		log.L().Error("marshal tracer config to json", log.ShortError(err))
	}
	return string(cfg)
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	return terror.ErrTracerConfigTomlTransform.Delegate(err)
}
