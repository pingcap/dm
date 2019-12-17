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
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"strings"

	"github.com/BurntSushi/toml"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

// SampleConfigFile is sample config file of dm-worker
// later we can read it from dm/worker/dm-worker.toml
// and assign it to SampleConfigFile while we build dm-worker
var SampleConfigFile string

var (
	getAllServerIDFunc = utils.GetAllServerID
)

// NewConfig creates a new base config for worker.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.flagSet = flag.NewFlagSet("worker", flag.ContinueOnError)
	fs := cfg.flagSet

	fs.BoolVar(&cfg.printVersion, "V", false, "prints version and exit")
	fs.BoolVar(&cfg.printSampleConfig, "print-sample-config", false, "print sample config file of dm-worker")
	fs.StringVar(&cfg.ConfigFile, "config", "", "path to config file")
	fs.StringVar(&cfg.WorkerAddr, "worker-addr", "", "worker API server and status addr")
	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.LogFile, "log-file", "", "log file path")
	//fs.StringVar(&cfg.LogRotate, "log-rotate", "day", "log file rotate type, hour/day")
	fs.StringVar(&cfg.Join, "join", "", "join to an existing cluster (usage: cluster's '${advertise-client-urls}'")
	fs.StringVar(&cfg.Name, "name", "", "human-readable name for DM-worker member")
	return cfg
}

// Config is the configuration.
type Config struct {
	flagSet *flag.FlagSet
	Name    string `toml:"name" json:"name"`

	LogLevel  string `toml:"log-level" json:"log-level"`
	LogFile   string `toml:"log-file" json:"log-file"`
	LogRotate string `toml:"log-rotate" json:"log-rotate"`

	Join       string `toml:"string" json:"join" `
	WorkerAddr string `toml:"worker-addr" json:"worker-addr"`

	ConfigFile string `json:"config-file"`

	printVersion      bool
	printSampleConfig bool
}

// Clone clones a config
func (c *Config) Clone() *Config {
	clone := &Config{}
	*clone = *c
	return clone
}

func (c *Config) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		log.L().Error("fail to marshal config to json", log.ShortError(err))
	}
	return string(cfg)
}

// Toml returns TOML format representation of config
func (c *Config) Toml() (string, error) {
	var b bytes.Buffer

	err := toml.NewEncoder(&b).Encode(c)
	if err != nil {
		log.L().Error("fail to marshal config to toml", log.ShortError(err))
	}

	return b.String(), nil
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.flagSet.Parse(arguments)
	if err != nil {
		return terror.ErrWorkerParseFlagSet.Delegate(err)
	}

	if c.printVersion {
		fmt.Println(utils.GetRawInfo())
		return flag.ErrHelp
	}

	if c.printSampleConfig {
		if strings.TrimSpace(SampleConfigFile) == "" {
			fmt.Println("sample config file of dm-worker is empty")
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
	err = c.flagSet.Parse(arguments)
	if err != nil {
		return terror.ErrWorkerParseFlagSet.Delegate(err)
	}

	if len(c.flagSet.Args()) != 0 {
		return terror.ErrWorkerInvalidFlag.Generate(c.flagSet.Arg(0))
	}

	return nil
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) error {
	metaData, err := toml.DecodeFile(path, c)
	if err != nil {
		return terror.ErrWorkerDecodeConfigFromFile.Delegate(err)
	}
	undecoded := metaData.Undecoded()
	if len(undecoded) > 0 && err == nil {
		var undecodedItems []string
		for _, item := range undecoded {
			undecodedItems = append(undecodedItems, item.String())
		}
		return terror.ErrWorkerUndecodedItemFromFile.Generate(strings.Join(undecodedItems, ","))
	}
	return nil
}

// Reload reload configure from ConfigFile
func (c *Config) Reload() error {
	if c.ConfigFile == "" {
		c.ConfigFile = "dm-worker-config.bak"
	}

	err := c.configFromFile(c.ConfigFile)
	if err != nil {
		return err
	}

	return nil
}
