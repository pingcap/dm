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

package worker

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-enterprise-tools/dm/config"
	"github.com/pingcap/tidb-enterprise-tools/pkg/utils"
)

// NewConfig creates a new base config for worker.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.flagSet = flag.NewFlagSet("worker", flag.ContinueOnError)
	fs := cfg.flagSet

	fs.BoolVar(&cfg.printVersion, "V", false, "prints version and exit")
	fs.StringVar(&cfg.ConfigFile, "config", "", "path to config file")
	fs.StringVar(&cfg.WorkerAddr, "worker-addr", "", "worker API server addr")
	fs.StringVar(&cfg.StatusAddr, "status-addr", "", "status addr")
	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.LogFile, "log-file", "", "log file path")
	fs.StringVar(&cfg.LogRotate, "log-rotate", "day", "log file rotate type, hour/day")
	fs.StringVar(&cfg.RelayDir, "relay-dir", "./relay_log", "relay log directory")

	return cfg
}

// Config is the configuration.
type Config struct {
	flagSet *flag.FlagSet

	LogLevel  string `toml:"log-level" json:"log-level"`
	LogFile   string `toml:"log-file" json:"log-file"`
	LogRotate string `toml:"log-rotate" json:"log-rotate"`

	WorkerAddr string `toml:"worker-addr" json:"worker-addr"`
	StatusAddr string `toml:"status-addr" json:"status-addr"`

	EnableGTID  bool   `toml:"enable-gtid" json:"enable-gtid"`
	AutoFixGTID bool   `toml:"auto-fix-gtid" json:"auto-fix-gtid"`
	MetaFile    string `toml:"meta-file" json:"meta-file"`
	RelayDir    string `toml:"relay-dir" json:"relay-dir"`
	ServerID    int    `toml:"server-id" json:"server-id"`
	Flavor      string `toml:"flavor" json:"flavor"`
	Charset     string `toml:"charset" json:"charset"`

	From config.DBConfig `toml:"from" json:"from"`

	ConfigFile string `json:"config-file"`

	printVersion bool
}

func (c *Config) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		log.Errorf("[worker] marshal config to json error %v", err)
	}
	return string(cfg)
}

// Toml returns TOML format representation of config
func (c *Config) Toml() (string, error) {
	var b bytes.Buffer
	var pswd string
	var err error

	enc := toml.NewEncoder(&b)
	if len(c.From.Password) > 0 {
		pswd, err = utils.Encrypt(c.From.Password)
		if err != nil {
			return "", errors.Annotatef(err, "can not encrypt password %s", c.From.Password)
		}
	}
	c.From.Password = pswd

	err = enc.Encode(c)
	if err != nil {
		log.Errorf("[worker] marshal config to toml error %v", err)
	}
	if len(c.From.Password) > 0 {
		pswd, err = utils.Decrypt(c.From.Password)
		if err != nil {
			return "", errors.Annotatef(err, "can not decrypt password %s", c.From.Password)
		}
	}
	c.From.Password = pswd
	return string(b.String()), nil
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.flagSet.Parse(arguments)
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
	err = c.flagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	if len(c.flagSet.Args()) != 0 {
		return errors.Errorf("'%s' is an invalid flag", c.flagSet.Arg(0))
	}

	// try decrypt password
	var pswd string
	if len(c.From.Password) > 0 {
		pswd, err = utils.Decrypt(c.From.Password)
		if err != nil {
			return errors.Annotatef(err, "can not decrypt password %s", c.From.Password)
		}
	}
	c.From.Password = pswd

	return nil
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	return errors.Trace(err)
}

// UpdateConfigFile write configure to local file
func (c *Config) UpdateConfigFile(content string) error {
	if c.ConfigFile == "" {
		c.ConfigFile = "dm-worker-config.bak"
	}
	err := ioutil.WriteFile(c.ConfigFile, []byte(content), 0666)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Reload reload configure from ConfigFile
func (c *Config) Reload() error {
	var pswd string
	var err error

	if c.ConfigFile == "" {
		c.ConfigFile = "dm-worker-config.bak"
	}

	err = c.configFromFile(c.ConfigFile)
	if err != nil {
		return errors.Trace(err)
	}

	if len(c.From.Password) > 0 {
		pswd, err = utils.Decrypt(c.From.Password)
		if err != nil {
			return errors.Annotatef(err, "can not decrypt password %s", c.From.Password)
		}
	}
	c.From.Password = pswd

	return nil
}
