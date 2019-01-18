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
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/pingcap/dm/pkg/log"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/streamer"
	"github.com/pingcap/dm/pkg/utils"
	"github.com/pingcap/dm/relay/purger"
)

// NewConfig creates a new base config for worker.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.flagSet = flag.NewFlagSet("worker", flag.ContinueOnError)
	fs := cfg.flagSet

	fs.BoolVar(&cfg.printVersion, "V", false, "prints version and exit")
	fs.StringVar(&cfg.ConfigFile, "config", "", "path to config file")
	fs.StringVar(&cfg.WorkerAddr, "worker-addr", "", "worker API server and status addr")
	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.LogFile, "log-file", "", "log file path")
	//fs.StringVar(&cfg.LogRotate, "log-rotate", "day", "log file rotate type, hour/day")
	fs.StringVar(&cfg.RelayDir, "relay-dir", "./relay_log", "relay log directory")
	fs.Int64Var(&cfg.Purge.Interval, "purge-interval", 60*60, "interval (seconds) try to check whether needing to purge relay log files")
	fs.Int64Var(&cfg.Purge.Expires, "purge-expires", 0, "try to purge relay log files if their modified time is older than this (hours)")
	fs.Int64Var(&cfg.Purge.RemainSpace, "purge-remain-space", 15, "try to purge relay log files if remain space is less than this (GB)")

	return cfg
}

// Config is the configuration.
type Config struct {
	flagSet *flag.FlagSet

	LogLevel  string `toml:"log-level" json:"log-level"`
	LogFile   string `toml:"log-file" json:"log-file"`
	LogRotate string `toml:"log-rotate" json:"log-rotate"`

	WorkerAddr string `toml:"worker-addr" json:"worker-addr"`

	EnableGTID  bool   `toml:"enable-gtid" json:"enable-gtid"`
	AutoFixGTID bool   `toml:"auto-fix-gtid" json:"auto-fix-gtid"`
	MetaFile    string `toml:"meta-file" json:"meta-file"`
	RelayDir    string `toml:"relay-dir" json:"relay-dir"`
	ServerID    int    `toml:"server-id" json:"server-id"`
	Flavor      string `toml:"flavor" json:"flavor"`
	Charset     string `toml:"charset" json:"charset"`

	// relay synchronous starting point (if specified)
	RelayBinLogName string `toml:"relay-binlog-name" json:"relay-binlog-name"`
	RelayBinlogGTID string `toml:"relay-binlog-gtid" json:"relay-binlog-gtid"`

	SourceID string          `toml:"source-id" json:"source-id"`
	From     config.DBConfig `toml:"from" json:"from"`

	// config items for purger
	Purge purger.Config `toml:"purge" json:"purge"`

	ConfigFile string `json:"config-file"`

	printVersion bool
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

	return c.verify()
}

// verify verifies the config
func (c *Config) verify() error {
	if len(c.SourceID) == 0 {
		return errors.Errorf("dm-worker should bind a non-empty source ID which represents a MySQL/MariaDB instance or a replica group. \n notice: if you use old version dm-ansible, please update to newest version.")
	}

	if len(c.RelayBinLogName) > 0 {
		_, err := streamer.GetBinlogFileIndex(c.RelayBinLogName)
		if err != nil {
			return errors.Annotatef(err, "relay-binlog-name %s", c.RelayBinLogName)
		}
	}
	if len(c.RelayBinlogGTID) > 0 {
		_, err := gtid.ParserGTID(c.Flavor, c.RelayBinlogGTID)
		if err != nil {
			return errors.Annotatef(err, "relay-binlog-gtid %s", c.RelayBinlogGTID)
		}
	}
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
