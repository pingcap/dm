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

package common

import (
	"encoding/json"
	"flag"
	"fmt"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/pingcap/tidb-enterprise-tools/pkg/utils"
)

// NewConfig creates a new base config for dmctl.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("dmctl", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.BoolVar(&cfg.printVersion, "V", false, "prints version and exit")
	fs.StringVar(&cfg.ConfigFile, "config", "", "path to config file")
	fs.StringVar(&cfg.MasterAddr, "master-addr", "", "master API server addr")
	fs.StringVar(&cfg.encrypt, "encrypt", "", "encrypt plaintext to ciphertext")

	return cfg
}

// Config is the configuration.
type Config struct {
	*flag.FlagSet `json:"-"`

	MasterAddr string `toml:"master-addr" json:"master-addr"`

	// supporting dmctl connect to dm-worker directly, but not export to config
	// now we don't add authentication in dm-worker's gRPC server
	WorkerAddr   string `toml:"worker-addr" json:"-"`
	ServerAddr   string `json:"-"` // MasterAddr or WorkerAddr
	IsWorkerAddr bool   `json:"-"`

	ConfigFile string `json:"config-file"`

	printVersion bool
	encrypt      string // string need to be encrypted
}

func (c *Config) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		fmt.Printf("marshal config to json error %v", err)
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

	if len(c.encrypt) > 0 {
		ciphertext, err := utils.Encrypt(c.encrypt)
		if err != nil {
			fmt.Println(errors.ErrorStack(err))
		} else {
			fmt.Println(ciphertext)
		}
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

	return errors.Trace(c.adjust())
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	return errors.Trace(err)
}

// adjust adjusts configs
func (c *Config) adjust() error {
	if c.MasterAddr != "" {
		c.ServerAddr = c.MasterAddr
		c.IsWorkerAddr = false
	} else {
		c.ServerAddr = c.WorkerAddr
		c.IsWorkerAddr = true
	}
	if c.ServerAddr == "" {
		return errors.Errorf("MasterAddr or WorkerAddr must be specified")
	}
	return nil
}
