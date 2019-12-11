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

package common

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/pingcap/dm/pkg/utils"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
)

const (
	defaultRPCTimeout = "10m"

	// EncryptCmdName is special command
	EncryptCmdName = "encrypt"
)

// NewConfig creates a new base config for dmctl.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("dmctl", flag.ContinueOnError)

	// ignore default help usage
	cfg.FlagSet.Usage = func() {}
	fs := cfg.FlagSet

	fs.BoolVar(&cfg.printVersion, "V", false, "prints version and exit")
	fs.StringVar(&cfg.ConfigFile, "config", "", "path to config file")
	fs.StringVar(&cfg.MasterAddr, "master-addr", "", "master API server addr")
	fs.StringVar(&cfg.RPCTimeoutStr, "rpc-timeout", defaultRPCTimeout, fmt.Sprintf("rpc timeout, default is %s", defaultRPCTimeout))
	fs.StringVar(&cfg.encrypt, EncryptCmdName, "", "encrypt plaintext to ciphertext")

	return cfg
}

// Config is the configuration.
type Config struct {
	*flag.FlagSet `json:"-"`

	MasterAddr string `toml:"master-addr" json:"master-addr"`

	RPCTimeoutStr string        `toml:"rpc-timeout" json:"rpc-timeout"`
	RPCTimeout    time.Duration `json:"-"`

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
func (c *Config) Parse(arguments []string) (finish bool, err error) {
	err = c.FlagSet.Parse(arguments)
	if err != nil {
		return false, errors.Trace(err)
	}

	if c.printVersion {
		fmt.Println(utils.GetRawInfo())
		return true, nil
	}

	if len(c.encrypt) > 0 {
		ciphertext, err1 := utils.Encrypt(c.encrypt)
		if err1 != nil {
			return true, err1
		}
		fmt.Println(ciphertext)
		return true, nil
	}

	// Load config file if specified.
	if c.ConfigFile != "" {
		err = c.configFromFile(c.ConfigFile)
		if err != nil {
			return false, errors.Trace(err)
		}
	}

	// Parse again to replace with command line options.
	err = c.FlagSet.Parse(arguments)
	if err != nil {
		return false, errors.Trace(err)
	}

	if len(c.FlagSet.Args()) != 0 {
		return false, errors.Errorf("'%s' is an invalid flag", c.FlagSet.Arg(0))
	}

	if c.MasterAddr == "" {
		return false, flag.ErrHelp
	}

	return false, errors.Trace(c.adjust())
}

// Validate check config is ready to execute commmand
func (c *Config) Validate() error {
	if c.MasterAddr == "" {
		return errors.New("--master-addr not provided")
	}
	if _, err := validateAddr(c.MasterAddr); err != nil {
		return errors.Annotatef(err, "specify master addr %s", c.MasterAddr)
	}
	return nil
}

// getMasterAddrs return config master addresses
func (c *Config) getMasterAddrs() []string {
	return strings.Split(c.MasterAddr, ",")
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	return errors.Trace(err)
}

// adjust adjusts configs
func (c *Config) adjust() error {
	if c.RPCTimeoutStr == "" {
		c.RPCTimeoutStr = defaultRPCTimeout
	}
	timeout, err := time.ParseDuration(c.RPCTimeoutStr)
	if err != nil {
		return errors.Trace(err)
	}
	if timeout <= time.Duration(0) {
		return errors.Errorf("invalid time duration: %s", c.RPCTimeoutStr)
	}
	c.RPCTimeout = timeout
	return nil
}

// validate host1:port1,host2:port2,... format address
// return a string that remove spaces and empty segments
func validateAddr(addrStr string) (string, error) {
	addrs := strings.Split(addrStr, ",")
	validAddrs := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		addr := strings.TrimSpace(addr)
		if addr != "" {
			_, _, err := net.SplitHostPort(addr)
			if err != nil {
				return "", errors.Trace(err)
			}
			validAddrs = append(validAddrs, addr)
		}
	}
	if len(validAddrs) == 0 {
		return "", errors.Errorf("invalid address: '%s'", addrStr)
	}
	return strings.Join(validAddrs, ","), nil
}
