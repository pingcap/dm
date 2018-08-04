// Copyright 2016 PingCAP, Inc.
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

package loader

import (
	"encoding/json"
	"flag"
	"fmt"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-enterprise-tools/pkg/filter"
	"github.com/pingcap/tidb-enterprise-tools/pkg/utils"
	"github.com/pingcap/tidb-tools/pkg/table-router"
)

// NewConfig creates a new config.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("loader", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.StringVar(&cfg.Dir, "d", "./", "Directory of the dump to import")

	fs.IntVar(&cfg.PoolSize, "t", 16, "Number of threads restoring concurrently for worker pool. Each worker restore one file at a time, increase this as TiKV nodes increase")
	fs.StringVar(&cfg.CheckPointSchema, "checkpoint-schema", "tidb_loader", "schema name of checkpoint")

	fs.StringVar(&cfg.DB.Host, "h", "127.0.0.1", "The host to connect to")
	fs.StringVar(&cfg.DB.User, "u", "root", "Username with privileges to run the dump")
	fs.StringVar(&cfg.DB.Password, "p", "", "User password")
	fs.IntVar(&cfg.DB.Port, "P", 4000, "TCP/IP port to connect to")

	fs.StringVar(&cfg.PprofAddr, "pprof-addr", ":10084", "Loader pprof addr")
	fs.StringVar(&cfg.LogLevel, "L", "info", "Loader log level: debug, info, warn, error, fatal")

	fs.StringVar(&cfg.ConfigFile, "c", "", "config file")
	fs.BoolVar(&cfg.printVersion, "V", false, "prints version and exit")
	fs.BoolVar(&cfg.RemoveCheckpoint, "rm-checkpoint", false, "delete corresponding checkpoint records after the table is restored successfully")

	return cfg
}

// DBConfig is the DB configuration.
type DBConfig struct {
	Host     string `toml:"host" json:"host"`
	User     string `toml:"user" json:"user"`
	Password string `toml:"password" json:"-"`
	Port     int    `toml:"port" json:"port"`
}

// Config is the configuration.
type Config struct {
	*flag.FlagSet `json:"-"`

	LogLevel string `toml:"log-level" json:"log-level"`

	LogFile string `toml:"log-file" json:"log-file"`

	PprofAddr string `toml:"pprof-addr" json:"pprof-addr"`

	PoolSize int `toml:"pool-size" json:"pool-size"`

	Dir string `toml:"dir" json:"dir"`

	DB               DBConfig `toml:"db" json:"db"`
	CheckPointSchema string   `toml:"checkpoint-schema" json:"checkpoint-schema"`

	ConfigFile   string `json:"config-file"`
	printVersion bool

	RouteRules []*router.TableRule `toml:"route-rules" json:"route-rules"`

	DoTables         []*filter.Table `toml:"do-table" json:"do-table"`
	DoDBs            []string        `toml:"do-db" json:"do-db"`
	IgnoreTables     []*filter.Table `toml:"ignore-table" json:"ignore-table"`
	IgnoreDBs        []string        `toml:"ignore-db" json:"ignore-db"`
	RemoveCheckpoint bool            `toml:"rm-checkpoint" json:"rm-checkpoint"`
}

func (c *Config) String() string {
	bytes, err := json.Marshal(c)
	if err != nil {
		log.Errorf("[loader] marshal config to json error %v", err)
	}
	return string(bytes)
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

	c.adjust()

	return nil
}

func (c *Config) adjust() {
	routeRules := make([]*router.TableRule, 0, len(c.RouteRules))
	for _, rule := range c.RouteRules {
		rule.SchemaPattern = strings.ToLower(rule.SchemaPattern)
		rule.TablePattern = strings.ToLower(rule.TablePattern)
		routeRules = append(routeRules, rule)
	}
	c.RouteRules = routeRules
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	return errors.Trace(err)
}
