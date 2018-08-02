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

package syncer

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
	"github.com/siddontang/go-mysql/mysql"
)

// NewConfig creates a new config.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("syncer", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.BoolVar(&cfg.printVersion, "V", false, "prints version and exit")
	fs.StringVar(&cfg.ConfigFile, "config", "", "path to config file")
	fs.IntVar(&cfg.ServerID, "server-id", 101, "MySQL slave server ID")
	fs.StringVar(&cfg.Flavor, "flavor", mysql.MySQLFlavor, "use flavor for different MySQL source versions; support \"mysql\", \"mariadb\" now; if you replicate from mariadb, please set it to \"mariadb\"")
	fs.IntVar(&cfg.WorkerCount, "c", 16, "parallel worker count")
	fs.IntVar(&cfg.Batch, "b", 10, "batch commit count")
	fs.StringVar(&cfg.StatusAddr, "status-addr", "", "status addr")
	fs.StringVar(&cfg.Meta, "meta", "syncer.meta", "syncer meta info")
	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.LogFile, "log-file", "", "log file path")
	fs.StringVar(&cfg.LogRotate, "log-rotate", "day", "log file rotate type, hour/day")
	fs.BoolVar(&cfg.EnableGTID, "enable-gtid", false, "enable gtid mode")
	fs.BoolVar(&cfg.AutoFixGTID, "auto-fix-gtid", false, "auto fix gtid while switch mysql master/slave")
	fs.BoolVar(&cfg.SafeMode, "safe-mode", false, "enable safe mode to make syncer reentrant")
	fs.BoolVar(&cfg.DisableCausality, "disable-detect", false, "disbale detect causality")
	fs.IntVar(&cfg.MaxRetry, "max-retry", 100, "maxinum retry when network interruption")

	return cfg
}

// DBConfig is the DB configuration.
type DBConfig struct {
	Host     string `toml:"host" json:"host"`
	User     string `toml:"user" json:"user"`
	Password string `toml:"password" json:"-"` // omit it for privacy
	Port     int    `toml:"port" json:"port"`
}

// RouteRule is route rule that syncing
// schema/table to specified schema/table
type RouteRule struct {
	PatternSchema string `toml:"pattern-schema" json:"pattern-schema"`
	PatternTable  string `toml:"pattern-table" json:"pattern-table"`
	TargetSchema  string `toml:"target-schema" json:"target-schema"`
	TargertTable  string `toml:"target-table" json:"target-table"`
}

// SkipDML defines config rule of skipping dml.
type SkipDML struct {
	Schema string `toml:"db-name" json:"db-name"`
	Table  string `toml:"tbl-name" json:"tbl-name"`
	Type   string `toml:"type" json:"type"`
}

// Config is the configuration.
type Config struct {
	*flag.FlagSet `json:"-"`

	LogLevel  string `toml:"log-level" json:"log-level"`
	LogFile   string `toml:"log-file" json:"log-file"`
	LogRotate string `toml:"log-rotate" json:"log-rotate"`

	StatusAddr string `toml:"status-addr" json:"status-addr"`

	ServerID int    `toml:"server-id" json:"server-id"`
	Meta     string `toml:"meta" json:"meta"`
	Flavor   string `toml:"flavor" json:"flavor"`

	WorkerCount int `toml:"worker-count" json:"worker-count"`
	Batch       int `toml:"batch" json:"batch"`
	MaxRetry    int `toml:"max-retry" json:"max-retry"`

	// Ref: http://dev.mysql.com/doc/refman/5.7/en/replication-options-slave.html#option_mysqld_replicate-do-table
	DoTables []*filter.Table `toml:"replicate-do-table" json:"replicate-do-table"`
	DoDBs    []string        `toml:"replicate-do-db" json:"replicate-do-db"`

	// Ref: http://dev.mysql.com/doc/refman/5.7/en/replication-options-slave.html#option_mysqld_replicate-ignore-db
	IgnoreTables []*filter.Table `toml:"replicate-ignore-table" json:"replicate-ignore-table"`
	IgnoreDBs    []string        `toml:"replicate-ignore-db" json:"replicate-ignore-db"`

	// SkipDDLs is deprecated, please use SkipSQLs instead.
	SkipDDLs []string `toml:"skip-ddls" json:"-"` // omit it since it's deprecated
	SkipSQLs []string `toml:"skip-sqls" json:"skip-sqls"`
	// SkipEvents is deprecated, please use SkipDMLs instead.
	SkipEvents []string   `toml:"skip-events" json:"-"` // omit it since it's deprecated
	SkipDMLs   []*SkipDML `toml:"skip-dmls" json:"skip-dmls"`

	RouteRules []*RouteRule `toml:"route-rules" json:"route-rules"`

	From DBConfig `toml:"from" json:"from"`
	To   DBConfig `toml:"to" json:"to"`

	EnableGTID  bool `toml:"enable-gtid" json:"enable-gtid"`
	AutoFixGTID bool `toml:"auto-fix-gtid" json:"auto-fix-gtid"`

	DisableCausality bool   `toml:"disable-detect" json:"disable-detect"`
	SafeMode         bool   `toml:"safe-mode" json:"safe-mode"`
	ConfigFile       string `json:"config-file"`

	printVersion bool
}

func (c *Config) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		log.Errorf("[syncer] marshal config to json error %v", err)
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

	c.adjust()

	return nil
}

func (c *Config) adjust() {
	if c.Flavor != mysql.MySQLFlavor && c.Flavor != mysql.MariaDBFlavor {
		log.Fatalf("please specify right mysql version, support mysql, mariadb now")
	}

	for _, table := range c.DoTables {
		table.Name = strings.ToLower(table.Name)
		table.Schema = strings.ToLower(table.Schema)
	}
	for _, table := range c.IgnoreTables {
		table.Name = strings.ToLower(table.Name)
		table.Schema = strings.ToLower(table.Schema)
	}
	for i, db := range c.IgnoreDBs {
		c.IgnoreDBs[i] = strings.ToLower(db)
	}
	for i, db := range c.DoDBs {
		c.DoDBs[i] = strings.ToLower(db)
	}
	for _, rule := range c.RouteRules {
		rule.PatternSchema = strings.ToLower(rule.PatternSchema)
		rule.PatternTable = strings.ToLower(rule.PatternTable)
	}

	c.SkipDDLs = append(c.SkipDDLs, c.SkipSQLs...)
	// ignore empty rule
	skipDDLs := make([]string, 0, len(c.SkipDDLs))
	for _, skipDDL := range c.SkipDDLs {
		if strings.TrimSpace(skipDDL) == "" {
			continue
		}
		skipDDLs = append(skipDDLs, skipDDL)
	}
	c.SkipDDLs = skipDDLs

	skipEvents := make([]string, 0, len(c.SkipEvents))
	for _, skipEvent := range c.SkipEvents {
		if strings.TrimSpace(skipEvent) == "" {
			continue
		}
		if toDmlType(skipEvent) == dmlInvalid {
			log.Fatalf("invalid dml type %s in skip-events", skipEvent)
		}
		skipEvents = append(skipEvents, skipEvent)
	}
	c.SkipEvents = skipEvents

	for _, skipDML := range c.SkipDMLs {
		skipDML.Schema = strings.ToLower(strings.TrimSpace(skipDML.Schema))
		skipDML.Table = strings.ToLower(strings.TrimSpace(skipDML.Table))
		skipDML.Type = strings.ToLower(strings.TrimSpace(skipDML.Type))
		if skipDML.Schema == "" && skipDML.Table != "" {
			log.Fatalf("it's not allowed for schema empty and table not empty in skip-dmls. rule %+v", skipDML)
		}
	}

	if c.MaxRetry == 0 {
		c.MaxRetry = 1
	}
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	return errors.Trace(err)
}
