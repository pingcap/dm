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

package config

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"

	"github.com/BurntSushi/toml"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	column "github.com/pingcap/tidb-tools/pkg/column-mapping"
	"github.com/pingcap/tidb-tools/pkg/filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"go.uber.org/zap"
)

// task modes
const (
	ModeAll       = "all"
	ModeFull      = "full"
	ModeIncrement = "incremental"
)

var (
	defaultMaxAllowedPacket = 64 * 1024 * 1024 // 64MiB, equal to TiDB's default
	defaultMaxIdleConns     = 2
)

// RawDBConfig contains some low level database config
type RawDBConfig struct {
	MaxIdleConns int
	ReadTimeout  string
	WriteTimeout string
}

// DefaultRawDBConfig returns a default raw database config
func DefaultRawDBConfig() *RawDBConfig {
	return &RawDBConfig{
		MaxIdleConns: defaultMaxIdleConns,
	}
}

// SetReadTimeout set readTimeout for raw database config
func (c *RawDBConfig) SetReadTimeout(readTimeout string) *RawDBConfig {
	c.ReadTimeout = readTimeout
	return c
}

// SetWriteTimeout set writeTimeout for raw database config
func (c *RawDBConfig) SetWriteTimeout(writeTimeout string) *RawDBConfig {
	c.WriteTimeout = writeTimeout
	return c
}

// SetMaxIdleConns set maxIdleConns for raw database config
// set value <= 0 then no idle connections are retained.
// set value > 0 then `value` idle connections are retained.
func (c *RawDBConfig) SetMaxIdleConns(value int) *RawDBConfig {
	c.MaxIdleConns = value
	return c
}

// DBConfig is the DB configuration.
type DBConfig struct {
	Host             string `toml:"host" json:"host" yaml:"host"`
	Port             int    `toml:"port" json:"port" yaml:"port"`
	User             string `toml:"user" json:"user" yaml:"user"`
	Password         string `toml:"password" json:"-" yaml:"password"` // omit it for privacy
	MaxAllowedPacket *int   `toml:"max-allowed-packet" json:"max-allowed-packet" yaml:"max-allowed-packet"`

	RawDBCfg *RawDBConfig `toml:"-" json:"-" yaml:"-"`
}

func (db *DBConfig) String() string {
	cfg, err := json.Marshal(db)
	if err != nil {
		log.L().Error("fail to marshal config to json", log.ShortError(err))
	}
	return string(cfg)
}

// Toml returns TOML format representation of config
func (db *DBConfig) Toml() (string, error) {
	var b bytes.Buffer
	enc := toml.NewEncoder(&b)
	err := enc.Encode(db)
	if err != nil {
		return "", terror.ErrConfigTomlTransform.Delegate(err, "encode db config to toml")
	}
	return b.String(), nil
}

// Decode loads config from file data
func (db *DBConfig) Decode(data string) error {
	_, err := toml.Decode(data, db)
	return terror.ErrConfigTomlTransform.Delegate(err, "decode db config")
}

// Adjust adjusts the config.
func (db *DBConfig) Adjust() {
	if db.MaxAllowedPacket == nil {
		cloneV := defaultMaxAllowedPacket
		db.MaxAllowedPacket = &cloneV
	}
}

// SubTaskConfig is the configuration for SubTask
type SubTaskConfig struct {
	// BurntSushi/toml seems have a bug for flag "-"
	// when doing encoding, if we use `toml:"-"`, it still try to encode it
	// and it will panic because of unsupported type (reflect.Func)
	// so we should not export flagSet
	flagSet *flag.FlagSet

	// when in sharding, multi dm-workers do one task
	IsSharding      bool   `toml:"is-sharding" json:"is-sharding"`
	OnlineDDLScheme string `toml:"online-ddl-scheme" json:"online-ddl-scheme"`

	// handle schema/table name mode, and only for schema/table name/pattern
	// if case insensitive, we would convert schema/table name/pattern to lower case
	CaseSensitive bool `toml:"case-sensitive" json:"case-sensitive"`

	Name string `toml:"name" json:"name"`
	Mode string `toml:"mode" json:"mode"`
	//  treat it as hidden configuration
	IgnoreCheckingItems []string `toml:"ignore-checking-items" json:"ignore-checking-items"`
	// it represents a MySQL/MariaDB instance or a replica group
	SourceID                string `toml:"source-id" json:"source-id"`
	ServerID                uint32 `toml:"server-id" json:"server-id"`
	Flavor                  string `toml:"flavor" json:"flavor"`
	MetaSchema              string `toml:"meta-schema" json:"meta-schema"`
	RemoveMeta              bool   `toml:"remove-meta" json:"remove-meta"`
	HeartbeatUpdateInterval int    `toml:"heartbeat-update-interval" json:"heartbeat-update-interval"`
	HeartbeatReportInterval int    `toml:"heartbeat-report-interval" json:"heartbeat-report-interval"`
	EnableHeartbeat         bool   `toml:"enable-heartbeat" json:"enable-heartbeat"`
	Meta                    *Meta  `toml:"meta" json:"meta"`
	Timezone                string `toml:"timezone" josn:"timezone"`

	// RelayDir get value from dm-worker config
	RelayDir string `toml:"relay-dir" json:"relay-dir"`

	// UseRelay get value from dm-worker config
	UseRelay bool     `toml:"use-relay" json:"use-relay"`
	From     DBConfig `toml:"from" json:"from"`
	To       DBConfig `toml:"to" json:"to"`

	RouteRules         []*router.TableRule   `toml:"route-rules" json:"route-rules"`
	FilterRules        []*bf.BinlogEventRule `toml:"filter-rules" json:"filter-rules"`
	ColumnMappingRules []*column.Rule        `toml:"mapping-rule" json:"mapping-rule"`
	BWList             *filter.Rules         `toml:"black-white-list" json:"black-white-list"`

	MydumperConfig // Mydumper configuration
	LoaderConfig   // Loader configuration
	SyncerConfig   // Syncer configuration

	// compatible with standalone dm unit
	LogLevel  string `toml:"log-level" json:"log-level"`
	LogFile   string `toml:"log-file" json:"log-file"`
	LogRotate string `toml:"log-rotate" json:"log-rotate"`

	PprofAddr  string `toml:"pprof-addr" json:"pprof-addr"`
	StatusAddr string `toml:"status-addr" json:"status-addr"`

	ConfigFile string `toml:"-" json:"config-file"`

	// still needed by Syncer / Loader bin
	printVersion bool
}

// NewSubTaskConfig creates a new SubTaskConfig
func NewSubTaskConfig() *SubTaskConfig {
	cfg := &SubTaskConfig{}
	return cfg
}

// GetFlagSet provides the pointer of subtask's flag set.
func (c *SubTaskConfig) GetFlagSet() *flag.FlagSet {
	return c.flagSet
}

// SetFlagSet writes back the flag set.
func (c *SubTaskConfig) SetFlagSet(flagSet *flag.FlagSet) {
	c.flagSet = flagSet
}

// String returns the config's json string
func (c *SubTaskConfig) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		log.L().Error("marshal subtask config to json", zap.String("task", c.Name), log.ShortError(err))
	}
	return string(cfg)
}

// Toml returns TOML format representation of config
func (c *SubTaskConfig) Toml() (string, error) {
	var b bytes.Buffer
	enc := toml.NewEncoder(&b)
	err := enc.Encode(c)
	if err != nil {
		return "", terror.ErrConfigTomlTransform.Delegate(err, "encode subtask config")
	}
	return b.String(), nil
}

// DecodeFile loads and decodes config from file
func (c *SubTaskConfig) DecodeFile(fpath string, verifyDecryptPassword bool) error {
	_, err := toml.DecodeFile(fpath, c)
	if err != nil {
		return terror.ErrConfigTomlTransform.Delegate(err, "decode subtask config from file")
	}

	return c.Adjust(verifyDecryptPassword)
}

// Decode loads config from file data
func (c *SubTaskConfig) Decode(data string, verifyDecryptPassword bool) error {
	_, err := toml.Decode(data, c)
	if err != nil {
		return terror.ErrConfigTomlTransform.Delegate(err, "decode subtask config from data")
	}

	return c.Adjust(verifyDecryptPassword)
}

// Adjust adjusts configs
func (c *SubTaskConfig) Adjust(verifyDecryptPassword bool) error {
	if c.Name == "" {
		return terror.ErrConfigTaskNameEmpty.Generate()
	}

	if c.SourceID == "" {
		return terror.ErrConfigEmptySourceID.Generate()
	}
	if len(c.SourceID) > MaxSourceIDLength {
		return terror.ErrConfigTooLongSourceID.Generate()
	}

	//if c.Flavor != mysql.MySQLFlavor && c.Flavor != mysql.MariaDBFlavor {
	//	return errors.Errorf("please specify right mysql version, support mysql, mariadb now")
	//}

	if c.OnlineDDLScheme != "" && c.OnlineDDLScheme != PT && c.OnlineDDLScheme != GHOST {
		return terror.ErrConfigOnlineSchemeNotSupport.Generate(c.OnlineDDLScheme)
	}

	if c.MetaSchema == "" {
		c.MetaSchema = defaultMetaSchema
	}

	if c.Timezone != "" {
		_, err := time.LoadLocation(c.Timezone)
		if err != nil {
			return terror.ErrConfigInvalidTimezone.Delegate(err, c.Timezone)
		}
	}

	dirSuffix := "." + c.Name
	if !strings.HasSuffix(c.LoaderConfig.Dir, dirSuffix) { // check to support multiple times calling
		// if not ends with the task name, we append the task name to the tail
		c.LoaderConfig.Dir += dirSuffix
	}

	c.From.Adjust()
	c.To.Adjust()

	if verifyDecryptPassword {
		_, err1 := c.DecryptPassword()
		if err1 != nil {
			return err1
		}
	}

	return nil
}

// Parse parses flag definitions from the argument list.
func (c *SubTaskConfig) Parse(arguments []string, verifyDecryptPassword bool) error {
	// Parse first to get config file.
	err := c.flagSet.Parse(arguments)
	if err != nil {
		return terror.ErrConfigParseFlagSet.Delegate(err)
	}

	if c.printVersion {
		fmt.Println(utils.GetRawInfo())
		return flag.ErrHelp
	}

	// Load config file if specified.
	if c.ConfigFile != "" {
		err = c.DecodeFile(c.ConfigFile, verifyDecryptPassword)
		if err != nil {
			return err
		}
	}

	// Parse again to replace with command line options.
	err = c.flagSet.Parse(arguments)
	if err != nil {
		return terror.ErrConfigParseFlagSet.Delegate(err)
	}

	if len(c.flagSet.Args()) != 0 {
		return terror.ErrConfigParseFlagSet.Generatef("'%s' is an invalid flag", c.flagSet.Arg(0))
	}

	return c.Adjust(verifyDecryptPassword)
}

// DecryptPassword tries to decrypt db password in config
func (c *SubTaskConfig) DecryptPassword() (*SubTaskConfig, error) {
	clone, err := c.Clone()
	if err != nil {
		return nil, err
	}

	var (
		pswdTo   string
		pswdFrom string
	)
	if len(clone.To.Password) > 0 {
		pswdTo, err = utils.Decrypt(clone.To.Password)
		if err != nil {
			return nil, terror.WithScope(terror.ErrConfigDecryptDBPassword.Delegate(err, clone.To.Password), terror.ScopeDownstream)
		}
	}
	if len(clone.From.Password) > 0 {
		pswdFrom, err = utils.Decrypt(clone.From.Password)
		if err != nil {
			return nil, terror.WithScope(terror.ErrConfigDecryptDBPassword.Delegate(err, clone.From.Password), terror.ScopeUpstream)
		}
	}
	clone.From.Password = pswdFrom
	clone.To.Password = pswdTo

	return clone, nil
}

// Clone returns a replica of SubTaskConfig
func (c *SubTaskConfig) Clone() (*SubTaskConfig, error) {
	content, err := c.Toml()
	if err != nil {
		return nil, err
	}

	clone := &SubTaskConfig{}
	_, err = toml.Decode(content, clone)
	if err != nil {
		return nil, terror.ErrConfigTomlTransform.Delegate(err, "decode subtask config from data")
	}

	return clone, nil
}
