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
	"flag"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"

	"github.com/dustin/go-humanize"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/pingcap/tidb-tools/pkg/column-mapping"
	"github.com/pingcap/tidb-tools/pkg/filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"go.uber.org/zap"
	yaml "gopkg.in/yaml.v2"
)

// Online DDL Scheme
const (
	GHOST = "gh-ost"
	PT    = "pt"
)

// default config item values
var (
	// TaskConfig
	defaultMetaSchema      = "dm_meta"
	defaultEnableHeartbeat = false
	defaultIsSharding      = false
	defaultUpdateInterval  = 1
	defaultReportInterval  = 10
	// MydumperConfig
	defaultMydumperPath        = "./bin/mydumper"
	defaultThreads             = 4
	defaultChunkFilesize int64 = 64
	defaultSkipTzUTC           = true
	// LoaderConfig
	defaultPoolSize = 16
	defaultDir      = "./dumped_data"
	// SyncerConfig
	defaultWorkerCount = 16
	defaultBatch       = 100
)

// Meta represents binlog's meta pos
// NOTE: refine to put these config structs into pkgs
// NOTE: now, syncer does not support GTID mode and which is supported by relay
type Meta struct {
	BinLogName string `toml:"binlog-name" yaml:"binlog-name"`
	BinLogPos  uint32 `toml:"binlog-pos" yaml:"binlog-pos"`
}

// Verify does verification on configs
func (m *Meta) Verify() error {
	if m != nil && len(m.BinLogName) == 0 {
		return terror.ErrConfigMetaNoBinlogName.Generate()
	}

	return nil
}

// MySQLInstance represents a sync config of a MySQL instance
type MySQLInstance struct {
	// it represents a MySQL/MariaDB instance or a replica group
	SourceID           string   `yaml:"source-id"`
	Meta               *Meta    `yaml:"meta"`
	FilterRules        []string `yaml:"filter-rules"`
	ColumnMappingRules []string `yaml:"column-mapping-rules"`
	RouteRules         []string `yaml:"route-rules"`
	BWListName         string   `yaml:"black-white-list"`

	MydumperConfigName string          `yaml:"mydumper-config-name"`
	Mydumper           *MydumperConfig `yaml:"mydumper"`
	// MydumperThread is alias for Threads in MydumperConfig, and its priority is higher than Threads
	MydumperThread int `yaml:"mydumper-thread"`

	LoaderConfigName string        `yaml:"loader-config-name"`
	Loader           *LoaderConfig `yaml:"loader"`
	// LoaderThread is alias for PoolSize in LoaderConfig, and its priority is higher than PoolSize
	LoaderThread int `yaml:"loader-thread"`

	SyncerConfigName string        `yaml:"syncer-config-name"`
	Syncer           *SyncerConfig `yaml:"syncer"`
	// SyncerThread is alias for WorkerCount in SyncerConfig, and its priority is higher than WorkerCount
	SyncerThread int `yaml:"syncer-thread"`
}

// Verify does verification on configs
func (m *MySQLInstance) Verify() error {
	if m == nil {
		return terror.ErrConfigMySQLInstNotFound.Generate()
	}

	if m.SourceID == "" {
		return terror.ErrConfigEmptySourceID.Generate()
	}

	if err := m.Meta.Verify(); err != nil {
		return terror.Annotatef(err, "source %s", m.SourceID)
	}

	if len(m.MydumperConfigName) > 0 && m.Mydumper != nil {
		return terror.ErrConfigMydumperCfgConflict.Generate()
	}
	if len(m.LoaderConfigName) > 0 && m.Loader != nil {
		return terror.ErrConfigLoaderCfgConflict.Generate()
	}
	if len(m.SyncerConfigName) > 0 && m.Syncer != nil {
		return terror.ErrConfigSyncerCfgConflict.Generate()
	}

	return nil
}

// MydumperConfig represents mydumper process unit's specific config
type MydumperConfig struct {
	MydumperPath  string `yaml:"mydumper-path" toml:"mydumper-path" json:"mydumper-path"`    // mydumper binary path
	Threads       int    `yaml:"threads" toml:"threads" json:"threads"`                      // -t, --threads
	ChunkFilesize int64  `yaml:"chunk-filesize" toml:"chunk-filesize" json:"chunk-filesize"` // -F, --chunk-filesize
	SkipTzUTC     bool   `yaml:"skip-tz-utc" toml:"skip-tz-utc" json:"skip-tz-utc"`          // --skip-tz-utc
	ExtraArgs     string `yaml:"extra-args" toml:"extra-args" json:"extra-args"`             // other extra args
	// NOTE: use LoaderConfig.Dir as --outputdir
	// TODO zxc: combine -B -T --regex with filter rules?
}

func defaultMydumperConfig() MydumperConfig {
	return MydumperConfig{
		MydumperPath:  defaultMydumperPath,
		Threads:       defaultThreads,
		ChunkFilesize: defaultChunkFilesize,
		SkipTzUTC:     defaultSkipTzUTC,
	}
}

// alias to avoid infinite recursion for UnmarshalYAML
type rawMydumperConfig MydumperConfig

// UnmarshalYAML implements Unmarshaler.UnmarshalYAML
func (m *MydumperConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	raw := rawMydumperConfig(defaultMydumperConfig())
	if err := unmarshal(&raw); err != nil {
		return terror.ErrConfigTaskYamlTransform.Delegate(err, "unmarshal mydumper config")
	}
	*m = MydumperConfig(raw) // raw used only internal, so no deep copy
	return nil
}

// LoaderConfig represents loader process unit's specific config
type LoaderConfig struct {
	PoolSize int    `yaml:"pool-size" toml:"pool-size" json:"pool-size"`
	Dir      string `yaml:"dir" toml:"dir" json:"dir"`
}

func defaultLoaderConfig() LoaderConfig {
	return LoaderConfig{
		PoolSize: defaultPoolSize,
		Dir:      defaultDir,
	}
}

// alias to avoid infinite recursion for UnmarshalYAML
type rawLoaderConfig LoaderConfig

// UnmarshalYAML implements Unmarshaler.UnmarshalYAML
func (m *LoaderConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	raw := rawLoaderConfig(defaultLoaderConfig())
	if err := unmarshal(&raw); err != nil {
		return terror.ErrConfigTaskYamlTransform.Delegate(err, "unmarshal loader config")
	}
	*m = LoaderConfig(raw) // raw used only internal, so no deep copy
	return nil
}

// SyncerConfig represents syncer process unit's specific config
type SyncerConfig struct {
	MetaFile    string `yaml:"meta-file" toml:"meta-file" json:"meta-file"` // meta filename, used only when load SubConfig directly
	WorkerCount int    `yaml:"worker-count" toml:"worker-count" json:"worker-count"`
	Batch       int    `yaml:"batch" toml:"batch" json:"batch"`
	// deprecated
	MaxRetry int `yaml:"max-retry" toml:"max-retry" json:"max-retry"`

	// refine following configs to top level configs?
	AutoFixGTID      bool `yaml:"auto-fix-gtid" toml:"auto-fix-gtid" json:"auto-fix-gtid"`
	EnableGTID       bool `yaml:"enable-gtid" toml:"enable-gtid" json:"enable-gtid"`
	DisableCausality bool `yaml:"disable-detect" toml:"disable-detect" json:"disable-detect"`
	SafeMode         bool `yaml:"safe-mode" toml:"safe-mode" json:"safe-mode"`
	EnableANSIQuotes bool `yaml:"enable-ansi-quotes" toml:"enable-ansi-quotes" json:"enable-ansi-quotes"`
}

func defaultSyncerConfig() SyncerConfig {
	return SyncerConfig{
		WorkerCount: defaultWorkerCount,
		Batch:       defaultBatch,
	}
}

// alias to avoid infinite recursion for UnmarshalYAML
type rawSyncerConfig SyncerConfig

// UnmarshalYAML implements Unmarshaler.UnmarshalYAML
func (m *SyncerConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	raw := rawSyncerConfig(defaultSyncerConfig())
	if err := unmarshal(&raw); err != nil {
		return terror.ErrConfigTaskYamlTransform.Delegate(err, "unmarshal syncer config")
	}
	*m = SyncerConfig(raw) // raw used only internal, so no deep copy
	return nil
}

// TaskConfig is the configuration for Task
type TaskConfig struct {
	*flag.FlagSet `yaml:"-"`

	Name       string `yaml:"name"`
	TaskMode   string `yaml:"task-mode"`
	IsSharding bool   `yaml:"is-sharding"`
	//  treat it as hidden configuration
	IgnoreCheckingItems []string `yaml:"ignore-checking-items"`
	// we store detail status in meta
	// don't save configuration into it
	MetaSchema string `yaml:"meta-schema"`
	// remove meta from downstreaming database
	// now we delete checkpoint and online ddl information
	RemoveMeta              bool   `yaml:"remove-meta"`
	EnableHeartbeat         bool   `yaml:"enable-heartbeat"`
	HeartbeatUpdateInterval int    `yaml:"heartbeat-update-interval"`
	HeartbeatReportInterval int    `yaml:"heartbeat-report-interval"`
	Timezone                string `yaml:"timezone"`

	// handle schema/table name mode, and only for schema/table name
	// if case insensitive, we would convert schema/table name to lower case
	CaseSensitive bool `yaml:"case-sensitive"`

	TargetDB *DBConfig `yaml:"target-database"`

	MySQLInstances []*MySQLInstance `yaml:"mysql-instances"`

	OnlineDDLScheme string `yaml:"online-ddl-scheme"`

	Routes         map[string]*router.TableRule   `yaml:"routes"`
	Filters        map[string]*bf.BinlogEventRule `yaml:"filters"`
	ColumnMappings map[string]*column.Rule        `yaml:"column-mappings"`
	BWList         map[string]*filter.Rules       `yaml:"black-white-list"`

	Mydumpers map[string]*MydumperConfig `yaml:"mydumpers"`
	Loaders   map[string]*LoaderConfig   `yaml:"loaders"`
	Syncers   map[string]*SyncerConfig   `yaml:"syncers"`
}

// NewTaskConfig creates a TaskConfig
func NewTaskConfig() *TaskConfig {
	cfg := &TaskConfig{
		// explicitly set default value
		MetaSchema:              defaultMetaSchema,
		EnableHeartbeat:         defaultEnableHeartbeat,
		HeartbeatUpdateInterval: defaultUpdateInterval,
		HeartbeatReportInterval: defaultReportInterval,
		MySQLInstances:          make([]*MySQLInstance, 0, 5),
		IsSharding:              defaultIsSharding,
		Routes:                  make(map[string]*router.TableRule),
		Filters:                 make(map[string]*bf.BinlogEventRule),
		ColumnMappings:          make(map[string]*column.Rule),
		BWList:                  make(map[string]*filter.Rules),
		Mydumpers:               make(map[string]*MydumperConfig),
		Loaders:                 make(map[string]*LoaderConfig),
		Syncers:                 make(map[string]*SyncerConfig),
	}
	cfg.FlagSet = flag.NewFlagSet("task", flag.ContinueOnError)
	return cfg
}

// String returns the config's yaml string
func (c *TaskConfig) String() string {
	cfg, err := yaml.Marshal(c)
	if err != nil {
		log.L().Error("marshal task config to yaml", zap.String("task", c.Name), log.ShortError(err))
	}
	return string(cfg)
}

// DecodeFile loads and decodes config from file
func (c *TaskConfig) DecodeFile(fpath string) error {
	bs, err := ioutil.ReadFile(fpath)
	if err != nil {
		return terror.ErrConfigReadTaskCfgFromFile.Delegate(err, fpath)
	}

	err = yaml.UnmarshalStrict(bs, c)
	if err != nil {
		return terror.ErrConfigTaskYamlTransform.Delegate(err)
	}

	return c.adjust()
}

// Decode loads config from file data
func (c *TaskConfig) Decode(data string) error {
	err := yaml.UnmarshalStrict([]byte(data), c)
	if err != nil {
		return terror.ErrConfigTaskYamlTransform.Delegate(err, "decode task config failed")
	}

	return c.adjust()
}

// adjust adjusts configs
func (c *TaskConfig) adjust() error {
	if len(c.Name) == 0 {
		return terror.ErrConfigNeedUniqueTaskName.Generate()
	}
	if c.TaskMode != ModeFull && c.TaskMode != ModeIncrement && c.TaskMode != ModeAll {
		return terror.ErrConfigInvalidTaskMode.Generate()
	}

	for _, item := range c.IgnoreCheckingItems {
		if err := ValidateCheckingItem(item); err != nil {
			return err
		}
	}

	if c.OnlineDDLScheme != "" && c.OnlineDDLScheme != PT && c.OnlineDDLScheme != GHOST {
		return terror.ErrConfigOnlineSchemeNotSupport.Generate(c.OnlineDDLScheme)
	}

	if c.TargetDB == nil {
		return terror.ErrConfigNeedTargetDB.Generate()
	}

	if len(c.MySQLInstances) == 0 {
		return terror.ErrConfigMySQLInstsAtLeastOne.Generate()
	}

	iids := make(map[string]int) // source-id -> instance-index
	duplicateErrorStrings := make([]string, 0)
	for i, inst := range c.MySQLInstances {
		if err := inst.Verify(); err != nil {
			return terror.Annotatef(err, "mysql-instance: %s", humanize.Ordinal(i))
		}
		if iid, ok := iids[inst.SourceID]; ok {
			return terror.ErrConfigMySQLInstSameSourceID.Generate(iid, i, inst.SourceID)
		}
		iids[inst.SourceID] = i

		switch c.TaskMode {
		case ModeFull, ModeAll:
			if inst.Meta != nil {
				log.L().Warn("metadata will not be used. for Full mode, incremental sync will never occur; for All mode, the meta dumped by MyDumper will be used", zap.Int("mysql instance", i), zap.String("task mode", c.TaskMode))
			}
		case ModeIncrement:
			if inst.Meta == nil {
				return terror.ErrConfigMetadataNotSet.Generate(i, c.TaskMode)
			}
			err := inst.Meta.Verify()
			if err != nil {
				return terror.Annotatef(err, "mysql-instance: %d", i)
			}
		}

		for _, name := range inst.RouteRules {
			if _, ok := c.Routes[name]; !ok {
				return terror.ErrConfigRouteRuleNotFound.Generate(i, name)
			}
		}
		for _, name := range inst.FilterRules {
			if _, ok := c.Filters[name]; !ok {
				return terror.ErrConfigFilterRuleNotFound.Generate(i, name)
			}
		}
		for _, name := range inst.ColumnMappingRules {
			if _, ok := c.ColumnMappings[name]; !ok {
				return terror.ErrConfigColumnMappingNotFound.Generate(i, name)
			}
		}
		if _, ok := c.BWList[inst.BWListName]; len(inst.BWListName) > 0 && !ok {
			return terror.ErrConfigBWListNotFound.Generate(i, inst.BWListName)
		}

		if len(inst.MydumperConfigName) > 0 {
			rule, ok := c.Mydumpers[inst.MydumperConfigName]
			if !ok {
				return terror.ErrConfigMydumperCfgNotFound.Generate(i, inst.MydumperConfigName)
			}
			inst.Mydumper = new(MydumperConfig)
			*inst.Mydumper = *rule // ref mydumper config
		}
		if inst.Mydumper == nil {
			defaultCfg := defaultMydumperConfig()
			inst.Mydumper = &defaultCfg
		}
		if inst.MydumperThread != 0 {
			inst.Mydumper.Threads = inst.MydumperThread
		}

		if (c.TaskMode == ModeFull || c.TaskMode == ModeAll) && len(inst.Mydumper.MydumperPath) == 0 {
			// only verify if set, whether is valid can only be verify when we run it
			return terror.ErrConfigMydumperPathNotValid.Generate(i)
		}

		if len(inst.LoaderConfigName) > 0 {
			rule, ok := c.Loaders[inst.LoaderConfigName]
			if !ok {
				return terror.ErrConfigLoaderCfgNotFound.Generate(i, inst.LoaderConfigName)
			}
			inst.Loader = new(LoaderConfig)
			*inst.Loader = *rule // ref loader config
		}
		if inst.Loader == nil {
			defaultCfg := defaultLoaderConfig()
			inst.Loader = &defaultCfg
		}
		if inst.LoaderThread != 0 {
			inst.Loader.PoolSize = inst.LoaderThread
		}

		if len(inst.SyncerConfigName) > 0 {
			rule, ok := c.Syncers[inst.SyncerConfigName]
			if !ok {
				return terror.ErrConfigSyncerCfgNotFound.Generate(i, inst.SyncerConfigName)
			}
			inst.Syncer = new(SyncerConfig)
			*inst.Syncer = *rule // ref syncer config
		}
		if inst.Syncer == nil {
			defaultCfg := defaultSyncerConfig()
			inst.Syncer = &defaultCfg
		}
		if inst.SyncerThread != 0 {
			inst.Syncer.WorkerCount = inst.SyncerThread
		}

		if dupeRules := checkDuplicateString(inst.RouteRules); len(dupeRules) > 0 {
			duplicateErrorStrings = append(duplicateErrorStrings, fmt.Sprintf("mysql-instance(%d)'s route-rules: %s", i, strings.Join(dupeRules, ", ")))
		}
		if dupeRules := checkDuplicateString(inst.FilterRules); len(dupeRules) > 0 {
			duplicateErrorStrings = append(duplicateErrorStrings, fmt.Sprintf("mysql-instance(%d)'s filter-rules: %s", i, strings.Join(dupeRules, ", ")))
		}
	}
	if len(duplicateErrorStrings) > 0 {
		return terror.ErrConfigDuplicateCfgItem.Generate(strings.Join(duplicateErrorStrings, "\n"))
	}

	if c.Timezone != "" {
		_, err := time.LoadLocation(c.Timezone)
		if err != nil {
			return terror.ErrConfigInvalidTimezone.Delegate(err, c.Timezone)
		}
	}

	return nil
}

// SubTaskConfigs generates sub task configs
func (c *TaskConfig) SubTaskConfigs(sources map[string]DBConfig) ([]*SubTaskConfig, error) {
	cfgs := make([]*SubTaskConfig, len(c.MySQLInstances))
	for i, inst := range c.MySQLInstances {
		dbCfg, exist := sources[inst.SourceID]
		if !exist {
			return nil, terror.ErrConfigSourceIDNotFound.Generate(inst.SourceID)
		}

		cfg := NewSubTaskConfig()
		cfg.IsSharding = c.IsSharding
		cfg.OnlineDDLScheme = c.OnlineDDLScheme
		cfg.IgnoreCheckingItems = c.IgnoreCheckingItems
		cfg.Name = c.Name
		cfg.Mode = c.TaskMode
		cfg.CaseSensitive = c.CaseSensitive
		cfg.MetaSchema = c.MetaSchema
		cfg.RemoveMeta = c.RemoveMeta
		cfg.EnableHeartbeat = c.EnableHeartbeat
		cfg.HeartbeatUpdateInterval = c.HeartbeatUpdateInterval
		cfg.HeartbeatReportInterval = c.HeartbeatReportInterval
		cfg.Timezone = c.Timezone
		cfg.Meta = inst.Meta

		cfg.From = dbCfg
		cfg.To = *c.TargetDB

		cfg.SourceID = inst.SourceID

		cfg.RouteRules = make([]*router.TableRule, len(inst.RouteRules))
		for j, name := range inst.RouteRules {
			cfg.RouteRules[j] = c.Routes[name]
		}

		cfg.FilterRules = make([]*bf.BinlogEventRule, len(inst.FilterRules))
		for j, name := range inst.FilterRules {
			cfg.FilterRules[j] = c.Filters[name]
		}

		cfg.ColumnMappingRules = make([]*column.Rule, len(inst.ColumnMappingRules))
		for j, name := range inst.ColumnMappingRules {
			cfg.ColumnMappingRules[j] = c.ColumnMappings[name]
		}

		cfg.BWList = c.BWList[inst.BWListName]

		cfg.MydumperConfig = *inst.Mydumper
		cfg.LoaderConfig = *inst.Loader
		cfg.SyncerConfig = *inst.Syncer

		err := cfg.Adjust(true)
		if err != nil {
			return nil, terror.Annotatef(err, "source %s", inst.SourceID)
		}

		cfgs[i] = cfg
	}
	return cfgs, nil
}

// checkDuplicateString checks whether the given string array has duplicate string item
// if there is duplicate, it will return **all** the duplicate strings
func checkDuplicateString(ruleNames []string) []string {
	mp := make(map[string]bool, len(ruleNames))
	dupeArray := make([]string, 0)
	for _, name := range ruleNames {
		if added, ok := mp[name]; ok {
			if !added {
				dupeArray = append(dupeArray, name)
				mp[name] = true
			}
		} else {
			mp[name] = false
		}
	}
	return dupeArray
}
