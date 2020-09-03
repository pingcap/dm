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
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/pingcap/dm/pkg/binlog"
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

// shard DDL mode.
const (
	ShardPessimistic = "pessimistic"
	ShardOptimistic  = "optimistic"
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
	defaultMydumperPath  = "./bin/mydumper"
	defaultThreads       = 4
	defaultChunkFilesize = "64"
	defaultSkipTzUTC     = true
	// LoaderConfig
	defaultPoolSize = 16
	defaultDir      = "./dumped_data"
	// SyncerConfig
	defaultWorkerCount             = 16
	defaultBatch                   = 100
	defaultQueueSize               = 1024 // do not give too large default value to avoid OOM
	defaultCheckpointFlushInterval = 30   // in seconds
)

// Meta represents binlog's meta pos
// NOTE: refine to put these config structs into pkgs
// NOTE: now, syncer does not support GTID mode and which is supported by relay
type Meta struct {
	BinLogName string `toml:"binlog-name" yaml:"binlog-name"`
	BinLogPos  uint32 `toml:"binlog-pos" yaml:"binlog-pos"`
	BinLogGTID string `toml:"binlog-gtid" yaml:"binlog-gtid"`
}

// Verify does verification on configs
// NOTE: we can't decide to verify `binlog-name` or `binlog-gtid` until bound to a source (with `enable-gtid` set).
func (m *Meta) Verify() error {
	if m != nil && len(m.BinLogName) == 0 && len(m.BinLogGTID) == 0 {
		return terror.ErrConfigMetaInvalid.Generate()
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

	// black-white-list is deprecated, use block-allow-list instead
	BWListName string `yaml:"black-white-list"`
	BAListName string `yaml:"block-allow-list"`

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

// VerifyAndAdjust does verification on configs, and adjust some configs
func (m *MySQLInstance) VerifyAndAdjust() error {
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

	if len(m.BAListName) == 0 && len(m.BWListName) != 0 {
		m.BAListName = m.BWListName
	}

	return nil
}

// RemoveDuplicateCfg remove duplicate mydumper, loader, and syncer config
func (m *MySQLInstance) RemoveDuplicateCfg() {
	if len(m.MydumperConfigName) > 0 && m.Mydumper != nil {
		m.Mydumper = nil
	}
	if len(m.LoaderConfigName) > 0 && m.Loader != nil {
		m.Loader = nil
	}
	if len(m.SyncerConfigName) > 0 && m.Syncer != nil {
		m.Syncer = nil
	}
}

// MydumperConfig represents mydumper process unit's specific config
type MydumperConfig struct {
	MydumperPath  string `yaml:"mydumper-path" toml:"mydumper-path" json:"mydumper-path"`    // mydumper binary path
	Threads       int    `yaml:"threads" toml:"threads" json:"threads"`                      // -t, --threads
	ChunkFilesize string `yaml:"chunk-filesize" toml:"chunk-filesize" json:"chunk-filesize"` // -F, --chunk-filesize
	StatementSize uint64 `yaml:"statement-size" toml:"statement-size" json:"statement-size"` // -S, --statement-size
	Rows          uint64 `yaml:"rows" toml:"rows" json:"rows"`                               // -r, --rows
	Where         string `yaml:"where" toml:"where" json:"where"`                            // --where

	SkipTzUTC bool   `yaml:"skip-tz-utc" toml:"skip-tz-utc" json:"skip-tz-utc"` // --skip-tz-utc
	ExtraArgs string `yaml:"extra-args" toml:"extra-args" json:"extra-args"`    // other extra args
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
		return terror.ErrConfigYamlTransform.Delegate(err, "unmarshal mydumper config")
	}
	*m = MydumperConfig(raw) // raw used only internal, so no deep copy
	return nil
}

// LoaderConfig represents loader process unit's specific config
type LoaderConfig struct {
	PoolSize int    `yaml:"pool-size" toml:"pool-size" json:"pool-size"`
	Dir      string `yaml:"dir" toml:"dir" json:"dir"`
	SQLMode  string `yaml:"-" toml:"-" json:"-"` // wrote by dump unit
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
		return terror.ErrConfigYamlTransform.Delegate(err, "unmarshal loader config")
	}
	*m = LoaderConfig(raw) // raw used only internal, so no deep copy
	return nil
}

// SyncerConfig represents syncer process unit's specific config
type SyncerConfig struct {
	MetaFile    string `yaml:"meta-file" toml:"meta-file" json:"meta-file"` // meta filename, used only when load SubConfig directly
	WorkerCount int    `yaml:"worker-count" toml:"worker-count" json:"worker-count"`
	Batch       int    `yaml:"batch" toml:"batch" json:"batch"`
	QueueSize   int    `yaml:"queue-size" toml:"queue-size" json:"queue-size"`
	// checkpoint flush interval in seconds.
	CheckpointFlushInterval int `yaml:"checkpoint-flush-interval" toml:"checkpoint-flush-interval" json:"checkpoint-flush-interval"`

	// deprecated
	MaxRetry int `yaml:"max-retry" toml:"max-retry" json:"max-retry"`

	// refine following configs to top level configs?
	AutoFixGTID      bool `yaml:"auto-fix-gtid" toml:"auto-fix-gtid" json:"auto-fix-gtid"`
	EnableGTID       bool `yaml:"enable-gtid" toml:"enable-gtid" json:"enable-gtid"`
	DisableCausality bool `yaml:"disable-detect" toml:"disable-detect" json:"disable-detect"`
	SafeMode         bool `yaml:"safe-mode" toml:"safe-mode" json:"safe-mode"`
	// when dump unit can't run consistent dump, enable safe mode until pass exit location of dumping
	SafeModeExitLoc *binlog.Location `yaml:"-" toml:"-" json:"-"`
	// deprecated, use `ansi-quotes` in top level config instead
	EnableANSIQuotes bool `yaml:"enable-ansi-quotes" toml:"enable-ansi-quotes" json:"enable-ansi-quotes"`
}

func defaultSyncerConfig() SyncerConfig {
	return SyncerConfig{
		WorkerCount:             defaultWorkerCount,
		Batch:                   defaultBatch,
		QueueSize:               defaultQueueSize,
		CheckpointFlushInterval: defaultCheckpointFlushInterval,
	}
}

// alias to avoid infinite recursion for UnmarshalYAML
type rawSyncerConfig SyncerConfig

// UnmarshalYAML implements Unmarshaler.UnmarshalYAML
func (m *SyncerConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	raw := rawSyncerConfig(defaultSyncerConfig())
	if err := unmarshal(&raw); err != nil {
		return terror.ErrConfigYamlTransform.Delegate(err, "unmarshal syncer config")
	}
	*m = SyncerConfig(raw) // raw used only internal, so no deep copy
	return nil
}

// TaskConfig is the configuration for Task
type TaskConfig struct {
	*flag.FlagSet `yaml:"-" toml:"-" json:"-"`

	Name       string `yaml:"name" toml:"name" json:"name"`
	TaskMode   string `yaml:"task-mode" toml:"task-mode" json:"task-mode"`
	IsSharding bool   `yaml:"is-sharding" toml:"is-sharding" json:"is-sharding"`
	ShardMode  string `yaml:"shard-mode" toml:"shard-mode" json:"shard-mode"` // when `shard-mode` set, we always enable sharding support.
	// treat it as hidden configuration
	IgnoreCheckingItems []string `yaml:"ignore-checking-items" toml:"ignore-checking-items" json:"ignore-checking-items"`
	// we store detail status in meta
	// don't save configuration into it
	MetaSchema string `yaml:"meta-schema" toml:"meta-schema" json:"meta-schema"`

	EnableHeartbeat         bool   `yaml:"enable-heartbeat" toml:"enable-heartbeat" json:"enable-heartbeat"`
	HeartbeatUpdateInterval int    `yaml:"heartbeat-update-interval" toml:"heartbeat-update-interval" json:"heartbeat-update-interval"`
	HeartbeatReportInterval int    `yaml:"heartbeat-report-interval" toml:"heartbeat-report-interval" json:"heartbeat-report-interval"`
	Timezone                string `yaml:"timezone" toml:"timezone" json:"timezone"`

	// handle schema/table name mode, and only for schema/table name
	// if case insensitive, we would convert schema/table name to lower case
	CaseSensitive bool `yaml:"case-sensitive" toml:"case-sensitive" json:"case-sensitive"`

	TargetDB *DBConfig `yaml:"target-database" toml:"target-database" json:"target-database"`

	MySQLInstances []*MySQLInstance `yaml:"mysql-instances" toml:"mysql-instances" json:"mysql-instances"`

	OnlineDDLScheme string `yaml:"online-ddl-scheme" toml:"online-ddl-scheme" json:"online-ddl-scheme"`

	Routes         map[string]*router.TableRule   `yaml:"routes" toml:"routes" json:"routes"`
	Filters        map[string]*bf.BinlogEventRule `yaml:"filters" toml:"filters" json:"filters"`
	ColumnMappings map[string]*column.Rule        `yaml:"column-mappings" toml:"column-mappings" json:"column-mappings"`

	// black-white-list is deprecated, use block-allow-list instead
	BWList map[string]*filter.Rules `yaml:"black-white-list" toml:"black-white-list" json:"black-white-list"`
	BAList map[string]*filter.Rules `yaml:"block-allow-list" toml:"block-allow-list" json:"block-allow-list"`

	Mydumpers map[string]*MydumperConfig `yaml:"mydumpers" toml:"mydumpers" json:"mydumpers"`
	Loaders   map[string]*LoaderConfig   `yaml:"loaders" toml:"loaders" json:"loaders"`
	Syncers   map[string]*SyncerConfig   `yaml:"syncers" toml:"syncers" json:"syncers"`

	CleanDumpFile bool `yaml:"clean-dump-file" toml:"clean-dump-file" json:"clean-dump-file"`
	// deprecated
	EnableANSIQuotes bool `yaml:"ansi-quotes" toml:"ansi-quotes" json:"ansi-quotes"`

	// deprecated, replaced by `start-task --remove-meta`
	RemoveMeta bool `yaml:"remove-meta"`
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
		BAList:                  make(map[string]*filter.Rules),
		Mydumpers:               make(map[string]*MydumperConfig),
		Loaders:                 make(map[string]*LoaderConfig),
		Syncers:                 make(map[string]*SyncerConfig),
		CleanDumpFile:           true,
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

// JSON returns the config's json string
func (c *TaskConfig) JSON() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		log.L().Error("marshal task config to json", zap.String("task", c.Name), log.ShortError(err))
	}
	return string(cfg)
}

// DecodeFile loads and decodes config from file
func (c *TaskConfig) DecodeFile(fpath string) error {
	bs, err := ioutil.ReadFile(fpath)
	if err != nil {
		return terror.ErrConfigReadCfgFromFile.Delegate(err, fpath)
	}

	err = yaml.UnmarshalStrict(bs, c)
	if err != nil {
		return terror.ErrConfigYamlTransform.Delegate(err)
	}

	return c.adjust()
}

// Decode loads config from file data
func (c *TaskConfig) Decode(data string) error {
	err := yaml.UnmarshalStrict([]byte(data), c)
	if err != nil {
		return terror.ErrConfigYamlTransform.Delegate(err, "decode task config failed")
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

	if c.ShardMode != "" && c.ShardMode != ShardPessimistic && c.ShardMode != ShardOptimistic {
		return terror.ErrConfigShardModeNotSupport.Generate(c.ShardMode)
	} else if c.ShardMode == "" && c.IsSharding {
		c.ShardMode = ShardPessimistic // use the pessimistic mode as default for back compatible.
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
		if err := inst.VerifyAndAdjust(); err != nil {
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

		// only when BAList is empty use BWList
		if len(c.BAList) == 0 && len(c.BWList) != 0 {
			c.BAList = c.BWList
		}
		if _, ok := c.BAList[inst.BAListName]; len(inst.BAListName) > 0 && !ok {
			return terror.ErrConfigBAListNotFound.Generate(i, inst.BAListName)
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

		// for backward compatible, set global config `ansi-quotes: true` if any syncer is true
		if inst.Syncer.EnableANSIQuotes == true {
			log.L().Warn("DM could discover proper ANSI_QUOTES, `enable-ansi-quotes` is no longer take effect")
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

	if c.RemoveMeta {
		log.L().Warn("`remove-meta` in task config is deprecated, please use `start-task ... --remove-meta` instead")
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
		cfg.ShardMode = c.ShardMode
		cfg.OnlineDDLScheme = c.OnlineDDLScheme
		cfg.IgnoreCheckingItems = c.IgnoreCheckingItems
		cfg.Name = c.Name
		cfg.Mode = c.TaskMode
		cfg.CaseSensitive = c.CaseSensitive
		cfg.MetaSchema = c.MetaSchema
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

		cfg.BAList = c.BAList[inst.BAListName]

		cfg.MydumperConfig = *inst.Mydumper
		cfg.LoaderConfig = *inst.Loader
		cfg.SyncerConfig = *inst.Syncer

		cfg.CleanDumpFile = c.CleanDumpFile

		err := cfg.Adjust(true)
		if err != nil {
			return nil, terror.Annotatef(err, "source %s", inst.SourceID)
		}

		cfgs[i] = cfg
	}
	return cfgs, nil
}

// FromSubTaskConfigs constructs task configs from a list of valid subtask configs.
// this method is only used to construct config when importing from v1.0.x now.
func (c *TaskConfig) FromSubTaskConfigs(stCfgs ...*SubTaskConfig) {
	// global configs.
	stCfg0 := stCfgs[0]
	c.Name = stCfg0.Name
	c.TaskMode = stCfg0.Mode
	c.IsSharding = stCfg0.IsSharding
	c.ShardMode = stCfg0.ShardMode
	c.IgnoreCheckingItems = stCfg0.IgnoreCheckingItems
	c.MetaSchema = stCfg0.MetaSchema
	c.EnableHeartbeat = stCfg0.EnableHeartbeat
	c.HeartbeatUpdateInterval = stCfg0.HeartbeatUpdateInterval
	c.HeartbeatReportInterval = stCfg0.HeartbeatReportInterval
	c.Timezone = stCfg0.Timezone
	c.CaseSensitive = stCfg0.CaseSensitive
	c.TargetDB = &stCfg0.To // just ref
	c.OnlineDDLScheme = stCfg0.OnlineDDLScheme
	c.CleanDumpFile = stCfg0.CleanDumpFile
	c.MySQLInstances = make([]*MySQLInstance, 0, len(stCfgs))
	c.BAList = make(map[string]*filter.Rules)
	c.Routes = make(map[string]*router.TableRule)
	c.Filters = make(map[string]*bf.BinlogEventRule)
	c.ColumnMappings = make(map[string]*column.Rule)
	c.Mydumpers = make(map[string]*MydumperConfig)
	c.Loaders = make(map[string]*LoaderConfig)
	c.Syncers = make(map[string]*SyncerConfig)

	// NOTE:
	// - we choose to ref global configs for instances now.
	// - no DeepEqual for rules now, so not combine REAL same rule into only one.
	for i, stCfg := range stCfgs {
		BAListName := fmt.Sprintf("balist-%02d", i+1)
		c.BAList[BAListName] = stCfg.BAList

		routeNames := make([]string, 0, len(stCfg.RouteRules))
		for j, rule := range stCfg.RouteRules {
			routeName := fmt.Sprintf("route-%02d-%02d", i+1, j+1)
			routeNames = append(routeNames, routeName)
			c.Routes[routeName] = rule
		}

		filterNames := make([]string, 0, len(stCfg.FilterRules))
		for j, rule := range stCfg.FilterRules {
			filterName := fmt.Sprintf("filter-%02d-%02d", i+1, j+1)
			filterNames = append(filterNames, filterName)
			c.Filters[filterName] = rule
		}

		dumpName := fmt.Sprintf("dump-%02d", i+1)
		c.Mydumpers[dumpName] = &stCfg.MydumperConfig

		loadName := fmt.Sprintf("load-%02d", i+1)
		c.Loaders[loadName] = &stCfg.LoaderConfig

		syncName := fmt.Sprintf("sync-%02d", i+1)
		c.Syncers[syncName] = &stCfg.SyncerConfig

		cmNames := make([]string, 0, len(stCfg.ColumnMappingRules))
		for j, rule := range stCfg.ColumnMappingRules {
			cmName := fmt.Sprintf("cm-%02d-%02d", i+1, j+1)
			cmNames = append(cmNames, cmName)
			c.ColumnMappings[cmName] = rule
		}

		c.MySQLInstances = append(c.MySQLInstances, &MySQLInstance{
			SourceID:           stCfg.SourceID,
			Meta:               stCfg.Meta,
			FilterRules:        filterNames,
			ColumnMappingRules: cmNames,
			RouteRules:         routeNames,
			BAListName:         BAListName,
			MydumperConfigName: dumpName,
			LoaderConfigName:   loadName,
			SyncerConfigName:   syncName,
		})
	}
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
