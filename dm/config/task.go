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

package config

import (
	"flag"
	"io/ioutil"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/siddontang/go-mysql/mysql"
	"gopkg.in/yaml.v2"
)

// Meta represents binlog's meta pos
// NOTE: refine to put these config structs into pkgs
type Meta struct {
	BinLogName string `yaml:"binlog-name"`
	BinLogPos  uint32 `yaml:"binlog-pos"`
	BinlogGTID string `yaml:"binlog-gtid"`
}

// MySQLInstance represents a sync config of a MySQL instance
type MySQLInstance struct {
	Config             *DBConfig `yaml:"config"`
	ServerID           int       `yaml:"server-id"`
	RelayDir           string    `yaml:"relay-dir"`
	Meta               *Meta     `yaml:"meta"`
	FilterRules        []string  `yaml:"filter-rules"`
	ColumnMappingRules []string  `yaml:"column-mapping-rules"`
	RouteRules         []string  `yaml:"route-rules"`

	MydumperConfigName string          `yaml:"mydumper-config-name"`
	Mydumper           *MydumperConfig `yaml:"mydumper"`
	LoaderConfigName   string          `yaml:"loader-config-name"`
	Loader             *LoaderConfig   `yaml:"loader"`
	SyncerConfigName   string          `yaml:"syncer-config-name"`
	Syncer             *SyncerConfig   `yaml:"syncer"`
}

// Verify does verification on configs
func (m *MySQLInstance) Verify() error {
	if m == nil || m.Config == nil {
		return errors.New("config must specify")
	}
	if m.ServerID < 1 {
		return errors.NotValidf("server-id should from 1 to 2^32 − 1, but set with %d", m.ServerID)
	}
	// if m.RelayDir == "" {
	// 	return errors.New("relay-dir must specify")
	// }

	if len(m.MydumperConfigName) > 0 && m.Mydumper != nil {
		return errors.New("mydumper-config-name and mydumper should only specify one")
	}
	if len(m.LoaderConfigName) > 0 && m.Loader != nil {
		return errors.New("loader-config-name and loader should only specify one")
	}
	if len(m.SyncerConfigName) > 0 && m.Syncer != nil {
		return errors.New("syncer-config-name and syncer should only specify one")
	}
	return nil
}

// FilterRule represents a filter rule
// not used until refactor *filter*
type FilterRule struct {
	PatternSchema string   `yaml:"pattern-schema"`
	PatternTable  string   `yaml:"pattern-table"`
	DoStatement   []string `yaml:"do-statement"`
	SkipStatement []string `yaml:"skip-statement"`
}

// ColumnMappingRule represents a column-mapping rule
// not used until add *column-mapping*
type ColumnMappingRule struct {
	PatternSchema string   `yaml:"pattern-schema"`
	PatternTable  string   `yaml:"pattern-table"`
	Op            string   `yaml:"op"`
	SourceColumn  string   `yaml:"source-column"`
	TargetColumn  string   `yaml:"target-column"`
	Expression    string   `yaml:"expression"`
	Arguments     []string `yaml:"arguments"`
	SQL           string   `yaml:"sql"`
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

// LoaderConfig represents loader process unit's specific config
type LoaderConfig struct {
	PoolSize         int    `yaml:"pool-size" toml:"pool-size" json:"pool-size"`
	Dir              string `yaml:"dir" toml:"dir" json:"dir"`
	CheckPointSchema string `yaml:"checkpoint-schema" toml:"checkpoint-schema" json:"checkpoint-schema"`
	RemoveCheckpoint bool   `yaml:"rm-checkpoint" toml:"rm-checkpoint" json:"rm-checkpoint"`
}

// SyncerConfig represents syncer process unit's specific config
type SyncerConfig struct {
	Meta        string `yaml:"meta" toml:"meta" json:"meta"` // meta filename
	WorkerCount int    `yaml:"worker-count" toml:"worker-count" json:"worker-count"`
	Batch       int    `yaml:"batch" toml:"batch" json:"batch"`
	MaxRetry    int    `yaml:"max-retry" toml:"max-retry" json:"max-retry"`

	// refine following configs to top level configs?
	EnableGTID  bool `yaml:"enable-gtid" toml:"enable-gtid" json:"enable-gtid"`
	AutoFixGTID bool `yaml:"auto-fix-gtid" toml:"auto-fix-gtid" json:"auto-fix-gtid"`

	DisableCausality bool `yaml:"disable-detect" toml:"disable-detect" json:"disable-detect"`
	SafeMode         bool `yaml:"safe-mode" toml:"safe-mode" json:"safe-mode"`
}

// TaskConfig is the configuration for Task
type TaskConfig struct {
	*flag.FlagSet `yaml:"-"`

	Name           string `yaml:"name"`
	TaskMode       string `yaml:"task-mode"`
	Flavor         string `yaml:"flavor"`
	VerifyChecksum bool   `yaml:"verify-checksum"`

	TargetDB *DBConfig `yaml:"target-database"`

	MySQLInstances []*MySQLInstance `yaml:"MySQL-instances"`

	Routes         map[string]*router.TableRule  `yaml:"routes"`
	Filters        map[string]*FilterRule        `yaml:"filters"`
	ColumnMappings map[string]*ColumnMappingRule `yaml:"column-mappings"`

	Mydumpers map[string]*MydumperConfig `yaml:"mydumpers"`
	Loaders   map[string]*LoaderConfig   `yaml:"loaders"`
	Syncers   map[string]*SyncerConfig   `yaml:"syncers"`
}

// NewTaskConfig creates a TaskConfig
func NewTaskConfig() *TaskConfig {
	cfg := &TaskConfig{}
	cfg.FlagSet = flag.NewFlagSet("task", flag.ContinueOnError)
	return cfg
}

// String returns the config's yaml string
func (c *TaskConfig) String() string {
	cfg, err := yaml.Marshal(c)
	if err != nil {
		log.Errorf("[config] marshal task config to yaml error %v", err)
	}
	return string(cfg)
}

// DecodeFile loads and decodes config from file
func (c *TaskConfig) DecodeFile(fpath string) error {
	bs, err := ioutil.ReadFile(fpath)
	if err != nil {
		return errors.Annotatef(err, "read config file %v", fpath)
	}

	err = yaml.Unmarshal(bs, c)
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(c.adjust())
}

// Decode loads config from file data
func (c *TaskConfig) Decode(data string) error {
	err := yaml.Unmarshal([]byte(data), c)
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(c.adjust())
}

// adjust adjusts configs
func (c *TaskConfig) adjust() error {
	if len(c.Name) == 0 {
		return errors.New("must specify a unique task name")
	}
	if c.TaskMode != ModeFull && c.TaskMode != ModeIncrement && c.TaskMode != ModeAll {
		return errors.New("please specify right task-mode, support `full`, `incremental`, `all`")
	}
	if c.Flavor != mysql.MySQLFlavor && c.Flavor != mysql.MariaDBFlavor {
		return errors.New("please specify right mysql flavor version, support `mysql`, `mariadb`")
	}

	if c.TargetDB == nil {
		return errors.New("must specify target-database")
	}

	if len(c.MySQLInstances) == 0 {
		return errors.New("must specify at least one MySQL-instances")
	}

	sids := make(map[int]int) // server-id -> instance-index
	for i, inst := range c.MySQLInstances {
		if err := inst.Verify(); err != nil {
			return errors.Annotatef(err, "MySQL-instance: %d", i)
		}
		if sid, ok := sids[inst.ServerID]; ok {
			return errors.Errorf("MySQL-instances (%d) and (%d) have same server-id (%d)", sid, i, inst.ServerID)
		}
		sids[inst.ServerID] = i

		for _, name := range inst.RouteRules {
			if _, ok := c.Routes[name]; !ok {
				return errors.Errorf("MySQL-instance(%d)'s route-rules %s not exist in routes", i, name)
			}
		}
		for _, name := range inst.FilterRules {
			if _, ok := c.Filters[name]; !ok {
				return errors.Errorf("MySQL-instance(%d)'s filter-rules %s not exist in filters", i, name)
			}
		}
		for _, name := range inst.ColumnMappingRules {
			if _, ok := c.ColumnMappings[name]; !ok {
				return errors.Errorf("MySQL-instance(%d)'s column-mapping-rules %s not exist in column-mapping", i, name)
			}
		}

		if len(inst.MydumperConfigName) > 0 {
			rule, ok := c.Mydumpers[inst.MydumperConfigName]
			if !ok {
				return errors.Errorf("MySQL-instance(%d)'s mydumper config %s not exist in mydumpers", i, inst.MydumperConfigName)
			}
			inst.Mydumper = rule // ref mydumper config
		}

		if (c.TaskMode == ModeFull || c.TaskMode == ModeAll) && len(inst.Mydumper.MydumperPath) == 0 {
			// only verify if set, whether is valid can only be verify when we run it
			return errors.Errorf("MySQL-instance(%d)'s mydumper-path must specify a valid path to mydumper binary when task-mode is all or full", i)
		}

		if len(inst.LoaderConfigName) > 0 {
			rule, ok := c.Loaders[inst.LoaderConfigName]
			if !ok {
				return errors.Errorf("MySQL-instance(%d)'s loader config %s not exist in loaders", i, inst.LoaderConfigName)
			}
			inst.Loader = rule // ref loader config
		}
		if len(inst.SyncerConfigName) > 0 {
			rule, ok := c.Syncers[inst.SyncerConfigName]
			if !ok {
				return errors.Errorf("MySQL-instance(%d)'s syncer config %s not exist in syncer", i, inst.SyncerConfigName)
			}
			inst.Syncer = rule // ref syncer config
		}
	}

	return nil
}

// SubTaskConfigs generates sub task configs
func (c *TaskConfig) SubTaskConfigs() []*SubTaskConfig {
	cfgs := make([]*SubTaskConfig, len(c.MySQLInstances))
	for i, inst := range c.MySQLInstances {
		cfg := NewSubTaskConfig()
		cfg.Name = c.Name
		cfg.Mode = c.TaskMode
		cfg.Flavor = c.Flavor
		cfg.BinlogType = "local" // let's force syncer to replay local binlog.
		cfg.RelayDir = inst.RelayDir
		cfg.VerifyChecksum = c.VerifyChecksum

		cfg.From = *inst.Config
		cfg.To = *c.TargetDB

		cfg.ServerID = inst.ServerID

		cfg.RouteRules = make([]*router.TableRule, len(inst.RouteRules))
		for j, name := range inst.RouteRules {
			cfg.RouteRules[j] = c.Routes[name]
		}
		// TODO zxc: update meta file's content by `Meta`, or save(and update) meta in DB
		// TODO zxc: add new filter rules
		// TODO zxc: add column-mapping rules

		cfg.MydumperConfig = *inst.Mydumper
		cfg.LoaderConfig = *inst.Loader
		cfg.SyncerConfig = *inst.Syncer

		cfgs[i] = cfg
	}
	return cfgs
}
