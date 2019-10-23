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
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/siddontang/go-mysql/mysql"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/binlog"
	"github.com/pingcap/dm/pkg/conn"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/tracing"
	"github.com/pingcap/dm/pkg/utils"
	"github.com/pingcap/dm/relay/purger"
)

const (
	// dbReadTimeout is readTimeout for DB connection in adjust
	dbReadTimeout = "30s"
	// dbGetTimeout is timeout for getting some information from DB
	dbGetTimeout = 30 * time.Second
)

// SampleConfigFile is sample config file of dm-worker
// later we can read it from dm/worker/dm-worker.toml
// and assign it to SampleConfigFile while we build dm-worker
var SampleConfigFile string

var (
	getAllServerIDFunc = utils.GetAllServerID
)

// NewConfig creates a new base config for worker.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.flagSet = flag.NewFlagSet("worker", flag.ContinueOnError)
	fs := cfg.flagSet

	fs.BoolVar(&cfg.printVersion, "V", false, "prints version and exit")
	fs.BoolVar(&cfg.printSampleConfig, "print-sample-config", false, "print sample config file of dm-worker")
	fs.StringVar(&cfg.ConfigFile, "config", "", "path to config file")
	fs.StringVar(&cfg.WorkerAddr, "worker-addr", "", "worker API server and status addr")
	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.LogFile, "log-file", "", "log file path")
	//fs.StringVar(&cfg.LogRotate, "log-rotate", "day", "log file rotate type, hour/day")
	fs.StringVar(&cfg.RelayDir, "relay-dir", "./relay_log", "relay log directory")
	fs.Int64Var(&cfg.Purge.Interval, "purge-interval", 60*60, "interval (seconds) try to check whether needing to purge relay log files")
	fs.Int64Var(&cfg.Purge.Expires, "purge-expires", 0, "try to purge relay log files if their modified time is older than this (hours)")
	fs.Int64Var(&cfg.Purge.RemainSpace, "purge-remain-space", 15, "try to purge relay log files if remain space is less than this (GB)")
	fs.BoolVar(&cfg.Checker.CheckEnable, "checker-check-enable", true, "whether enable task status checker")
	fs.DurationVar(&cfg.Checker.BackoffRollback.Duration, "checker-backoff-rollback", DefaultBackoffRollback, "task status checker backoff rollback interval")
	fs.DurationVar(&cfg.Checker.BackoffMax.Duration, "checker-backoff-max", DefaultBackoffMax, "task status checker backoff max delay duration")
	fs.BoolVar(&cfg.Tracer.Enable, "tracer-enable", false, "whether to enable tracing")
	fs.StringVar(&cfg.Tracer.TracerAddr, "tracer-server-addr", "", "tracing service rpc address")
	fs.IntVar(&cfg.Tracer.BatchSize, "tracer-batch-size", 20, "upload to tracing service batch size")
	fs.BoolVar(&cfg.Tracer.Checksum, "tracer-checksum", false, "whether to calculate checksum of some data")

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
	RelayDir    string `toml:"relay-dir" json:"relay-dir"`
	MetaDir     string `toml:"meta-dir" json:"meta-dir"`
	ServerID    uint32 `toml:"server-id" json:"server-id"`
	Flavor      string `toml:"flavor" json:"flavor"`
	Charset     string `toml:"charset" json:"charset"`

	// relay synchronous starting point (if specified)
	RelayBinLogName string `toml:"relay-binlog-name" json:"relay-binlog-name"`
	RelayBinlogGTID string `toml:"relay-binlog-gtid" json:"relay-binlog-gtid"`

	SourceID string          `toml:"source-id" json:"source-id"`
	From     config.DBConfig `toml:"from" json:"from"`

	// config items for purger
	Purge purger.Config `toml:"purge" json:"purge"`

	// config items for task status checker
	Checker CheckerConfig `toml:"checker" json:"checker"`

	// config items for tracer
	Tracer tracing.Config `toml:"tracer" json:"tracer"`

	ConfigFile string `json:"config-file"`

	printVersion      bool
	printSampleConfig bool
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
		log.L().Error("fail to marshal config to json", log.ShortError(err))
	}
	return string(cfg)
}

// Toml returns TOML format representation of config
func (c *Config) Toml() (string, error) {
	var b bytes.Buffer

	err := toml.NewEncoder(&b).Encode(c)
	if err != nil {
		log.L().Error("fail to marshal config to toml", log.ShortError(err))
	}

	return b.String(), nil
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.flagSet.Parse(arguments)
	if err != nil {
		return terror.ErrWorkerParseFlagSet.Delegate(err)
	}

	if c.printVersion {
		fmt.Println(utils.GetRawInfo())
		return flag.ErrHelp
	}

	if c.printSampleConfig {
		if strings.TrimSpace(SampleConfigFile) == "" {
			fmt.Println("sample config file of dm-worker is empty")
		} else {
			rawConfig, err2 := base64.StdEncoding.DecodeString(SampleConfigFile)
			if err2 != nil {
				fmt.Println("base64 decode config error:", err2)
			} else {
				fmt.Println(string(rawConfig))
			}
		}
		return flag.ErrHelp
	}

	// Load config file if specified.
	if c.ConfigFile != "" {
		err = c.configFromFile(c.ConfigFile)
		if err != nil {
			return err
		}
	}

	// Parse again to replace with command line options.
	err = c.flagSet.Parse(arguments)
	if err != nil {
		return terror.ErrWorkerParseFlagSet.Delegate(err)
	}

	if len(c.flagSet.Args()) != 0 {
		return terror.ErrWorkerInvalidFlag.Generate(c.flagSet.Arg(0))
	}

	if len(c.MetaDir) == 0 {
		c.MetaDir = "./dm_worker_meta"
	}

	// assign tracer id to source id
	c.Tracer.Source = c.SourceID

	err = c.adjust()
	if err != nil {
		return err
	}
	return c.verify()
}

// verify verifies the config
func (c *Config) verify() error {
	if len(c.SourceID) == 0 {
		return terror.ErrWorkerNeedSourceID.Generate()
	}
	if len(c.SourceID) > config.MaxSourceIDLength {
		return terror.ErrWorkerTooLongSourceID.Generate(c.SourceID, config.MaxSourceIDLength)
	}

	var err error
	if len(c.RelayBinLogName) > 0 {
		if !binlog.VerifyFilename(c.RelayBinLogName) {
			return terror.ErrWorkerRelayBinlogName.Generate(c.RelayBinLogName)
		}
	}
	if len(c.RelayBinlogGTID) > 0 {
		_, err = gtid.ParserGTID(c.Flavor, c.RelayBinlogGTID)
		if err != nil {
			return terror.WithClass(terror.Annotatef(err, "relay-binlog-gtid %s", c.RelayBinlogGTID), terror.ClassDMWorker)
		}
	}

	_, err = c.DecryptPassword()
	if err != nil {
		return err
	}

	return nil
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) error {
	metaData, err := toml.DecodeFile(path, c)
	if err != nil {
		return terror.ErrWorkerDecodeConfigFromFile.Delegate(err)
	}
	undecoded := metaData.Undecoded()
	if len(undecoded) > 0 && err == nil {
		var undecodedItems []string
		for _, item := range undecoded {
			undecodedItems = append(undecodedItems, item.String())
		}
		return terror.ErrWorkerUndecodedItemFromFile.Generate(strings.Join(undecodedItems, ","))
	}

	return c.verify()
}

func (c *Config) adjust() error {
	c.From.Adjust()
	c.Checker.adjust()

	if c.Flavor == "" || c.ServerID == 0 {
		fromDB, err := c.createBaseDB()
		if err != nil {
			return err
		}
		defer fromDB.Close()

		ctx, cancel := context.WithTimeout(context.Background(), dbGetTimeout)
		defer cancel()

		err = c.adjustFlavor(ctx, fromDB.DB)
		if err != nil {
			return err
		}

		err = c.adjustServerID(ctx, fromDB.DB)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Config) createBaseDB() (*conn.BaseDB, error) {
	// decrypt password
	clone, err := c.DecryptPassword()
	if err != nil {
		return nil, err
	}
	from := clone.From
	from.RawDBCfg = config.DefaultRawDBConfig().SetReadTimeout(dbReadTimeout)
	fromDB, err := conn.DefaultDBProvider.Apply(from)
	if err != nil {
		return nil, terror.WithScope(err, terror.ScopeUpstream)
	}

	return fromDB, nil
}

// adjustFlavor adjusts flavor through querying from given database
func (c *Config) adjustFlavor(ctx context.Context, db *sql.DB) (err error) {
	if c.Flavor != "" {
		switch c.Flavor {
		case mysql.MariaDBFlavor, mysql.MySQLFlavor:
			return nil
		default:
			return terror.ErrNotSupportedFlavor.Generate(c.Flavor)
		}
	}

	c.Flavor, err = utils.GetFlavor(ctx, db)
	if ctx.Err() != nil {
		err = terror.Annotatef(err, "time cost to get flavor info exceeds %s", dbGetTimeout)
	}
	return terror.WithScope(err, terror.ScopeUpstream)
}

func (c *Config) adjustServerID(ctx context.Context, db *sql.DB) error {
	if c.ServerID != 0 {
		return nil
	}

	serverIDs, err := getAllServerIDFunc(ctx, db)
	if ctx.Err() != nil {
		err = terror.Annotatef(err, "time cost to get server-id info exceeds %s", dbGetTimeout)
	}
	if err != nil {
		return terror.WithScope(err, terror.ScopeUpstream)
	}

	for i := 0; i < 5; i++ {
		randomValue := uint32(rand.Intn(100000))
		randomServerID := maxServerID/10 + randomValue
		if _, ok := serverIDs[randomServerID]; ok {
			continue
		}

		c.ServerID = randomServerID
		return nil
	}

	return terror.ErrInvalidServerID.Generatef("can't find a random available server ID")
}

// UpdateConfigFile write configure to local file
func (c *Config) UpdateConfigFile(content string) error {
	if c.ConfigFile == "" {
		c.ConfigFile = "dm-worker-config.bak"
	}
	err := ioutil.WriteFile(c.ConfigFile, []byte(content), 0666)
	if err != nil {
		return terror.ErrWorkerWriteConfigFile.Delegate(err)
	}
	return nil
}

// Reload reload configure from ConfigFile
func (c *Config) Reload() error {
	if c.ConfigFile == "" {
		c.ConfigFile = "dm-worker-config.bak"
	}

	err := c.configFromFile(c.ConfigFile)
	if err != nil {
		return err
	}

	return nil
}

// DecryptPassword returns a decrypted config replica in config
func (c *Config) DecryptPassword() (*Config, error) {
	clone := c.Clone()
	var (
		pswdFrom string
		err      error
	)
	if len(clone.From.Password) > 0 {
		pswdFrom, err = utils.Decrypt(clone.From.Password)
		if err != nil {
			return nil, terror.WithClass(err, terror.ClassDMWorker)
		}
	}
	clone.From.Password = pswdFrom
	return clone, nil
}
