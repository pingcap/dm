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

package master

import (
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/BurntSushi/toml"
	"go.etcd.io/etcd/embed"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

const (
	defaultRPCTimeout          = "30s"
	defaultNamePrefix          = "dm-master"
	defaultDataDirPrefix       = "default"
	defaultPeerUrls            = "http://127.0.0.1:8291"
	defaultInitialClusterState = embed.ClusterStateFlagNew
)

var (
	// EnableZap enable the zap logger in embed etcd.
	EnableZap = false
	// SampleConfigFile is sample config file of dm-master
	// later we can read it from dm/master/dm-master.toml
	// and assign it to SampleConfigFile while we build dm-master
	SampleConfigFile string
)

// NewConfig creates a config for dm-master
func NewConfig() *Config {
	cfg := &Config{}
	cfg.Debug = false
	cfg.FlagSet = flag.NewFlagSet("dm-master", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.BoolVar(&cfg.printVersion, "V", false, "prints version and exit")
	fs.BoolVar(&cfg.printSampleConfig, "print-sample-config", false, "print sample config file of dm-worker")
	fs.StringVar(&cfg.ConfigFile, "config", "", "path to config file")
	fs.StringVar(&cfg.MasterAddr, "master-addr", "", "master API server and status addr")
	fs.StringVar(&cfg.AdvertiseAddr, "advertise-addr", "", `advertise address for client traffic (default "${master-addr}")`)
	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.LogFile, "log-file", "", "log file path")
	fs.StringVar(&cfg.LogFormat, "log-format", "text", `the format of the log, "text" or "json"`)
	//fs.StringVar(&cfg.LogRotate, "log-rotate", "day", "log file rotate type, hour/day")

	fs.StringVar(&cfg.Name, "name", "", "human-readable name for this DM-master member")
	fs.StringVar(&cfg.DataDir, "data-dir", "", `path to the data directory (default "default.${name}")`)
	fs.StringVar(&cfg.InitialCluster, "initial-cluster", "", fmt.Sprintf("initial cluster configuration for bootstrapping, e.g. dm-master=%s", defaultPeerUrls))
	fs.StringVar(&cfg.PeerUrls, "peer-urls", defaultPeerUrls, "URLs for peer traffic")
	fs.StringVar(&cfg.AdvertisePeerUrls, "advertise-peer-urls", "", `advertise URLs for peer traffic (default "${peer-urls}")`)
	fs.StringVar(&cfg.Join, "join", "", `join to an existing cluster (usage: cluster's "${master-addr}" list, e.g. "127.0.0.1:8261,127.0.0.1:18261"`)

	fs.StringVar(&cfg.SSLCA, "ssl-ca", "", "path of file that contains list of trusted SSL CAs for connection")
	fs.StringVar(&cfg.SSLCert, "ssl-cert", "", "path of file that contains X509 certificate in PEM format for connection")
	fs.StringVar(&cfg.SSLKey, "ssl-key", "", "path of file that contains X509 key in PEM format for connection")
	fs.Var(&cfg.CertAllowedCN, "cert-allowed-cn", "the trusted common name that allowed to visit")

	fs.StringVar(&cfg.V1SourcesPath, "v1-sources-path", "", "directory path used to store source config files when upgrading from v1.0.x")

	return cfg
}

// DeployMapper defines dm-worker's deploy mapper info: source id -> dm-worker ${host:ip}
type DeployMapper struct {
	MySQL  string `toml:"mysql-instance" json:"mysql-instance"` //  deprecated, use source-id instead
	Source string `toml:"source-id" json:"source-id"`           // represents a MySQL/MariaDB instance or a replica group
	Worker string `toml:"dm-worker" json:"dm-worker"`
}

// Verify verifies deploy configuration
func (d *DeployMapper) Verify() error {
	if d.MySQL == "" && d.Source == "" {
		return terror.ErrMasterDeployMapperVerify.Generate(d)
	}

	return nil
}

// Config is the configuration for dm-master
type Config struct {
	*flag.FlagSet `json:"-"`

	LogLevel  string `toml:"log-level" json:"log-level"`
	LogFile   string `toml:"log-file" json:"log-file"`
	LogFormat string `toml:"log-format" json:"log-format"`
	LogRotate string `toml:"log-rotate" json:"log-rotate"`

	RPCTimeoutStr string        `toml:"rpc-timeout" json:"rpc-timeout"`
	RPCTimeout    time.Duration `json:"-"`
	RPCRateLimit  float64       `toml:"rpc-rate-limit" json:"rpc-rate-limit"`
	RPCRateBurst  int           `toml:"rpc-rate-burst" json:"rpc-rate-burst"`

	MasterAddr    string `toml:"master-addr" json:"master-addr"`
	AdvertiseAddr string `toml:"advertise-addr" json:"advertise-addr"`

	Deploy []*DeployMapper `toml:"deploy" json:"-"`
	// TODO: remove
	DeployMap map[string]string `json:"deploy"`

	ConfigFile string `json:"config-file"`

	// etcd relative config items
	// NOTE: we use `MasterAddr` to generate `ClientUrls` and `AdvertiseClientUrls`
	// NOTE: more items will be add when adding leader election
	Name                string `toml:"name" json:"name"`
	DataDir             string `toml:"data-dir" json:"data-dir"`
	PeerUrls            string `toml:"peer-urls" json:"peer-urls"`
	AdvertisePeerUrls   string `toml:"advertise-peer-urls" json:"advertise-peer-urls"`
	InitialCluster      string `toml:"initial-cluster" json:"initial-cluster"`
	InitialClusterState string `toml:"initial-cluster-state" json:"initial-cluster-state"`
	Join                string `toml:"join" json:"join"`   // cluster's client address (endpoints), not peer address
	Debug               bool   `toml:"debug" json:"debug"` // only use for test

	// directory path used to store source config files when upgrading from v1.0.x.
	// if this path set, DM-master leader will try to upgrade from v1.0.x to the current version.
	V1SourcesPath string `toml:"v1-sources-path" json:"v1-sources-path"`

	// tls config
	config.Security

	printVersion      bool
	printSampleConfig bool
}

func (c *Config) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		log.L().Error("marshal to json", zap.Reflect("master config", c), log.ShortError(err))
	}
	return string(cfg)
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.FlagSet.Parse(arguments)
	if err != nil {
		return terror.ErrMasterConfigParseFlagSet.Delegate(err)
	}

	if c.printVersion {
		fmt.Println(utils.GetRawInfo())
		return flag.ErrHelp
	}

	if c.printSampleConfig {
		if strings.TrimSpace(SampleConfigFile) == "" {
			fmt.Println("sample config file of dm-master is empty")
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
	err = c.FlagSet.Parse(arguments)
	if err != nil {
		return terror.ErrMasterConfigParseFlagSet.Delegate(err)
	}

	if len(c.FlagSet.Args()) != 0 {
		return terror.ErrMasterConfigInvalidFlag.Generate(c.FlagSet.Arg(0))
	}

	return c.adjust()
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) error {
	metaData, err := toml.DecodeFile(path, c)
	if err != nil {
		return terror.ErrMasterConfigTomlTransform.Delegate(err)
	}
	undecoded := metaData.Undecoded()
	if len(undecoded) > 0 {
		var undecodedItems []string
		for _, item := range undecoded {
			undecodedItems = append(undecodedItems, item.String())
		}
		return terror.ErrMasterConfigUnknownItem.Generate(strings.Join(undecodedItems, ","))
	}
	return nil
}

// adjust adjusts configs
func (c *Config) adjust() error {
	c.MasterAddr = utils.UnwrapScheme(c.MasterAddr)
	// MasterAddr's format may be "host:port" or ":port"
	host, port, err := net.SplitHostPort(c.MasterAddr)
	if err != nil {
		return terror.ErrMasterHostPortNotValid.Delegate(err, c.MasterAddr)
	}

	if c.AdvertiseAddr == "" {
		if host == "" || host == "0.0.0.0" || len(port) == 0 {
			return terror.ErrMasterHostPortNotValid.Generatef("master-addr (%s) must include the 'host' part (should not be '0.0.0.0') when advertise-addr is not set", c.MasterAddr)
		}
		c.AdvertiseAddr = c.MasterAddr
	} else {
		c.AdvertiseAddr = utils.UnwrapScheme(c.AdvertiseAddr)
		// AdvertiseAddr's format should be "host:port"
		host, port, err = net.SplitHostPort(c.AdvertiseAddr)
		if err != nil {
			return terror.ErrMasterAdvertiseAddrNotValid.Delegate(err, c.AdvertiseAddr)
		}
		if len(host) == 0 || host == "0.0.0.0" || len(port) == 0 {
			return terror.ErrMasterAdvertiseAddrNotValid.Generate(c.AdvertiseAddr)
		}
	}

	c.DeployMap = make(map[string]string)
	for _, item := range c.Deploy {
		if err = item.Verify(); err != nil {
			return err
		}

		// compatible with mysql instance which is deprecated
		if item.Source == "" {
			item.Source = item.MySQL
		}

		c.DeployMap[item.Source] = item.Worker
	}

	if c.RPCTimeoutStr == "" {
		c.RPCTimeoutStr = defaultRPCTimeout
	}
	timeout, err := time.ParseDuration(c.RPCTimeoutStr)
	if err != nil {
		return terror.ErrMasterConfigTimeoutParse.Delegate(err)
	}
	c.RPCTimeout = timeout

	// for backward compatibility
	if c.RPCRateLimit <= 0 {
		log.L().Warn("invalid rpc-rate-limit, default value used", zap.Float64("specified rpc-rate-limit", c.RPCRateLimit), zap.Float64("default rpc-rate-limit", DefaultRate))
		c.RPCRateLimit = DefaultRate
	}
	if c.RPCRateBurst <= 0 {
		log.L().Warn("invalid rpc-rate-burst, default value use", zap.Int("specified rpc-rate-burst", c.RPCRateBurst), zap.Int("default rpc-rate-burst", DefaultBurst))
		c.RPCRateBurst = DefaultBurst
	}

	if c.Name == "" {
		var hostname string
		hostname, err = os.Hostname()
		if err != nil {
			return terror.ErrMasterGetHostnameFail.Delegate(err)
		}
		c.Name = fmt.Sprintf("%s-%s", defaultNamePrefix, hostname)
	}

	if c.DataDir == "" {
		c.DataDir = fmt.Sprintf("%s.%s", defaultDataDirPrefix, c.Name)
	}

	if c.PeerUrls == "" {
		c.PeerUrls = defaultPeerUrls
	} else {
		c.PeerUrls = utils.WrapSchemes(c.PeerUrls, c.SSLCA != "")
	}

	if c.AdvertisePeerUrls == "" {
		c.AdvertisePeerUrls = c.PeerUrls
	} else {
		c.AdvertisePeerUrls = utils.WrapSchemes(c.AdvertisePeerUrls, c.SSLCA != "")
	}

	if c.InitialCluster == "" {
		items := strings.Split(c.AdvertisePeerUrls, ",")
		for i, item := range items {
			items[i] = fmt.Sprintf("%s=%s", c.Name, item)
		}
		c.InitialCluster = strings.Join(items, ",")
	} else {
		c.InitialCluster = utils.WrapSchemesForInitialCluster(c.InitialCluster, c.SSLCA != "")
	}

	if c.InitialClusterState == "" {
		c.InitialClusterState = defaultInitialClusterState
	}

	if c.Join != "" {
		c.Join = utils.WrapSchemes(c.Join, c.SSLCA != "")
	}

	return err
}

// UpdateConfigFile write config to local file
// if ConfigFile is nil, it will write to dm-master.toml
func (c *Config) UpdateConfigFile(content string) error {
	if c.ConfigFile != "" {
		err := ioutil.WriteFile(c.ConfigFile, []byte(content), 0666)
		return terror.ErrMasterConfigUpdateCfgFile.Delegate(err)
	}
	c.ConfigFile = "dm-master.toml"
	err := ioutil.WriteFile(c.ConfigFile, []byte(content), 0666)
	return terror.ErrMasterConfigUpdateCfgFile.Delegate(err)
}

// Reload load config from local file
func (c *Config) Reload() error {
	if c.ConfigFile != "" {
		err := c.configFromFile(c.ConfigFile)
		if err != nil {
			return err
		}
	}

	return c.adjust()
}

// genEmbedEtcdConfig generates the configuration needed by embed etcd.
// This method should be called after logger initialized and before any concurrent gRPC calls.
func (c *Config) genEmbedEtcdConfig(cfg *embed.Config) (*embed.Config, error) {
	cfg.Name = c.Name
	cfg.Dir = c.DataDir

	// reuse the previous master-addr as the client listening URL.
	var err error
	cfg.LCUrls, err = parseURLs(c.MasterAddr)
	if err != nil {
		return nil, terror.ErrMasterGenEmbedEtcdConfigFail.Delegate(err, "invalid master-addr")
	}
	cfg.ACUrls, err = parseURLs(c.AdvertiseAddr)
	if err != nil {
		return nil, terror.ErrMasterGenEmbedEtcdConfigFail.Delegate(err, "invalid advertise-addr")
	}

	cfg.LPUrls, err = parseURLs(c.PeerUrls)
	if err != nil {
		return nil, terror.ErrMasterGenEmbedEtcdConfigFail.Delegate(err, "invalid peer-urls")
	}

	cfg.APUrls, err = parseURLs(c.AdvertisePeerUrls)
	if err != nil {
		return nil, terror.ErrMasterGenEmbedEtcdConfigFail.Delegate(err, "invalid advertise-peer-urls")
	}

	cfg.InitialCluster = c.InitialCluster
	cfg.ClusterState = c.InitialClusterState

	err = cfg.Validate() // verify & trigger the builder
	if err != nil {
		return nil, terror.ErrMasterGenEmbedEtcdConfigFail.AnnotateDelegate(err, "fail to validate embed etcd config")
	}

	// security config
	if len(c.SSLCA) != 0 {
		cfg.ClientTLSInfo.TrustedCAFile = c.SSLCA
		cfg.ClientTLSInfo.CertFile = c.SSLCert
		cfg.ClientTLSInfo.KeyFile = c.SSLKey

		cfg.PeerTLSInfo.TrustedCAFile = c.SSLCA
		cfg.PeerTLSInfo.CertFile = c.SSLCert
		cfg.PeerTLSInfo.KeyFile = c.SSLKey

		// NOTE: etcd only support one allowed CN
		if len(c.CertAllowedCN) > 0 {
			cfg.ClientTLSInfo.AllowedCN = c.CertAllowedCN[0]
			cfg.PeerTLSInfo.AllowedCN = c.CertAllowedCN[0]
			cfg.PeerTLSInfo.ClientCertAuth = len(c.SSLCA) != 0
			cfg.ClientTLSInfo.ClientCertAuth = len(c.SSLCA) != 0
		}
	}

	return cfg, nil
}

// parseURLs parse a string into multiple urls.
// if the URL in the string without protocol scheme, use `http` as the default.
// if no IP exists in the address, `0.0.0.0` is used.
func parseURLs(s string) ([]url.URL, error) {
	if s == "" {
		return nil, nil
	}

	items := strings.Split(s, ",")
	urls := make([]url.URL, 0, len(items))
	for _, item := range items {
		// tolerate valid `master-addr`, but invalid URL format. mainly caused by no protocol scheme
		if !(strings.HasPrefix(item, "http://") || strings.HasPrefix(item, "https://")) {
			prefix := "http://"
			if atomic.LoadInt32(&useTLS) == 1 {
				prefix = "https://"
			}
			item = prefix + item
		}
		u, err := url.Parse(item)
		if err != nil {
			return nil, terror.ErrMasterParseURLFail.Delegate(err, item)
		}
		if strings.Index(u.Host, ":") == 0 {
			u.Host = "0.0.0.0" + u.Host
		}
		urls = append(urls, *u)
	}
	return urls, nil
}

func genEmbedEtcdConfigWithLogger(logLevel string) *embed.Config {
	cfg := embed.NewConfig()
	cfg.EnableGRPCGateway = true // enable gRPC gateway for the internal etcd.

	// use zap as the logger for embed etcd
	// NOTE: `genEmbedEtcdConfig` can only be called after logger initialized.
	// NOTE: if using zap logger for etcd, must build it before any concurrent gRPC calls,
	// otherwise, DATA RACE occur in NewZapCoreLoggerBuilder and gRPC.
	logger := log.L().WithFields(zap.String("component", "embed etcd"))
	// if logLevel is info, set etcd log level to WARN to reduce log
	if strings.ToLower(logLevel) == "info" {
		log.L().Info("Set log level of etcd to `warn`, if you want to log more message about etcd, change log-level to `debug` in master configuration file")
		logger.Logger = logger.WithOptions(zap.IncreaseLevel(zap.WarnLevel))
	}

	cfg.ZapLoggerBuilder = embed.NewZapCoreLoggerBuilder(logger.Logger, logger.Core(), log.Props().Syncer) // use global app props.
	cfg.Logger = "zap"

	// TODO: we run ZapLoggerBuilder to set SetLoggerV2 before we do some etcd operations
	//       otherwise we will meet data race while running `grpclog.SetLoggerV2`
	//       It's vert tricky here, we should use a better way to avoid this in the future.
	cfg.ZapLoggerBuilder(cfg)

	return cfg
}
