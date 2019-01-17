package common

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/pingcap/dm/pkg/utils"
)

// DmctlMode works in which mode
type DmctlMode string

// three work mode
var (
	WorkerMode  DmctlMode = "worker mode"
	MasterMode  DmctlMode = "master mode"
	OfflineMode DmctlMode = "offline mode"
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
	WorkerAddr string    `toml:"worker-addr" json:"-"`
	ServerAddr string    `json:"-"` // MasterAddr or WorkerAddr
	Mode       DmctlMode `json:"-"`

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
		ciphertext, err1 := utils.Encrypt(c.encrypt)
		if err1 != nil {
			fmt.Println(errors.ErrorStack(err1))
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

	if c.MasterAddr != "" {
		if err = validateAddr(c.MasterAddr); err != nil {
			return errors.Annotatef(err, "specify master addr %s", c.MasterAddr)
		}
	} else if c.WorkerAddr != "" {
		if err = validateAddr(c.WorkerAddr); err != nil {
			return errors.Annotatef(err, "specify worker addr %s", c.WorkerAddr)
		}
	}

	c.adjust()
	return nil
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	return errors.Trace(err)
}

// adjust adjusts configs
func (c *Config) adjust() {
	if c.MasterAddr != "" {
		c.ServerAddr = c.MasterAddr
		c.Mode = MasterMode
	} else if c.WorkerAddr != "" {
		c.ServerAddr = c.WorkerAddr
		c.Mode = WorkerMode
	} else {
		c.Mode = OfflineMode
	}
}

// validate host:port format address
func validateAddr(addr string) error {
	_, _, err := net.SplitHostPort(addr)
	return errors.Trace(err)
}
