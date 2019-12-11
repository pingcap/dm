package config

import (
	"bytes"
	"github.com/BurntSushi/toml"
	"github.com/pingcap/dm/pkg/binlog"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/tracing"
	"github.com/pingcap/dm/pkg/utils"
	"github.com/pingcap/dm/relay/purger"
	"strings"
)

// WorkerConfig is the configuration for Worker
type WorkerConfig struct {
	LogLevel  string `toml:"log-level" json:"log-level"`
	LogFile   string `toml:"log-file" json:"log-file"`
	LogRotate string `toml:"log-rotate" json:"log-rotate"`

	WorkerAddr string `toml:"worker-addr" json:"worker-addr"`

	EnableGTID  bool   `toml:"enable-gtid" json:"enable-gtid"`
	AutoFixGTID bool   `toml:"auto-fix-gtid" json:"auto-fix-gtid"`
	RelayDir    string `toml:"relay-dir" json:"relay-dir"`
	MetaDir     string `toml:"meta-dir" json:"meta-dir"`
	Flavor      string `toml:"flavor" json:"flavor"`
	Charset     string `toml:"charset" json:"charset"`

	// relay synchronous starting point (if specified)
	RelayBinLogName string `toml:"relay-binlog-name" json:"relay-binlog-name"`
	RelayBinlogGTID string `toml:"relay-binlog-gtid" json:"relay-binlog-gtid"`

	SourceID string   `toml:"source-id" json:"source-id"`
	From     DBConfig `toml:"from" json:"from"`

	// config items for purger
	Purge purger.Config `toml:"purge" json:"purge"`

	// config items for task status checker
	Checker CheckerConfig `toml:"checker" json:"checker"`

	// config items for tracer
	Tracer tracing.Config `toml:"tracer" json:"tracer"`

	// id of the worker on which this task run
	ServerID uint32
}

// Clone clones a config
func (c *WorkerConfig) Clone() *WorkerConfig {
	clone := &WorkerConfig{}
	*clone = *c
	return clone
}

// Toml returns TOML format representation of config
func (c *WorkerConfig) Toml() (string, error) {
	var b bytes.Buffer

	err := toml.NewEncoder(&b).Encode(c)
	if err != nil {
		log.L().Error("fail to marshal config to toml", log.ShortError(err))
	}

	return b.String(), nil
}

// Parse parses flag definitions from the argument list.
func (c *WorkerConfig) Parse(content string) error {
	// Parse first to get config file.
	metaData, err := toml.Decode(content, c)
	if err != nil {
		return terror.ErrWorkerDecodeConfigFromFile.Delegate(err)
	}
	undecoded := metaData.Undecoded()
	if len(undecoded) > 0 {
		var undecodedItems []string
		for _, item := range undecoded {
			undecodedItems = append(undecodedItems, item.String())
		}
		return terror.ErrWorkerUndecodedItemFromFile.Generate(strings.Join(undecodedItems, ","))
	}
	return nil
}

// verify verifies the config
func (c *WorkerConfig) verify() error {
	if len(c.SourceID) == 0 {
		return terror.ErrWorkerNeedSourceID.Generate()
	}
	if len(c.SourceID) > MaxSourceIDLength {
		return terror.ErrWorkerTooLongSourceID.Generate(c.SourceID, MaxSourceIDLength)
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

// DecryptPassword returns a decrypted config replica in config
func (c *WorkerConfig) DecryptPassword() (*WorkerConfig, error) {
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
