package main

import (
	"flag"

	"github.com/pingcap/errors"
)

// config is the configuration used by this binlog-event-blackhole.
type config struct {
	*flag.FlagSet

	logLevel string
	logFile  string

	addr       string
	username   string
	password   string
	serverID   int
	binlogName string
	binlogPos  int
}

// newConfig creates a new config instance.
func newConfig() *config {
	cfg := &config{
		FlagSet: flag.NewFlagSet("binlog-event-blackhole", flag.ContinueOnError),
	}
	fs := cfg.FlagSet

	fs.StringVar(&cfg.logLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.logFile, "log-file", "", "log file path")

	fs.StringVar(&cfg.addr, "addr", "", "master's address")
	fs.StringVar(&cfg.username, "u", "", "master's username")
	fs.StringVar(&cfg.password, "p", "", "password for `username`")
	fs.IntVar(&cfg.serverID, "server-id", 0, "slave's server-id")
	fs.StringVar(&cfg.binlogName, "binlog-name", "", "startup binlog filename")
	fs.IntVar(&cfg.binlogPos, "binlog-pos", 4, "startup binlog position")

	return cfg
}

// parse parses flag definitions from the argument list.
func (c *config) parse(args []string) error {
	err := c.FlagSet.Parse(args)
	return errors.Trace(err)
}
