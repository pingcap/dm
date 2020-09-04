// Copyright 2020 PingCAP, Inc.
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

package main

import (
	"flag"
	"time"
)

// config is used to run chaos tests.
type config struct {
	*flag.FlagSet `toml:"-" yaml:"-" json:"-"`

	ConfigDir  string        `toml:"config-dir" yaml:"config-dir" json:"config-dir"`
	MasterAddr string        `toml:"master-addr" yaml:"master-addr" json:"master-addr"`
	Duration   time.Duration `toml:"duration" yaml:"duration" json:"duration"`
}

// newConfig creates a config for this chaos testing suite.
func newConfig() *config {
	cfg := &config{}
	cfg.FlagSet = flag.NewFlagSet("chaos-case", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.StringVar(&cfg.ConfigDir, "config-dir", "", "path of the source and task config files")
	fs.StringVar(&cfg.MasterAddr, "master-addr", "", "address of dm-master")
	fs.DurationVar(&cfg.Duration, "duration", 20*time.Minute, "duration of cases running")

	return cfg
}

// parse parses flag definitions from the argument list.
func (c *config) parse(args []string) error {
	err := c.FlagSet.Parse(args)
	if err != nil {
		return err
	}
	return nil
}
