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

package mydumper

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb-enterprise-tools/dm/config"
	"github.com/pingcap/tidb-enterprise-tools/dm/pb"
	"github.com/pingcap/tidb-enterprise-tools/dm/unit"
	"github.com/siddontang/go/sync2"
	"golang.org/x/net/context"
)

// Mydumper is a simple wrapper for mydumper binary
type Mydumper struct {
	cfg *config.SubTaskConfig

	args   []string
	closed sync2.AtomicBool
}

// NewMydumper creates a new Mydumper
func NewMydumper(cfg *config.SubTaskConfig) *Mydumper {
	m := &Mydumper{
		cfg: cfg,
	}
	m.args = m.constructArgs(cfg)
	return m
}

// Init implements Unit.Init
func (m *Mydumper) Init() error {
	return nil // always return nil
}

// Process implements Unit.Process
func (m *Mydumper) Process(ctx context.Context, pr chan pb.ProcessResult) {
	errs := make([]*pb.ProcessError, 0, 1)
	isCanceled := false

	// NOTE: remove output dir before start dumping
	// every time re-dump, loader should re-prepare
	err := os.RemoveAll(m.cfg.Dir)
	if err != nil {
		log.Error("[mydumper] remove output dir %s fail %v", m.cfg.Dir, err)
	}

	// Cmd cannot be reused, so we create a new cmd when begin processing
	cmd := exec.CommandContext(ctx, m.cfg.MydumperPath, m.args...)
	log.Infof("[mydumper] starting mydumper using args %v", cmd.Args)
	output, err := cmd.CombinedOutput()

	if err != nil {
		errs = append(errs, unit.NewProcessError(pb.ErrorType_UnknownError, fmt.Sprintf("%s. %s", err.Error(), output)))
	} else {
		select {
		case <-ctx.Done():
			isCanceled = true
		default:
		}
	}

	pr <- pb.ProcessResult{
		IsCanceled: isCanceled,
		Errors:     errs,
	}
}

// Close implements Unit.Close
func (m *Mydumper) Close() {
	if m.closed.Get() {
		return
	}
	// do nothing, external will cancel the command (if running)
	m.closed.Set(true)
}

// Pause implements Unit.Pause
func (m *Mydumper) Pause() {
	if m.closed.Get() {
		log.Warn("[mydumper] try to pause, but already closed")
		return
	}
	// do nothing, external will cancel the command (if running)
}

// Resume implements Unit.Resume
func (m *Mydumper) Resume(ctx context.Context, pr chan pb.ProcessResult) {
	if m.closed.Get() {
		log.Warn("[mydumper] try to resume, but already closed")
		return
	}
	// just call Process
	m.Process(ctx, pr)
}

// Status implements Unit.Status
func (m *Mydumper) Status() interface{} {
	// NOTE: try to add some status, like dumped file count
	return &pb.DumpStatus{}
}

// Type implements Unit.Type
func (m *Mydumper) Type() pb.UnitType {
	return pb.UnitType_Dump
}

// IsFreshTask implements Unit.IsFreshTask
func (m *Mydumper) IsFreshTask() (bool, error) {
	return true, nil
}

// constructArgs constructs arguments for exec.Command
func (m *Mydumper) constructArgs(cfg *config.SubTaskConfig) []string {
	db := cfg.From
	ret := []string{
		"--host",
		db.Host,
		"--port",
		strconv.Itoa(db.Port),
		"--user",
		db.User,
		"--password",
		db.Password,
		"--outputdir",
		cfg.Dir, // use LoaderConfig.Dir as --outputdir
	}

	ret = append(ret, m.logArgs(cfg)...)

	if cfg.Threads > 0 {
		ret = append(ret, "--threads", strconv.Itoa(cfg.Threads))
	}
	if cfg.ChunkFilesize > 0 {
		ret = append(ret, "--chunk-filesize", strconv.FormatInt(cfg.ChunkFilesize, 10))
	}
	if cfg.SkipTzUTC {
		ret = append(ret, "--skip-tz-utc")
	}
	extraArgs := strings.Fields(cfg.ExtraArgs)
	if len(extraArgs) > 0 {
		ret = append(ret, extraArgs...)
	}
	return ret
}

// logArgs constructs arguments for log from SubTaskConfig
func (m *Mydumper) logArgs(cfg *config.SubTaskConfig) []string {
	args := make([]string, 0, 4)
	if len(cfg.LogFile) > 0 {
		args = append(args, "--logfile", cfg.LogFile)
	}
	switch strings.ToLower(cfg.LogLevel) {
	case "fatal", "error":
		args = append(args, "--verbose", "1")
	case "warn", "warning":
		args = append(args, "--verbose", "2")
	case "info", "debug":
		args = append(args, "--verbose", "3")
	}
	return args
}
