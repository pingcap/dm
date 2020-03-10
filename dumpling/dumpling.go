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

package dumpling

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/unit"
	"github.com/pingcap/dm/pkg/log"

	"github.com/pingcap/dumpling/v4/export"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/siddontang/go/sync2"
	"go.uber.org/zap"
)

// Dumpling dumps full data from a MySQL-compatible database
type Dumpling struct {
	cfg *config.SubTaskConfig

	logger log.Logger

	dumpConfig *export.Config
	closed     sync2.AtomicBool
}

// NewDumpling creates a new Dumpling
func NewDumpling(cfg *config.SubTaskConfig) *Dumpling {
	m := &Dumpling{
		cfg:    cfg,
		logger: log.With(zap.String("task", cfg.Name), zap.String("unit", "dump")),
	}
	return m
}

// Init implements Unit.Init
func (m *Dumpling) Init(ctx context.Context) error {
	var err error
	m.dumpConfig, err = m.constructArgs()
	return err
}

// Process implements Unit.Process
func (m *Dumpling) Process(ctx context.Context, pr chan pb.ProcessResult) {
	dumplingExitWithErrorCounter.WithLabelValues(m.cfg.Name).Add(0)

	failpoint.Inject("dumpUnitProcessWithError", func(val failpoint.Value) {
		m.logger.Info("dump unit runs with injected error", zap.String("failpoint", "dumpUnitProcessWithError"), zap.Reflect("error", val))
		msg, ok := val.(string)
		if !ok {
			msg = "unknown process error"
		}
		pr <- pb.ProcessResult{
			IsCanceled: false,
			Errors:     []*pb.ProcessError{unit.NewProcessError(errors.New(msg))},
		}
		failpoint.Return()
	})

	begin := time.Now()
	errs := make([]*pb.ProcessError, 0, 1)
	isCanceled := false

	failpoint.Inject("dumpUnitProcessForever", func() {
		m.logger.Info("dump unit runs forever", zap.String("failpoint", "dumpUnitProcessForever"))
		<-ctx.Done()
		failpoint.Return()
	})

	// NOTE: remove output dir before start dumping
	// every time re-dump, loader should re-prepare
	err := os.RemoveAll(m.cfg.Dir)
	if err != nil {
		m.logger.Error("fail to remove output directory", zap.String("directory", m.cfg.Dir), log.ShortError(err))
	}

	// Cmd cannot be reused, so we create a new cmd when begin processing
	err = export.Dump(m.dumpConfig)

	if err != nil {
		dumplingExitWithErrorCounter.WithLabelValues(m.cfg.Name).Inc()
		errs = append(errs, unit.NewProcessError(err))
	} else {
		select {
		case <-ctx.Done():
			isCanceled = true
		default:
		}
	}

	m.logger.Info("dump data finished", zap.Duration("cost time", time.Since(begin)))

	pr <- pb.ProcessResult{
		IsCanceled: isCanceled,
		Errors:     errs,
	}
}

// Close implements Unit.Close
func (m *Dumpling) Close() {
	if m.closed.Get() {
		return
	}
	// do nothing, external will cancel the command (if running)
	m.closed.Set(true)
}

// Pause implements Unit.Pause
func (m *Dumpling) Pause() {
	if m.closed.Get() {
		m.logger.Warn("try to pause, but already closed")
		return
	}
	// do nothing, external will cancel the command (if running)
}

// Resume implements Unit.Resume
func (m *Dumpling) Resume(ctx context.Context, pr chan pb.ProcessResult) {
	if m.closed.Get() {
		m.logger.Warn("try to resume, but already closed")
		return
	}
	// just call Process
	m.Process(ctx, pr)
}

// Update implements Unit.Update
func (m *Dumpling) Update(cfg *config.SubTaskConfig) error {
	// not support update configuration now
	return nil
}

// Status implements Unit.Status
func (m *Dumpling) Status() interface{} {
	// NOTE: try to add some status, like dumped file count
	return &pb.DumpStatus{}
}

// Error implements Unit.Error
func (m *Dumpling) Error() interface{} {
	return &pb.DumpError{}
}

// Type implements Unit.Type
func (m *Dumpling) Type() pb.UnitType {
	return pb.UnitType_Dump
}

// IsFreshTask implements Unit.IsFreshTask
func (m *Dumpling) IsFreshTask(ctx context.Context) (bool, error) {
	return true, nil
}

// constructArgs constructs arguments for exec.Command
func (m *Dumpling) constructArgs() (*export.Config, error) {
	cfg := m.cfg
	db := cfg.From

	dumpConfig := export.DefaultConfig()
	dumpConfig.Host = db.Host
	dumpConfig.Port = db.Port
	dumpConfig.User = db.User
	dumpConfig.Password = db.Password
	dumpConfig.OutputDirPath = cfg.Dir // use LoaderConfig.Dir as output dir
	dumpConfig.BlackWhiteList = export.BWListConf{
		Mode: export.MySQLReplicationMode,
		Rules: &export.MySQLReplicationConf{
			Rules:         cfg.BWList,
			CaseSensitive: cfg.CaseSensitive,
		},
	}

	var ret []string
	// TODO: support log relative configs
	ret = append(ret, m.logArgs(cfg)...)

	if cfg.Threads > 0 {
		dumpConfig.Threads = cfg.Threads
	}
	if cfg.ChunkFilesize > 0 {
		dumpConfig.FileSize = uint64(cfg.ChunkFilesize)
	}

	if cfg.SkipTzUTC {
		// TODO: support skip-tz-utc
		ret = append(ret, "--skip-tz-utc")
	}

	extraArgs := strings.Fields(cfg.ExtraArgs)
	if len(extraArgs) > 0 {
		ret = append(ret, ParseArgLikeBash(extraArgs)...)
	}

	// TODO: support String for export.Config
	// m.logger.Info("create dumpling", zap.Stringer("config", dumpConfig))
	if len(ret) > 0 {
		m.logger.Warn("meeting some unsupported arguments", zap.Strings("argument", ret))
	}

	return dumpConfig, nil
}

// logArgs constructs arguments for log from SubTaskConfig
func (m *Dumpling) logArgs(cfg *config.SubTaskConfig) []string {
	args := make([]string, 0, 4)
	if len(cfg.LogFile) > 0 {
		// for writing mydumper output into stderr (fixme: won't work on Windows, if anyone cares)
		args = append(args, "--logfile", "/dev/stderr")
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
