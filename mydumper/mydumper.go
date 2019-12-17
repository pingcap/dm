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

package mydumper

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/unit"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/siddontang/go/sync2"
	"go.uber.org/zap"
)

// Mydumper is a simple wrapper for mydumper binary
type Mydumper struct {
	cfg *config.SubTaskConfig

	logger log.Logger

	args   []string
	closed sync2.AtomicBool
}

// NewMydumper creates a new Mydumper
func NewMydumper(cfg *config.SubTaskConfig) *Mydumper {
	m := &Mydumper{
		cfg:    cfg,
		logger: log.With(zap.String("task", cfg.Name), zap.String("unit", "dump")),
	}
	return m
}

// Init implements Unit.Init
func (m *Mydumper) Init(ctx context.Context) error {
	var err error
	m.args, err = m.constructArgs()
	return err
}

// Process implements Unit.Process
func (m *Mydumper) Process(ctx context.Context, pr chan pb.ProcessResult) {
	mydumperExitWithErrorCounter.WithLabelValues(m.cfg.Name).Add(0)

	failpoint.Inject("dumpUnitProcessWithError", func(val failpoint.Value) {
		m.logger.Info("dump unit runs with injected error", zap.String("failpoint", "dumpUnitProcessWithError"), zap.Reflect("error", val))
		msg, ok := val.(string)
		if !ok {
			msg = "unknown process error"
		}
		pr <- pb.ProcessResult{
			IsCanceled: false,
			Errors:     []*pb.ProcessError{unit.NewProcessError(pb.ErrorType_UnknownError, errors.New(msg))},
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
	output, err := m.spawn(ctx)

	if err != nil {
		mydumperExitWithErrorCounter.WithLabelValues(m.cfg.Name).Inc()
		errs = append(errs, unit.NewProcessError(pb.ErrorType_UnknownError, fmt.Errorf("%s. %s", err.Error(), output)))
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

var mydumperLogRegexp = regexp.MustCompile(
	`^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} \[(DEBUG|INFO|WARNING|ERROR)\] - `,
)

func (m *Mydumper) spawn(ctx context.Context) ([]byte, error) {
	var (
		stdout bytes.Buffer
		stderr bytes.Buffer
	)
	cmd := exec.CommandContext(ctx, m.cfg.MydumperPath, m.args...)
	cmd.Stdout = &stdout
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return nil, terror.ErrDumpUnitRuntime.Delegate(err)
	}
	if err = cmd.Start(); err != nil {
		return nil, terror.ErrDumpUnitRuntime.Delegate(err)
	}

	// Read the stderr from mydumper, which contained the logs.
	// mydumper's logs are all in the form
	//
	// 2016-01-02 15:04:05 [DEBUG] - actual message
	//
	// so we parse all these lines and translate into our own logs.
	scanner := bufio.NewScanner(stderrPipe)
	for scanner.Scan() {
		line := scanner.Bytes()
		if loc := mydumperLogRegexp.FindSubmatchIndex(line); len(loc) == 4 {
			level := string(line[loc[2]:loc[3]])
			msg := line[loc[1]:]
			switch level {
			case "DEBUG":
				m.logger.Debug(string(msg))
				continue
			case "INFO":
				m.logger.Info(string(msg))
				continue
			case "WARNING":
				m.logger.Warn(string(msg))
				continue
			case "ERROR":
				m.logger.Error(string(msg))
				continue
			}
		}
		stderr.Write(line)
		stderr.WriteByte('\n')
	}
	if err = scanner.Err(); err != nil {
		stdout.Write(stderr.Bytes())
		return stdout.Bytes(), terror.ErrDumpUnitRuntime.Delegate(err)
	}

	err = cmd.Wait()
	stdout.Write(stderr.Bytes())
	return stdout.Bytes(), terror.ErrDumpUnitRuntime.Delegate(err)
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
		m.logger.Warn("try to pause, but already closed")
		return
	}
	// do nothing, external will cancel the command (if running)
}

// Resume implements Unit.Resume
func (m *Mydumper) Resume(ctx context.Context, pr chan pb.ProcessResult) {
	if m.closed.Get() {
		m.logger.Warn("try to resume, but already closed")
		return
	}
	// just call Process
	m.Process(ctx, pr)
}

// Update implements Unit.Update
func (m *Mydumper) Update(cfg *config.SubTaskConfig) error {
	// not support update configuration now
	return nil
}

// Status implements Unit.Status
func (m *Mydumper) Status() interface{} {
	// NOTE: try to add some status, like dumped file count
	return &pb.DumpStatus{}
}

// Error implements Unit.Error
func (m *Mydumper) Error() interface{} {
	return &pb.DumpError{}
}

// Type implements Unit.Type
func (m *Mydumper) Type() pb.UnitType {
	return pb.UnitType_Dump
}

// IsFreshTask implements Unit.IsFreshTask
func (m *Mydumper) IsFreshTask(ctx context.Context) (bool, error) {
	return true, nil
}

// constructArgs constructs arguments for exec.Command
func (m *Mydumper) constructArgs() ([]string, error) {
	cfg := m.cfg
	db := cfg.From

	ret := []string{
		"--host",
		db.Host,
		"--port",
		strconv.Itoa(db.Port),
		"--user",
		db.User,
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
		ret = append(ret, ParseArgLikeBash(extraArgs)...)
	}
	if needToGenerateDoTables(extraArgs) {
		m.logger.Info("Tables needed to dump are not given, now we will start to generate table list that mydumper needs to dump through black-white list from given fromDB")
		doTables, err := fetchMyDumperDoTables(cfg)
		if err != nil {
			return nil, err
		}
		ret = append(ret, "--tables-list", doTables)
	}

	m.logger.Info("create mydumper", zap.Strings("argument", ret))

	ret = append(ret, "--password", db.Password)
	return ret, nil
}

// logArgs constructs arguments for log from SubTaskConfig
func (m *Mydumper) logArgs(cfg *config.SubTaskConfig) []string {
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
