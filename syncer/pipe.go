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

package syncer

import (
	"context"
	"sync"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go/sync2"
)

// Pipe is the littlest process unit in syncer
type Pipe interface {
	// Name is the pipe's name
	Name() string

	// Init initial the pipe
	Init(*config.SubTaskConfig, func() error) error

	// Update updates the pipe's config
	Update(*config.SubTaskConfig)

	// Input receives data
	Input() chan *PipeData

	// Run start the pipe's process
	Run()

	// Close closes the pipe
	Close()

	// SetNextPipe sets the pipe's next pipe, pipe will send data to the next pipe after process
	SetNextPipe(Pipe)

	// Report reports the pipe's status to pipeline
	Report()

	// Wait waits all the data in the pipe is processed
	Wait()

	// SetErrorChan set the error channel, send error to this channel when meet error
	SetErrorChan(chan error)

	// SetResolveFunc set the resolveFunc, which used when data is resolved
	SetResolveFunc(func())
}

// PipeData is the data processed in pipe
type PipeData struct {
	binlogEvent *replication.Event

	tp           opType
	sourceSchema string
	sourceTable  string
	targetSchema string
	targetTable  string
	sqls         []string
	args         [][]interface{}
	keys         [][]string
	retry        bool
	pos          mysql.Position
	currentPos   mysql.Position // exactly binlog position of current SQL
	gtidSet      gtid.Set
	ddls         []string
	traceID      string
	traceGID     string

	ddlExecItem *DDLExecItem
}

// Pipeline ...
type Pipeline struct {
	ctx    context.Context
	cancel context.CancelFunc

	pipes []Pipe

	isClosed sync2.AtomicBool

	errCh    chan error
	errChLen int

	wg sync.WaitGroup
}

// NewPipeline returns a new pipeline
func NewPipeline(cfg *config.SubTaskConfig) *Pipeline {
	return &Pipeline{
		errChLen: cfg.WorkerCount + 1,
		pipes:    make([]Pipe, 0, 5),
	}
}

// AddPipe adds a pipe to this pipeline
func (p *Pipeline) AddPipe(pipe Pipe) {
	if len(p.pipes) == 0 {
		p.pipes = []Pipe{pipe}
		return
	}

	lastPipe := p.pipes[len(p.pipes)-1]
	lastPipe.SetNextPipe(pipe)

	p.pipes = append(p.pipes, pipe)
}

// Input receives data
func (p *Pipeline) Input(data *PipeData) error {
	if len(p.pipes) == 0 {
		return errors.New("no pipes in this pipeline")
	}

	if p.isClosed.Get() {
		return errors.New("pipeline is closed")
	}

	select {
	case p.pipes[0].Input() <- data:
		p.wg.Add(1)
		return nil
	case <-p.ctx.Done():
		return errors.New("pipeline's context is done")
	}
}

// Flush sends a PipeData with flush type, all pipes should wait all received data is processed
func (p *Pipeline) Flush() {
	p.Input(&PipeData{tp: flush})
	p.Wait()
}

// Wait waits all pipes have no data to process
func (p *Pipeline) Wait() {
	p.wg.Wait()

	for _, pipe := range p.pipes {
		pipe.Wait()
	}
}

// Start starts the pipeline's process, run all pipes
func (p *Pipeline) Start() {
	log.Info("start pipeline")
	p.ctx, p.cancel = context.WithCancel(context.Background())
	p.errCh = make(chan error, p.errChLen)

	for _, pipe := range p.pipes {
		pipe.SetErrorChan(p.errCh)
		pipe.SetResolveFunc(func() {
			p.wg.Done()
		})
		pipe.Run()
	}

	p.isClosed.Set(false)
}

// Close closes the pipeline
func (p *Pipeline) Close() {
	log.Info("close pipeline")
	p.cancel()
	p.isClosed.Set(true)

	for _, pipe := range p.pipes {
		pipe.Close()
	}

	close(p.errCh)
}

// Errors returns a channel for receive this pipeline's error
func (p *Pipeline) Errors() chan error {
	return p.errCh
}
