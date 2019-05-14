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
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/log"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go/sync2"
)

// Pipe ...
type Pipe interface {
	// Name is the pipe's name
	Name() string

	// Init ...
	Init(*config.SubTaskConfig, func() error) error

	// Update ...
	Update(*config.SubTaskConfig)

	// Input data
	Input() chan *PipeData

	// Run ...
	Run()

	// Close ...
	Close()

	// SetNextPipe ...
	SetNextPipe(Pipe)

	// Output ...
	//Output(*PipeData)

	// Report ...
	Report()

	// Wait ...
	Wait()

	// SetErrorChan ...
	SetErrorChan(chan *pb.ProcessError)
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
	ddlExecItem  *DDLExecItem
	ddls         []string
	traceID      string
	traceGID     string

	ddlInfo *shardingDDLInfo
}

// Pipeline ...
type Pipeline struct {
	sync.RWMutex

	// errCh chan error

	ctx    context.Context
	cancel context.CancelFunc

	pipes []Pipe

	isClosed sync2.AtomicBool

	errCh chan *pb.ProcessError
	errChLen int
}

func NewPipeline(cfg *config.SubTaskConfig) *Pipeline {
	//ctx, cancel := context.WithCancel(context.Background())
	return &Pipeline{
		//ctx:    ctx,
		//cancel: cancel,
		errChLen: cfg.WorkerCount+1,
		pipes:  make([]Pipe, 0, 5),
		//errCh:  make(chan *pb.ProcessError, cfg.WorkerCount+1),
	}
}

// AddPipe adds a pipe to this pipeline
func (p *Pipeline) AddPipe(pipe Pipe) {
	p.Lock()
	defer p.Unlock()

	if len(p.pipes) == 0 {
		p.pipes = []Pipe{pipe}
		return
	}

	lastPipe := p.pipes[len(p.pipes)-1]
	lastPipe.SetNextPipe(pipe)

	//pipe.SetErrorChan(p.errCh)

	p.pipes = append(p.pipes, pipe)
}

// Input ...
func (p *Pipeline) Input(data *PipeData) {
	log.Infof("pipeline get pipedata %v", data)
	if len(p.pipes) == 0 {
		log.Warn("no pipes in this pipeline")
		return
	}

	if p.isClosed.Get() {
		log.Warn("pipeline is closed")
		return
	}

	select {
	case p.pipes[0].Input() <- data:
		log.Info("send data to first pipe")
	case <-p.ctx.Done():
		log.Info("context is done")
		return
	}
}

func (p *Pipeline) Flush() {
	p.Input(&PipeData{tp: flush})
	p.Wait()
}

// Wait waits all pipes have no data to process
func (p *Pipeline) Wait() {
	for _, pipe := range p.pipes {
		pipe.Wait()
	}
}

func (p *Pipeline) Start() {
	p.ctx, p.cancel = context.WithCancel(context.Background())
	p.errCh =  make(chan *pb.ProcessError, p.errChLen)
	
	for _, pipe := range p.pipes {
		pipe.SetErrorChan(p.errCh)
		pipe.Run()
	}

	p.isClosed.Set(false)
}

func (p *Pipeline) Close() {
	p.cancel()
	p.isClosed.Set(true)

	for _, pipe := range p.pipes {
		pipe.Close()
	}

	close(p.errCh)
}

func (p *Pipeline) Errors() chan *pb.ProcessError {
	return p.errCh
}
