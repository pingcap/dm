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
	"sync"

	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go/sync2"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/log"
)

// Pipe is the littlest process unit in syncer
type Pipe interface {
	// Name is the pipe's name
	Name() string

	// Init initializes the pipe, iFunc also do some initialize job.
	Init(cfg *config.SubTaskConfig, iFunc func() error) error

	// Update updates the pipe's config
	Update(*config.SubTaskConfig) error

	// Input receives data
	Input() chan *PipeData

	// Output exports data already be processed
	Output() chan *PipeData

	// Run starts the pipe's process
	Run()

	// Close closes the pipe
	Close()

	// Wait waits all the data in the pipe is processed
	Wait()

	// SetErrorChan set the error channel, send error to this channel when meet error
	SetErrorChan(chan error)

	// SetResolveFunc set the resolveFunc, which used when data is resolved, used in the last pipe, or data is skiped
	SetResolveFunc(func())
}

// PipeData is the data processed in pipe
type PipeData struct {
	binlogEvent *replication.Event

	tp             opType
	sourceSchema   string
	sourceTable    string
	targetSchema   string
	targetTable    string
	sqls           []string
	args           [][]interface{}
	keys           [][]string
	retry          bool
	lastPos        mysql.Position
	currentPos     mysql.Position // exactly binlog position of current SQL
	currentGTIDSet gtid.Set
	ddls           []string
	traceID        string
	traceGID       string

	ddlExecItem *DDLExecItem

	// may have other data needed
	additionalData interface{}
}

// Pipeline contains many pipes, and resolve data by every pipe.
type Pipeline struct {
	pipes []Pipe

	closeCh  chan interface{}
	isClosed sync2.AtomicBool

	errCh chan error

	wg     sync.WaitGroup
	dataWg sync.WaitGroup
}

// NewPipeline returns a new pipeline
func NewPipeline() *Pipeline {
	return &Pipeline{
		pipes: make([]Pipe, 0, 5),
	}
}

// AddPipe adds a pipe to this pipeline
func (p *Pipeline) AddPipe(pipe Pipe) {
	if len(p.pipes) == 0 {
		p.pipes = []Pipe{pipe}
		return
	}

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
		p.dataWg.Add(1)
		return nil
	case <-p.closeCh:
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
	p.dataWg.Wait()

	for _, pipe := range p.pipes {
		pipe.Wait()
	}
}

// Start starts the pipeline's process, run all pipes
func (p *Pipeline) Start() {
	log.Info("start pipeline")

	p.closeCh = make(chan interface{})
	p.errCh = make(chan error, 10)

	for _, pipe := range p.pipes {
		pipe.SetErrorChan(p.errCh)
		pipe.SetResolveFunc(func() {
			p.dataWg.Done()
		})
		pipe.Run()
	}

	for i := 0; i < len(p.pipes)-1; i++ {
		if i+1 < len(p.pipes) {
			p.wg.Add(1)
			go p.link(p.pipes[i].Name(), p.pipes[i+1].Name(), p.pipes[i].Output(), p.pipes[i+1].Input())
		}
	}

	p.isClosed.Set(false)
}

func (p *Pipeline) link(pipeName1, pipeName2 string, output, input chan *PipeData) {
	defer p.wg.Done()

	select {
	case data := <-output:
		select {
		case input <- data:
		case <-p.closeCh:
			log.Infof("pipeline closed, will stop link %s-%s, data %v is not processed", pipeName1, pipeName2, data)
			return
		}
	case <-p.closeCh:
		return
	}
}

// Close closes the pipeline
func (p *Pipeline) Close() {
	log.Info("close pipeline")

	p.isClosed.Set(true)

	close(p.closeCh)
	p.wg.Wait()

	for _, pipe := range p.pipes {
		pipe.Close()
	}

	close(p.errCh)
}

// Errors returns a channel for receiving this pipeline's error
func (p *Pipeline) Errors() chan error {
	return p.errCh
}
