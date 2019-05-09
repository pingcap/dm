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
	"context"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/dm/config"
	"github.com/siddontang/go-mysql/replication"
)
// Pipe ...
type Pipe interface {
	// Name is the pipe's name
	Name() string

	// Init ...
	Init(*config.SubTaskConfig, func() error) error

	// Update ...
	Update()

	// Input data
	Input() chan *PipeData

	// Process ...
	Process()

	// SetNextPipe ...
	SetNextPipe(Pipe)

	// Output ...
	//Output(*PipeData)

	// Report ...
	Report()

	// Error ...
	//SetErrorChan(chan error)
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
	key          string
	retry        bool
	pos          mysql.Position
	currentPos   mysql.Position // exactly binlog position of current SQL
	gtidSet      gtid.Set
	ddlExecItem  *DDLExecItem
	ddls         []string
	traceID      string
	traceGID     string
}

// Pipeline ...
type Pipeline struct {
	sync.RWMutex

	// errCh chan error

	ctx context.Context

	pipes []Pipe
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
	if len(p.pipes) == 0 {
		log.Warn("no pipes in this pipeline")
		return
	}

	select {
	case p.pipes[0].Input() <- data:
	case <-p.ctx.Done():
		log.Info("context is done")
		return
	}
}

/*
func (p *Pipeline) Error() (chan error) {
	select {

	}
}
*/
