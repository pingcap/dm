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

package shardddl

import (
	"context"

	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/shardddl/pessimism"
)

// Pessimist used to coordinate the shard DDL migration in pessimism mode.
type Pessimist struct {
	logger log.Logger
	cli    *clientv3.Client
	task   string
	source string
}

// NewPessimist creates a new Pessimist instance.
func NewPessimist(pLogger *log.Logger, cli *clientv3.Client, task, source string) *Pessimist {
	return &Pessimist{
		logger: pLogger.WithFields(zap.String("component", "shard DDL pessimist")),
		cli:    cli,
		task:   task,
		source: source,
	}
}

// ConstructInfo constructs a shard DDL info.
func (p *Pessimist) ConstructInfo(schema, table string, DDLs []string) pessimism.Info {
	return pessimism.NewInfo(p.task, p.source, schema, table, DDLs)
}

// PutInfo puts the shard DDL info into etcd and returns the revision.
func (p *Pessimist) PutInfo(info pessimism.Info) (int64, error) {
	return pessimism.PutInfo(p.cli, info)
}

// GetOperation gets the shard DDL lock operation relative to the shard DDL info.
func (p *Pessimist) GetOperation(ctx context.Context, info pessimism.Info, rev int64) (pessimism.Operation, error) {
	ctx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()

	ch := make(chan pessimism.Operation, 1)
	go pessimism.WatchOperationPut(ctx2, p.cli, info.Task, info.Source, rev, ch)

	select {
	case op := <-ch:
		return op, nil
	case <-ctx.Done():
		return pessimism.Operation{}, ctx.Err()
	}
}

// DoneOperationDeleteInfo marks the shard DDL lock operation as done and delete the shard DDL info.
func (p *Pessimist) DoneOperationDeleteInfo(op pessimism.Operation, info pessimism.Info) error {
	op.Done = true // mark the operation as `done`.
	_, err := pessimism.PutOperationDeleteInfo(p.cli, op, info)
	return err
}
