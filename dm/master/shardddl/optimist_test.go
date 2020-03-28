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
	"fmt"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	tiddl "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/mock"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/shardddl/optimism"
	"github.com/pingcap/dm/pkg/utils"
)

type testOptimist struct{}

var _ = Suite(&testOptimist{})

// clear keys in etcd test cluster.
func clearOptimistTestSourceInfoOperation(c *C) {
	c.Assert(optimism.ClearTestInfoOperation(etcdTestCli), IsNil)
}

func createTableInfo(c *C, p *parser.Parser, se sessionctx.Context, tableID int64, sql string) *model.TableInfo {
	node, err := p.ParseOneStmt(sql, "utf8mb4", "utf8mb4_bin")
	if err != nil {
		c.Fatalf("fail to parse stmt, %v", err)
	}
	createStmtNode, ok := node.(*ast.CreateTableStmt)
	if !ok {
		c.Fatalf("%s is not a CREATE TABLE statement", sql)
	}
	info, err := tiddl.MockTableInfo(se, createStmtNode, tableID)
	if err != nil {
		c.Fatalf("fail to create table info, %v", err)
	}
	return info
}

func (t *testOptimist) testOptimistSourceTables(c *C) {
	defer clearOptimistTestSourceInfoOperation(c)

	var (
		logger  = log.L()
		o       = NewOptimist(&logger)
		task    = "task"
		source1 = "mysql-replica-1"
		source2 = "mysql-replica-2"
		st1     = optimism.NewSourceTables(task, source1, map[string]map[string]struct{}{
			"db": {"tbl-1": struct{}{}, "tbl-2": struct{}{}},
		})
		st2 = optimism.NewSourceTables(task, source2, map[string]map[string]struct{}{
			"db": {"tbl-1": struct{}{}, "tbl-2": struct{}{}},
		})
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// CASE 1: start without any previous kv and no etcd operation.
	c.Assert(o.Start(ctx, etcdTestCli), IsNil)
	c.Assert(o.tk.FindTables(task), IsNil)
	o.Close()
	o.Close() // close multiple times.

	// CASE 2: start again without any previous kv.
	c.Assert(o.Start(ctx, etcdTestCli), IsNil)
	c.Assert(o.tk.FindTables(task), IsNil)

	// PUT st1, should find tables.
	_, err := optimism.PutSourceTables(etcdTestCli, st1)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		sts := o.tk.FindTables(task)
		return len(sts) == 1
	}), IsTrue)
	sts := o.tk.FindTables(task)
	c.Assert(sts, HasLen, 1)
	c.Assert(sts[0], DeepEquals, st1)
	o.Close()

	// CASE 3: start again with previous source tables.
	c.Assert(o.Start(ctx, etcdTestCli), IsNil)
	sts = o.tk.FindTables(task)
	c.Assert(sts, HasLen, 1)
	c.Assert(sts[0], DeepEquals, st1)

	// PUT st2, should find more tables.
	_, err = optimism.PutSourceTables(etcdTestCli, st2)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		sts = o.tk.FindTables(task)
		return len(sts) == 2
	}), IsTrue)
	sts = o.tk.FindTables(task)
	c.Assert(sts, HasLen, 2)
	c.Assert(sts[0], DeepEquals, st1)
	c.Assert(sts[1], DeepEquals, st2)
	o.Close()

	// CASE 4: create (not re-start) a new optimist with previous source tables.
	o = NewOptimist(&logger)
	c.Assert(o.Start(ctx, etcdTestCli), IsNil)
	sts = o.tk.FindTables(task)
	c.Assert(sts, HasLen, 2)
	c.Assert(sts[0], DeepEquals, st1)
	c.Assert(sts[1], DeepEquals, st2)

	// DELETE st1, should find less tables.
	_, err = optimism.DeleteSourceTables(etcdTestCli, st1)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		sts = o.tk.FindTables(task)
		return len(sts) == 1
	}), IsTrue)
	sts = o.tk.FindTables(task)
	c.Assert(sts, HasLen, 1)
	c.Assert(sts[0], DeepEquals, st2)
	o.Close()
}

func (t *testOptimist) TestOptimist(c *C) {
	defer clearOptimistTestSourceInfoOperation(c)

	var (
		backOff      = 30
		waitTime     = 100 * time.Millisecond
		watchTimeout = 500 * time.Millisecond
		logger       = log.L()
		o            = NewOptimist(&logger)
		task         = "task-test-optimist"
		source1      = "mysql-replica-1"
		source2      = "mysql-replica-2"
		downSchema   = "foo"
		downTable    = "bar"
		lockID       = fmt.Sprintf("%s-`%s`.`%s`", task, downSchema, downTable)
		st1          = optimism.NewSourceTables(task, source1, map[string]map[string]struct{}{
			"foo": {"bar-1": struct{}{}, "bar-2": struct{}{}},
		})
		p           = parser.New()
		se          = mock.NewContext()
		tblID int64 = 111
		DDLs1       = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		DDLs2       = []string{"ALTER TABLE bar ADD COLUMN c2 INT"}
		ti0         = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1         = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)
		ti2         = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 INT)`)
		i11         = optimism.NewInfo(task, source1, "foo", "bar-1", downSchema, downTable, DDLs1, ti0, ti1)
		i12         = optimism.NewInfo(task, source1, "foo", "bar-2", downSchema, downTable, DDLs1, ti0, ti1)
		i21         = optimism.NewInfo(task, source1, "foo", "bar-1", downSchema, downTable, DDLs2, ti1, ti2)
		i23         = optimism.NewInfo(task, source2, "foo-2", "bar-3", downSchema, downTable,
			[]string{`CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 INT)`}, ti2, ti2)
	)

	// put source tables first.
	_, err := optimism.PutSourceTables(etcdTestCli, st1)
	c.Assert(err, IsNil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// CASE 1: start without any previous shard DDL info.
	c.Assert(o.Start(ctx, etcdTestCli), IsNil)
	c.Assert(o.Locks(), HasLen, 0)
	o.Close()
	o.Close() // close multiple times.

	// CASE 2: start again without any previous shard DDL info.
	c.Assert(o.Start(ctx, etcdTestCli), IsNil)
	c.Assert(o.Locks(), HasLen, 0)

	// PUT i11, will create a lock but not synced.
	rev1, err := optimism.PutInfo(etcdTestCli, i11)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(backOff, waitTime, func() bool {
		return len(o.Locks()) == 1
	}), IsTrue)
	c.Assert(o.Locks(), HasKey, lockID)
	synced, remain := o.Locks()[lockID].IsSynced()
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 1)

	// wait operation for i11 become available.
	opCh := make(chan optimism.Operation, 10)
	errCh := make(chan error, 10)
	ctx2, cancel2 := context.WithTimeout(ctx, watchTimeout)
	optimism.WatchOperationPut(ctx2, etcdTestCli, i11.Task, i11.Source, i11.UpSchema, i11.UpTable, rev1, opCh, errCh)
	cancel2()
	close(opCh)
	close(errCh)
	c.Assert(len(opCh), Equals, 1)
	op11 := <-opCh
	c.Assert(op11.DDLs, DeepEquals, DDLs1)
	c.Assert(op11.ConflictStage, Equals, optimism.ConflictNone)
	c.Assert(len(errCh), Equals, 0)

	// PUT i12, the lock will be synced.
	rev2, err := optimism.PutInfo(etcdTestCli, i12)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(backOff, waitTime, func() bool {
		synced, _ = o.Locks()[lockID].IsSynced()
		return synced
	}), IsTrue)

	// wait operation for i12 become available.
	opCh = make(chan optimism.Operation, 10)
	errCh = make(chan error, 10)
	ctx2, cancel2 = context.WithTimeout(ctx, watchTimeout)
	optimism.WatchOperationPut(ctx2, etcdTestCli, i12.Task, i12.Source, i12.UpSchema, i12.UpTable, rev2, opCh, errCh)
	cancel2()
	close(opCh)
	close(errCh)
	c.Assert(len(opCh), Equals, 1)
	op12 := <-opCh
	c.Assert(op12.DDLs, DeepEquals, DDLs1)
	c.Assert(op12.ConflictStage, Equals, optimism.ConflictNone)
	c.Assert(len(errCh), Equals, 0)

	// mark op11 as done.
	op11c := op11
	op11c.Done = true
	_, putted, err := optimism.PutOperation(etcdTestCli, false, op11c)
	c.Assert(err, IsNil)
	c.Assert(putted, IsTrue)
	c.Assert(utils.WaitSomething(backOff, waitTime, func() bool {
		lock := o.Locks()[lockID]
		if lock == nil {
			return false
		}
		return lock.IsDone(op11.Source, op11.UpSchema, op11.UpTable)
	}), IsTrue)
	c.Assert(o.Locks()[lockID].IsDone(op12.Source, op12.UpSchema, op12.UpTable), IsFalse)

	// mark op12 as done, the lock should be resolved.
	op12c := op12
	op12c.Done = true
	_, putted, err = optimism.PutOperation(etcdTestCli, false, op12c)
	c.Assert(err, IsNil)
	c.Assert(putted, IsTrue)
	c.Assert(utils.WaitSomething(backOff, waitTime, func() bool {
		_, ok := o.Locks()[lockID]
		return !ok
	}), IsTrue)
	c.Assert(o.Locks(), HasLen, 0)

	// no shard DDL info or lock operation exists.
	ifm, _, err := optimism.GetAllInfo(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(ifm, HasLen, 0)
	opm, _, err := optimism.GetAllOperations(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(opm, HasLen, 0)

	// put another table info.
	_, err = optimism.PutInfo(etcdTestCli, i21)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(backOff, waitTime, func() bool {
		return len(o.Locks()) == 1
	}), IsTrue)
	c.Assert(o.Locks(), HasKey, lockID)
	synced, remain = o.Locks()[lockID].IsSynced()
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 1)

	// put table info for a new table (to simulate `CREATE TABLE`).
	_, err = optimism.PutInfo(etcdTestCli, i23)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(backOff, waitTime, func() bool {
		ready := o.Locks()[lockID].Ready()
		return ready[source2][i23.UpSchema][i23.UpTable]
	}), IsTrue)
	synced, remain = o.Locks()[lockID].IsSynced()
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 1)
	sts := o.tk.FindTables(task)
	c.Assert(sts, HasLen, 2)
	c.Assert(sts[1].Source, Equals, source2)
	c.Assert(sts[1].Tables, HasKey, i23.UpSchema)
	c.Assert(sts[1].Tables[i23.UpSchema], HasKey, i23.UpTable)

	// delete i21 for a table (to simulate `DROP TABLE`).
	_, err = optimism.DeleteInfosOperations(etcdTestCli, []optimism.Info{i21}, nil)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(backOff, waitTime, func() bool {
		ready := o.Locks()[lockID].Ready()
		return len(ready) == 2
	}), IsTrue)
	sts = o.tk.FindTables(task)
	c.Assert(sts, HasLen, 2)
	c.Assert(sts[0].Source, Equals, source1)
	c.Assert(sts[0].Tables, HasLen, 1)
	c.Assert(sts[0].Tables[i12.UpSchema], HasKey, i12.UpTable)
	c.Assert(sts[1].Source, Equals, source2)
	c.Assert(sts[1].Tables, HasLen, 1)
	c.Assert(sts[1].Tables[i23.UpSchema], HasKey, i23.UpTable)
}
