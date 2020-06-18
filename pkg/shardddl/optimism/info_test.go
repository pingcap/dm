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

package optimism

import (
	"context"
	"sync"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	tiddl "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/mock"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/integration"
)

var (
	etcdTestCli *clientv3.Client
)

func TestInfo(t *testing.T) {
	mockCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer mockCluster.Terminate(t)

	etcdTestCli = mockCluster.RandClient()

	TestingT(t)
}

// clear keys in etcd test cluster.
func clearTestInfoOperation(c *C) {
	c.Assert(ClearTestInfoOperationSchema(etcdTestCli), IsNil)
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

type testForEtcd struct{}

var _ = Suite(&testForEtcd{})

func (t *testForEtcd) TestInfoJSON(c *C) {
	i1 := NewInfo("test", "mysql-replica-1",
		"db-1", "tbl-1", "db", "tbl", []string{
			"ALTER TABLE tbl ADD COLUMN c1 INT",
			"ALTER TABLE tbl ADD COLUMN c2 INT",
		}, nil, nil)

	j, err := i1.toJSON()
	c.Assert(err, IsNil)
	c.Assert(j, Equals, `{"task":"test","source":"mysql-replica-1","up-schema":"db-1","up-table":"tbl-1","down-schema":"db","down-table":"tbl","ddls":["ALTER TABLE tbl ADD COLUMN c1 INT","ALTER TABLE tbl ADD COLUMN c2 INT"],"table-info-before":null,"table-info-after":null}`)
	c.Assert(j, Equals, i1.String())

	i2, err := infoFromJSON(j)
	c.Assert(err, IsNil)
	c.Assert(i2, DeepEquals, i1)
}

func (t *testForEtcd) TestInfoEtcd(c *C) {
	defer clearTestInfoOperation(c)

	var (
		watchTimeout       = 2 * time.Second
		source1            = "mysql-replica-1"
		source2            = "mysql-replica-2"
		task1              = "task-1"
		task2              = "task-2"
		upSchema           = "foo_1"
		upTable            = "bar_1"
		downSchema         = "foo"
		downTable          = "bar"
		p                  = parser.New()
		se                 = mock.NewContext()
		tblID        int64 = 111
		tblI1              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		tblI2              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)
		tblI3              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 INT)`)
		tblI4              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 INT, c3 INT)`)
		i11                = NewInfo(task1, source1, upSchema, upTable, downSchema, downTable, []string{"ALTER TABLE bar ADD COLUMN c1 INT"}, tblI1, tblI2)
		i12                = NewInfo(task1, source2, upSchema, upTable, downSchema, downTable, []string{"ALTER TABLE bar ADD COLUMN c2 INT"}, tblI2, tblI3)
		i21                = NewInfo(task2, source1, upSchema, upTable, downSchema, downTable, []string{"ALTER TABLE bar ADD COLUMN c3 INT"}, tblI3, tblI4)
	)

	// put the same key twice.
	rev1, err := PutInfo(etcdTestCli, i11)
	c.Assert(err, IsNil)
	rev2, err := PutInfo(etcdTestCli, i11)
	c.Assert(err, IsNil)
	c.Assert(rev2, Greater, rev1)

	// get with only 1 info.
	ifm, rev3, err := GetAllInfo(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(rev3, Equals, rev2)
	c.Assert(ifm, HasLen, 1)
	c.Assert(ifm, HasKey, task1)
	c.Assert(ifm[task1], HasLen, 1)
	c.Assert(ifm[task1][source1], HasLen, 1)
	c.Assert(ifm[task1][source1][upSchema], HasLen, 1)
	c.Assert(ifm[task1][source1][upSchema][upTable], DeepEquals, i11)

	// put another key and get again with 2 info.
	rev4, err := PutInfo(etcdTestCli, i12)
	c.Assert(err, IsNil)
	ifm, _, err = GetAllInfo(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(ifm, HasLen, 1)
	c.Assert(ifm, HasKey, task1)
	c.Assert(ifm[task1], HasLen, 2)
	c.Assert(ifm[task1][source1][upSchema][upTable], DeepEquals, i11)
	c.Assert(ifm[task1][source2][upSchema][upTable], DeepEquals, i12)

	// start the watcher.
	wch := make(chan Info, 10)
	ech := make(chan error, 10)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), watchTimeout)
		defer cancel()
		WatchInfo(ctx, etcdTestCli, rev4+1, wch, ech) // revision+1
		close(wch)                                    // close the chan
		close(ech)
	}()

	// put another key for a different task.
	_, err = PutInfo(etcdTestCli, i21)
	c.Assert(err, IsNil)
	wg.Wait()

	// watch should only get i21.
	c.Assert(len(wch), Equals, 1)
	c.Assert(<-wch, DeepEquals, i21)
	c.Assert(len(ech), Equals, 0)

	// delete i12.
	deleteOp := deleteInfoOp(i12)
	resp, err := etcdTestCli.Txn(context.Background()).Then(deleteOp).Commit()
	c.Assert(err, IsNil)

	// get again.
	ifm, _, err = GetAllInfo(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(ifm, HasLen, 2)
	c.Assert(ifm, HasKey, task1)
	c.Assert(ifm, HasKey, task2)
	c.Assert(ifm[task1], HasLen, 1)
	c.Assert(ifm[task1][source1][upSchema][upTable], DeepEquals, i11)
	c.Assert(ifm[task2], HasLen, 1)
	c.Assert(ifm[task2][source1][upSchema][upTable], DeepEquals, i21)

	// watch the deletion for i12.
	wch = make(chan Info, 10)
	ech = make(chan error, 10)
	ctx, cancel := context.WithTimeout(context.Background(), watchTimeout)
	WatchInfo(ctx, etcdTestCli, resp.Header.Revision, wch, ech)
	cancel()
	close(wch)
	close(ech)
	c.Assert(len(wch), Equals, 1)
	info := <-wch
	i12c := i12
	i12c.IsDeleted = true
	c.Assert(info, DeepEquals, i12c)
	c.Assert(len(ech), Equals, 0)
}
