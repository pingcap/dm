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

package loader

import (
	"context"
	"os"
	"path"
	"time"

	. "github.com/pingcap/check"
	"golang.org/x/sync/errgroup"

	"github.com/pingcap/dm/dm/config"
)

var _ = Suite(&testWorkerSuite{})

type testWorkerSuite struct {
	file      *os.File
	tableInfo *tableInfo
	w         *Worker
}

func (t *testWorkerSuite) SetUpSuite(c *C) {
	cfg := &config.SubTaskConfig{}
	l := NewLoader(cfg)
	l.toDBConns = make([]*DBConn, 1)
	t.w = NewWorker(l, 0)

	file, err := os.Create(path.Join(c.MkDir(), "test.sql"))
	c.Assert(err, IsNil)
	sql := `INSERT INTO t1 VALUES
(10,1,9223372036854775807,123.123,123456789012.1234567890120000000);
INSERT INTO t1 VALUES
(9,1,9223372036854775807,123.123,123456789012.1234567890120000000),
(8,1,9223372036854775807,123.123,123456789012.1234567890120000000);
`
	_, err = file.WriteString(sql)
	c.Assert(err, IsNil)
	t.file = file
	t.tableInfo = &tableInfo{}
}

func (t *testWorkerSuite) TearDownSuite(c *C) {
}

func (t *testWorkerSuite) TestRestoreDataFile(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	// make jobQueue buffer to 2, because we have 2 sql in prepare data.
	// so that jobQueue <- nil in restoreDataFile will blocked.
	t.w.jobQueue = make(chan *dataJob, 2)

	var eg errgroup.Group
	eg.Go(func() error {
		return t.w.restoreDataFile(ctx, t.file.Name(), 0, t.tableInfo)
	})

	eg.Go(func() error {
		// cancel ctx in another goroutine.
		// sleep to wait restoreDataFile blocked
		time.Sleep(time.Second)
		cancel()
		return nil
	})
	c.Assert(eg.Wait(), IsNil)
}

func (t *testWorkerSuite) TestDispatchSQL(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	// make jobQueue full to see whether ctx.cancel worked.
	t.w.jobQueue = make(chan *dataJob, 1)
	t.w.jobQueue <- nil
	var eg errgroup.Group
	eg.Go(func() error {
		return t.w.dispatchSQL(ctx, t.file.Name(), 0, t.tableInfo)
	})

	eg.Go(func() error {
		// cancel ctx in another goroutine.
		// sleep to make dispatchSQL blocked
		time.Sleep(time.Second)
		cancel()
		return nil
	})

	c.Assert(eg.Wait(), IsNil)
}
