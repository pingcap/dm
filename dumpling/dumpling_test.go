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

package dumpling

import (
	"context"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/failpoint"

	. "github.com/pingcap/check"
)

var _ = Suite(&testDumplingSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testDumplingSuite struct {
	cfg *config.SubTaskConfig
}

func getDBConfigFromEnv() config.DBConfig {
	host := os.Getenv("MYSQL_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port, _ := strconv.Atoi(os.Getenv("MYSQL_PORT"))
	if port == 0 {
		port = 3306
	}
	user := os.Getenv("MYSQL_USER")
	if user == "" {
		user = "root"
	}
	pswd := os.Getenv("MYSQL_PSWD")
	return config.DBConfig{
		Host:     host,
		User:     user,
		Password: pswd,
		Port:     port,
	}
}

func (d *testDumplingSuite) SetUpSuite(c *C) {
	dir := c.MkDir()
	d.cfg = &config.SubTaskConfig{
		Name: "dumplint_ut",
		From: getDBConfigFromEnv(),
		LoaderConfig: config.LoaderConfig{
			Dir: dir,
		},
	}
	c.Assert(log.InitLogger(&log.Config{}), IsNil)
}

func (d *testDumplingSuite) TestDumpling(c *C) {
	dumpling := NewDumpling(d.cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := dumpling.Init(ctx)
	c.Assert(err, IsNil)
	resultCh := make(chan pb.ProcessResult, 1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		dumpling.Process(ctx, resultCh)
	}()
	wg.Wait()
	c.Assert(len(resultCh), Equals, 1)
	result := <-resultCh
	c.Assert(result.IsCanceled, IsFalse)
	c.Assert(len(result.Errors), Equals, 0)

	c.Assert(failpoint.Enable("github.com/pingcap/dm/dumpling/dumpUnitProcessWithError", `return("unknown error")`), IsNil)
	// nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/dm/dumpling/dumpUnitProcessWithError")

	// return error
	wg.Add(1)
	go func() {
		defer wg.Done()
		dumpling.Process(ctx, resultCh)
	}()
	wg.Wait()
	c.Assert(len(resultCh), Equals, 1)
	result = <-resultCh
	c.Assert(result.IsCanceled, IsFalse)
	c.Assert(len(result.Errors), Equals, 1)
	c.Assert(result.Errors[0].Message, Equals, "unknown error")

	// nolint:errcheck
	failpoint.Disable("github.com/pingcap/dm/dumpling/dumpUnitProcessWithError")

	c.Assert(failpoint.Enable("github.com/pingcap/dm/dumpling/dumpUnitProcessForever", `return("unknown error")`), IsNil)
	// nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/dm/dumpling/dumpUnitProcessForever")

	// cancel
	wg.Add(1)
	go func() {
		defer wg.Done()
		dumpling.Process(ctx, resultCh)
	}()
	cancel()
	wg.Wait()
	c.Assert(len(resultCh), Equals, 1)
	result = <-resultCh
	c.Assert(result.IsCanceled, IsTrue)
	c.Assert(len(result.Errors), Equals, 0)
}
