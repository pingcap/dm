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

package master

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/pingcap/check"
)

var _ = check.Suite(&testHTTPServer{})

type testHTTPServer struct {
	server *Server
	cfg    *Config
}

func (t *testHTTPServer) startServer(ctx context.Context, c *check.C) {
	t.cfg = NewConfig()
	t.cfg.MasterAddr = ":8261"
	t.cfg.RPCRateLimit = DefaultRate
	t.cfg.RPCRateBurst = DefaultBurst
	t.cfg.DataDir = c.MkDir()
	c.Assert(t.cfg.adjust(), check.IsNil)

	t.server = NewServer(t.cfg)
	err := t.server.Start(ctx)
	c.Assert(err, check.IsNil)
}

func (t *testHTTPServer) stopServer(c *check.C) {
	if t.server != nil {
		t.server.Close()
	}
}

func (t *testHTTPServer) TestStatus(c *check.C) {
	ctx, cancel := context.WithCancel(context.Background())
	t.startServer(ctx, c)
	defer func() {
		cancel()
		t.stopServer(c)
	}()

	statusURL := fmt.Sprintf("http://127.0.0.1%s/status", t.cfg.MasterAddr)
	resp, err := http.Get(statusURL)
	c.Assert(err, check.IsNil)
	c.Assert(resp.StatusCode, check.Equals, http.StatusOK)
	buf, err2 := ioutil.ReadAll(resp.Body)
	c.Assert(err2, check.IsNil)
	status := string(buf)
	c.Assert(status, check.Matches, "Release Version:.*\nGit Commit Hash:.*\nGit Branch:.*\nUTC Build Time:.*\nGo Version:.*\n")
}
