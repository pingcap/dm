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
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
)

var _ = check.Suite(&testHTTPServer{})

type testHTTPServer struct {
	server *Server
	cfg    *Config
}

func (t *testHTTPServer) startServer(c *check.C) {
	t.cfg = NewConfig()
	t.cfg.MasterAddr = ":8261"
	t.cfg.RPCRateLimit = DefaultRate
	t.cfg.RPCRateBurst = DefaultBurst

	t.server = NewServer(t.cfg)
	go func() {
		err := t.server.Start()
		c.Assert(err, check.IsNil)
	}()

	err := t.waitUntilServerOnline()
	c.Assert(err, check.IsNil)
}

func (t *testHTTPServer) stopServer(c *check.C) {
	if t.server != nil {
		t.server.Close()
	}
}

const retryTime = 100

func (t *testHTTPServer) waitUntilServerOnline() error {
	statusURL := fmt.Sprintf("http://127.0.0.1%s/status", t.cfg.MasterAddr)
	for i := 0; i < retryTime; i++ {
		resp, err := http.Get(statusURL)
		if err == nil && resp.StatusCode == http.StatusOK {
			ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			return nil
		}
		time.Sleep(time.Millisecond * 10)
	}
	return errors.Errorf("failed to connect http status for %d retries in every 10ms", retryTime)
}

func (t *testHTTPServer) TestStatus(c *check.C) {
	t.startServer(c)
	defer t.stopServer(c)

	statusURL := fmt.Sprintf("http://127.0.0.1%s/status", t.cfg.MasterAddr)
	resp, err := http.Get(statusURL)
	c.Assert(err, check.IsNil)
	c.Assert(resp.StatusCode, check.Equals, http.StatusOK)
	buf, err2 := ioutil.ReadAll(resp.Body)
	c.Assert(err2, check.IsNil)
	status := string(buf)
	c.Assert(status, check.Matches, "Release Version:.*\nGit Commit Hash:.*\nGit Branch:.*\nUTC Build Time:.*\nGo Version:.*\n")
}
