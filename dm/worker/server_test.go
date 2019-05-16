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

package worker

import (
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	. "github.com/pingcap/check"
)

func TestServer(t *testing.T) {
	TestingT(t)
}

type testServer struct{}

var _ = Suite(&testServer{})

func (t *testServer) TestServerStartAndClose(c *C) {
	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), IsNil)

	dir := c.MkDir()
	cfg.RelayDir = dir
	cfg.MetaDir = dir

	NewRelayHolder = NewDummyRelayHolder
	defer func() {
		NewRelayHolder = NewRealRelayHolder
	}()

	s := NewServer(cfg)

	go func() {
		err1 := s.Start()
		c.Assert(err1, IsNil)
	}()

	c.Assert(waitSomthing(10, func() bool {
		return !s.closed.Get()
	}), IsTrue)

	// test condition hub
	t.testConidtionHub(c, s)

	t.testHTTPInterface(c, "status")
	t.testHTTPInterface(c, "metrics")

	s.Close()

	c.Assert(waitSomthing(10, func() bool {
		return s.closed.Get()
	}), IsTrue)

	// test worker, just make sure testing sort
	t.testWorker(c)
}

func (t *testServer) testHTTPInterface(c *C, uri string) {
	resp, err := http.Get("http://127.0.0.1:8262/" + uri)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	_, err = ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
}

func waitSomthing(backoff int, fn func() bool) bool {
	for i := 0; i < backoff; i++ {
		if fn() {
			return true
		}

		time.Sleep(10 * time.Millisecond)
	}

	return false
}
