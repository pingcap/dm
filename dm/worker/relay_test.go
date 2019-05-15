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
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/dm/relay"
	"github.com/pingcap/dm/relay/purger"
	"github.com/pingcap/errors"
)

func TestRunRelay(t *testing.T) {
	TestingT(t)
}

type testRelay struct{}

var _ = Suite(&testRelay{})

func (t *testRelay) TestRelay(c *C) {
	originNewRelay := relay.NewRelay
	relay.NewRelay = relay.NewDummyRelay
	originNewPurger := purger.NewPurger
	purger.NewPurger = purger.NewDummyPurger
	defer func() {
		relay.NewRelay = originNewRelay
		purger.NewPurger = originNewPurger
	}()

	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), IsNil)

	dir := c.MkDir()
	cfg.RelayDir = dir
	cfg.MetaDir = dir

	relayHolder := NewRealRelayHolder(cfg)
	c.Assert(relayHolder, NotNil)

	holder, ok := relayHolder.(*RealRelayHolder)
	c.Assert(ok, IsTrue)

	t.testInit(c, holder)
}

func (t *testRelay) testInit(c *C, holder *RealRelayHolder) {
	_, err := holder.Init(nil)
	c.Assert(err, IsNil)

	r, ok := holder.relay.(*relay.DummyRelay)
	c.Assert(ok, IsTrue)

	initErr := errors.New("init error")
	r.InjectInitError(initErr)

	_, err = holder.Init(nil)
	c.Assert(err, Equals, initErr)

}
