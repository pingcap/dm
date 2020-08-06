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

package master

import (
	"context"
	"io/ioutil"
	"path/filepath"
	"time"

	. "github.com/pingcap/check"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/terror"
)

func (t *testMaster) TestCollectSourceConfigFilesV1Import(c *C) {
	s := testDefaultMasterServer(c)
	defer s.Close()
	s.cfg.V1SourcesPath = c.MkDir()

	// no source file exist.
	cfgs, err := s.collectSourceConfigFilesV1Import()
	c.Assert(err, IsNil)
	c.Assert(cfgs, HasLen, 0)

	// load a valid source file.
	cfg1 := config.NewSourceConfig()
	cfg1.From.Session = map[string]string{} // fix empty map after marshal/unmarshal becomes nil
	c.Assert(cfg1.LoadFromFile("./source.yaml"), IsNil)
	cfg2 := cfg1.Clone()
	cfg2.SourceID = "mysql-replica-02"

	// write into source files.
	data1, err := cfg1.Yaml()
	c.Assert(err, IsNil)
	c.Assert(ioutil.WriteFile(filepath.Join(s.cfg.V1SourcesPath, "source1.yaml"), []byte(data1), 0644), IsNil)
	data2, err := cfg2.Yaml()
	c.Assert(err, IsNil)
	c.Assert(ioutil.WriteFile(filepath.Join(s.cfg.V1SourcesPath, "source2.yaml"), []byte(data2), 0644), IsNil)

	// collect again, two configs exist.
	cfgs, err = s.collectSourceConfigFilesV1Import()
	c.Assert(err, IsNil)
	c.Assert(cfgs, HasLen, 2)
	c.Assert(cfgs[cfg1.SourceID], DeepEquals, *cfg1)
	c.Assert(cfgs[cfg2.SourceID], DeepEquals, *cfg2)

	// put a invalid source file.
	c.Assert(ioutil.WriteFile(filepath.Join(s.cfg.V1SourcesPath, "invalid.yaml"), []byte("invalid-source-data"), 0644), IsNil)
	cfgs, err = s.collectSourceConfigFilesV1Import()
	c.Assert(terror.ErrConfigYamlTransform.Equal(err), IsTrue)
	c.Assert(cfgs, HasLen, 0)
}

func (t *testMaster) TestWaitWorkersReadyV1Import(c *C) {
	oldWaitWorkerV1Timeout := waitWorkerV1Timeout
	defer func() {
		waitWorkerV1Timeout = oldWaitWorkerV1Timeout
	}()
	waitWorkerV1Timeout = 5 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := testDefaultMasterServer(c)
	defer s.Close()
	s.cfg.V1SourcesPath = c.MkDir()
	c.Assert(s.scheduler.Start(ctx, etcdTestCli), IsNil)

	cfg1 := config.NewSourceConfig()
	c.Assert(cfg1.LoadFromFile("./source.yaml"), IsNil)
	cfg2 := cfg1.Clone()
	cfg2.SourceID = "mysql-replica-02"
	cfgs := map[string]config.SourceConfig{
		cfg1.SourceID: *cfg1,
		cfg2.SourceID: *cfg2,
	}

	// no worker registered, timeout.
	err := s.waitWorkersReadyV1Import(context.Background(), cfgs)
	c.Assert(err, ErrorMatches, ".*wait for DM-worker instances timeout.*")
}
