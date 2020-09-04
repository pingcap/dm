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

package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/pingcap/dm/dm/pb"
)

// createSources does `operate-source create` operation for two sources.
// NOTE: we put two source config files (`source1.yaml` and `source2.yaml`) in `conf` directory.
func createSources(ctx context.Context, cli pb.MasterClient) error {
	_, currFile, _, _ := runtime.Caller(0)
	confDir := filepath.Join(filepath.Dir(currFile), "conf")

	s1Path := filepath.Join(confDir, "source1.yaml")
	s2Path := filepath.Join(confDir, "source2.yaml")

	s1Content, err := ioutil.ReadFile(s1Path)
	if err != nil {
		return err
	}
	s2Content, err := ioutil.ReadFile(s2Path)
	if err != nil {
		return err
	}

	resp, err := cli.OperateSource(ctx, &pb.OperateSourceRequest{
		Op:     pb.SourceOp_StartSource,
		Config: []string{string(s1Content), string(s2Content)},
	})
	if err != nil {
		return err
	} else if !resp.Result && !strings.Contains(resp.Msg, "already exists") {
		return fmt.Errorf("fail to create source: %s", resp.Msg)
	}
	return nil
}
