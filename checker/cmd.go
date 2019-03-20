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

package checker

import (
	"context"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/errors"
)

var (
	// ErrorMsgHeader used as the header of the error message when checking config failed.
	ErrorMsgHeader = "fail to check synchronization configuration with type"
)

// CheckSyncConfig checks synchronization configuration
func CheckSyncConfig(ctx context.Context, cfgs []*config.SubTaskConfig) error {
	if len(cfgs) == 0 {
		return nil
	}

	// all `IgnoreCheckingItems` of sub-task are same, so we take first one
	checkingItems := config.FilterCheckingItems(cfgs[0].IgnoreCheckingItems)
	if len(checkingItems) == 0 {
		return nil
	}

	c := NewChecker(cfgs, checkingItems)

	err := c.Init()
	if err != nil {
		return errors.Annotate(err, "fail to initial checker")
	}
	defer c.Close()

	pr := make(chan pb.ProcessResult, 1)
	c.Process(ctx, pr)
	for len(pr) > 0 {
		r := <-pr
		// we only want first error
		if len(r.Errors) > 0 {
			return errors.Errorf("%s %v: %v\n detail: %v", ErrorMsgHeader, r.Errors[0].Type, r.Errors[0].Msg, string(r.Detail))
		}
	}

	return nil
}
