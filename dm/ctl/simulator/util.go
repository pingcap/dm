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

package simulator

import (
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
)

// checkResp checks the result of RPC response
func checkResp(err error, resp *pb.SimulationResponse) error {
	if err != nil {
		return errors.Trace(err)
	} else if !resp.Result {
		return errors.New(resp.Msg)
	}
	return nil
}

func getTableFromCMD(cmd *cobra.Command) (string, error) {
	tableName, err := cmd.Flags().GetString("table")
	if err != nil {
		return "", errors.Annotate(err, "get table arg failed")
	}
	if tableName == "" {
		return "", errors.New("argument table is not given. please check it again")
	}
	return tableName, nil
}
