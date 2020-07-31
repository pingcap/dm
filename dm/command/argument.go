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

package command

import (
	"github.com/siddontang/go-mysql/mysql"

	"github.com/pingcap/dm/pkg/binlog"
	"github.com/pingcap/dm/pkg/terror"
)

// TrimQuoteMark tries to trim leading and tailing quote(") mark if exists
// only trim if leading and tailing quote matched as a pair
func TrimQuoteMark(s string) string {
	if len(s) > 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}

// VerifyBinlogPos verify binlog pos string
func VerifyBinlogPos(pos string) (*mysql.Position, error) {
	binlogPosStr := TrimQuoteMark(pos)
	pos2, err := binlog.PositionFromStr(binlogPosStr)
	if err != nil {
		return nil, terror.ErrVerifyHandleErrorArgs.Generatef("invalid --binlog-pos %s in handle-error operation: %s", binlogPosStr, terror.Message(err))
	}
	return &pos2, nil
}
