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
	"regexp"
	"strings"

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

// VerifySQLOperateArgs verify args for sql operation, including sql-skip, sql-replace.
// NOTE: only part of args verified in here and others need to be verified from outer.
func VerifySQLOperateArgs(binlogPosStr, sqlPattern string, sharding bool) (*mysql.Position, *regexp.Regexp, error) {
	binlogPosStr = TrimQuoteMark(binlogPosStr)
	sqlPattern = TrimQuoteMark(sqlPattern)

	var (
		pos *mysql.Position
		reg *regexp.Regexp
	)

	if len(binlogPosStr) > 0 && len(sqlPattern) > 0 {
		return nil, nil, terror.ErrVerifySQLOperateArgs.New("cannot specify both --binlog-pos and --sql-pattern in sql operation")
	}

	if len(binlogPosStr) > 0 {
		pos2, err := binlog.PositionFromStr(binlogPosStr)
		if err != nil {
			return nil, nil, terror.ErrVerifySQLOperateArgs.Generatef("invalid --binlog-pos %s in sql operation: %s", binlogPosStr, terror.Message(err))
		}
		pos = &pos2
	} else if len(sqlPattern) > 0 {
		var pattern string
		if strings.HasPrefix(sqlPattern, "~") {
			pattern = sqlPattern[1:]
		} else {
			pattern = "^" + regexp.QuoteMeta(sqlPattern) + "$"
		}

		var err error
		reg, err = regexp.Compile(pattern)
		if err != nil {
			return nil, nil, terror.ErrVerifySQLOperateArgs.AnnotateDelegate(err, "invalid --sql-pattern %s in sql operation", sqlPattern)
		}
	} else {
		return nil, nil, terror.ErrVerifySQLOperateArgs.New("must specify one of --binlog-pos and --sql-pattern in sql operation")
	}

	if sharding && len(binlogPosStr) > 0 {
		return nil, nil, terror.ErrVerifySQLOperateArgs.New("cannot specify --binlog-pos with --sharding in sql operation")
	}

	return pos, reg, nil
}
