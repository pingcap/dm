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

package utils

import (
	"context"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/mysql"

	"github.com/pingcap/dm/pkg/terror"
)

var (
	// OsExit is function placeholder for os.Exit
	OsExit func(int)
	/*
		CREATE [TEMPORARY] TABLE [IF NOT EXISTS] tbl_name
			{ LIKE old_tbl_name | (LIKE old_tbl_name) }
	*/
	builtInSkipDDLs = []string{
		// transaction
		"^SAVEPOINT",

		// skip all flush sqls
		"^FLUSH",

		// table maintenance
		"^OPTIMIZE\\s+TABLE",
		"^ANALYZE\\s+TABLE",
		"^REPAIR\\s+TABLE",

		// temporary table
		"^DROP\\s+(\\/\\*\\!40005\\s+)?TEMPORARY\\s+(\\*\\/\\s+)?TABLE",

		// trigger
		"^CREATE\\s+(DEFINER\\s?=.+?)?TRIGGER",
		"^DROP\\s+TRIGGER",

		// procedure
		"^DROP\\s+PROCEDURE",
		"^CREATE\\s+(DEFINER\\s?=.+?)?PROCEDURE",
		"^ALTER\\s+PROCEDURE",

		// view
		"^CREATE\\s*(OR REPLACE)?\\s+(ALGORITHM\\s?=.+?)?(DEFINER\\s?=.+?)?\\s+(SQL SECURITY DEFINER)?VIEW",
		"^DROP\\s+VIEW",
		"^ALTER\\s+(ALGORITHM\\s?=.+?)?(DEFINER\\s?=.+?)?(SQL SECURITY DEFINER)?VIEW",

		// function
		// user-defined function
		"^CREATE\\s+(AGGREGATE)?\\s*?FUNCTION",
		// stored function
		"^CREATE\\s+(DEFINER\\s?=.+?)?FUNCTION",
		"^ALTER\\s+FUNCTION",
		"^DROP\\s+FUNCTION",

		// tableSpace
		"^CREATE\\s+TABLESPACE",
		"^ALTER\\s+TABLESPACE",
		"^DROP\\s+TABLESPACE",

		// account management
		"^GRANT",
		"^REVOKE",
		"^CREATE\\s+USER",
		"^ALTER\\s+USER",
		"^RENAME\\s+USER",
		"^DROP\\s+USER",
		"^SET\\s+PASSWORD",
	}
	builtInSkipDDLPatterns *regexp.Regexp
)

func init() {
	OsExit = os.Exit
	builtInSkipDDLPatterns = regexp.MustCompile("(?i)" + strings.Join(builtInSkipDDLs, "|"))
}

// DecodeBinlogPosition parses a mysql.Position from string format
func DecodeBinlogPosition(pos string) (*mysql.Position, error) {
	if len(pos) < 3 {
		return nil, terror.ErrInvalidBinlogPosStr.Generate(pos)
	}
	if pos[0] != '(' || pos[len(pos)-1] != ')' {
		return nil, terror.ErrInvalidBinlogPosStr.Generate(pos)
	}
	sp := strings.Split(pos[1:len(pos)-1], ",")
	if len(sp) != 2 {
		return nil, terror.ErrInvalidBinlogPosStr.Generate(pos)
	}
	position, err := strconv.ParseUint(strings.TrimSpace(sp[1]), 10, 32)
	if err != nil {
		return nil, terror.ErrInvalidBinlogPosStr.Delegate(err, pos)
	}
	return &mysql.Position{
		Name: strings.TrimSpace(sp[0]),
		Pos:  uint32(position),
	}, nil
}

// CompareBinlogPos compares binlog positions.
// The result will be 0 if |a-b| < deviation, otherwise -1 if a < b, and +1 if a > b.
func CompareBinlogPos(a, b mysql.Position, deviation float64) int {
	if a.Name < b.Name {
		return -1
	}

	if a.Name > b.Name {
		return 1
	}

	if math.Abs(float64(a.Pos)-float64(b.Pos)) <= deviation {
		return 0
	}

	if a.Pos < b.Pos {
		return -1
	}

	return 1
}

// WaitSomething waits for something done with `true`.
func WaitSomething(backoff int, waitTime time.Duration, fn func() bool) bool {
	for i := 0; i < backoff; i++ {
		if fn() {
			return true
		}

		time.Sleep(waitTime)
	}

	return false
}

// IsContextCanceledError checks whether err is context.Canceled
func IsContextCanceledError(err error) bool {
	return errors.Cause(err) == context.Canceled
}

// IsBuildInSkipDDL return true when checked sql that will be skipped for syncer
func IsBuildInSkipDDL(sql string) bool {
	return builtInSkipDDLPatterns.FindStringIndex(sql) != nil
}
