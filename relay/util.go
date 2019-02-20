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

package relay

import (
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/siddontang/go-mysql/replication"

	parserpkg "github.com/pingcap/dm/pkg/parser"
	"github.com/pingcap/dm/pkg/utils"
)

// checkIsDDL checks input SQL whether is a valid DDL statement
func checkIsDDL(sql string, p *parser.Parser) bool {
	sql = utils.TrimCtrlChars(sql)

	// if parse error, treat it as not a DDL
	stmts, err := parserpkg.Parse(p, sql, "", "")
	if err != nil || len(stmts) == 0 {
		return false
	}

	stmt := stmts[0]
	switch stmt.(type) {
	case ast.DDLNode:
		return true
	default:
		// some this like `BEGIN`
		return false
	}
}

// searchLastCompleteEventPos searches the last complete event (end) pos with an incomplete event at the end of the file.
// NOTE: test this when we can generate various types of binlog events
func searchLastCompleteEventPos(file string) (uint32, error) {
	var lastPos uint32
	onEventFunc := func(e *replication.BinlogEvent) error {
		lastPos = e.Header.LogPos
		return nil
	}

	parser2 := replication.NewBinlogParser()
	parser2.SetVerifyChecksum(true)
	parser2.SetUseDecimal(true)

	err := parser2.ParseFile(file, 4, onEventFunc)
	if strings.Contains(err.Error(), "err EOF") {
		if lastPos > 0 {
			return lastPos, nil
		}
		return 0, errors.Errorf("search for %s end, but not got a valid last pos", file)
	}
	return 0, errors.Annotatef(ErrNoIncompleteEventFound, "file %s, last pos %d", file, lastPos)
}

// tryFindGapStartPos tries to find a correct pos can be used to fill the gap.
// support cases:
// ERROR 1236 (HY000): binlog truncated in the middle of event; consider out of disk space on master; the first event 'mysql-bin.000003' at 1000, the last event read from '/var/lib/mysql/mysql-bin.000003' at 123, the last byte read from '/var/lib/mysql/mysql-bin.000003' at 1019.
// ERROR 1236 (HY000): bogus data in log event; the first event 'mysql-bin.000003' at 1000, the last event read from '/var/lib/mysql/mysql-bin.000003' at 123, the last byte read from '/var/lib/mysql/mysql-bin.000003' at 1019.
// if more cases found, add them later.
func tryFindGapStartPos(err error, file string) (uint32, error) {
	if !utils.IsErrBinlogPurged(err) {
		return 0, errors.NotSupportedf("error code is not 1236")
	}
	originErr := errors.Cause(err)
	errMsg := originErr.Error()
	if strings.Contains(errMsg, "binlog truncated in the middle of event") ||
		strings.Contains(errMsg, "bogus data in log event") {
		// try to handle the case where relay log file trimmed (or not wrote successfully) with an incomplete binlog event at the end of the file
		pos, err2 := searchLastCompleteEventPos(file) // try search a valid end pos for the file
		if err2 != nil {
			return 0, errors.Trace(err)
		}
		return pos, nil
	}
	return 0, errors.NotFoundf("correct start pos to fill the gap")
}
