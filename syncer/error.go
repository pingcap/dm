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

package syncer

import (
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	tmysql "github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	tddl "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	gmysql "github.com/siddontang/go-mysql/mysql"
	"go.uber.org/zap"

	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/log"
)

func ignoreDDLError(err error) bool {
	err = originError(err)
	mysqlErr, ok := err.(*mysql.MySQLError)
	if !ok {
		return false
	}

	errCode := terror.ErrCode(mysqlErr.Number)
	switch errCode {
	case infoschema.ErrDatabaseExists.Code(), infoschema.ErrDatabaseNotExists.Code(), infoschema.ErrDatabaseDropExists.Code(),
		infoschema.ErrTableExists.Code(), infoschema.ErrTableNotExists.Code(), infoschema.ErrTableDropExists.Code(),
		infoschema.ErrColumnExists.Code(), infoschema.ErrColumnNotExists.Code(),
		infoschema.ErrIndexExists.Code(), tddl.ErrCantDropFieldOrKey.Code():
		return true
	case tmysql.ErrDupKeyName:
		return true
	default:
		return false
	}
}

func needRetryReplicate(err error) bool {
	err = originError(err)
	return err == gmysql.ErrBadConn
}

func isBinlogPurgedError(err error) bool {
	return isMysqlError(err, tmysql.ErrMasterFatalErrorReadingBinlog)
}

func isMysqlError(err error, code uint16) bool {
	err = originError(err)
	mysqlErr, ok := err.(*mysql.MySQLError)
	return ok && mysqlErr.Number == code
}

// originError return original error
func originError(err error) error {
	for {
		e := errors.Cause(err)
		if e == err {
			break
		}
		err = e
	}
	return err
}

// handleSpecialDDLError handles special errors for DDL execution.
// it only ignore `invalid connection` error (timeout or other causes) for `ADD INDEX` now.
// `invalid connection` means some data already sent to the server,
// and we assume that the whole SQL statement has already sent to the server for this error.
// if we have other methods to judge the DDL dispatched but timeout for executing, we can update this method.
// NOTE: we must ensure other PK/UK exists for correctness.
// NOTE: when we are refactoring the shard DDL algorithm, we also need to consider supporting non-blocking `ADD INDEX`.
func (s *Syncer) handleSpecialDDLError(tctx *tcontext.Context, err error, ddls []string, index int, conn *DBConn) error {
	// must ensure only the last statement executed failed with the `invalid connection` error
	if len(ddls) == 0 || index != len(ddls)-1 || errors.Cause(err) != mysql.ErrInvalidConn {
		return err // return the original error
	}

	parser2, err2 := s.fromDB.getParser(s.cfg.EnableANSIQuotes)
	if err2 != nil {
		return err // return the original error
	}

	ddl2 := ddls[index]
	stmt, err2 := parser2.ParseOneStmt(ddl2, "", "")
	if err2 != nil {
		return err // return the original error
	}

	handle := func() {
		tctx.L().Warn("ignore special error for DDL", zap.String("DDL", ddl2), log.ShortError(err))
		err2 := conn.resetConn(tctx) // also reset the `invalid connection` for later use.
		if err2 != nil {
			tctx.L().Warn("reset connection failed", log.ShortError(err2))
		}
	}

	switch v := stmt.(type) {
	case *ast.AlterTableStmt:
		// ddls should be split with only one spec
		if len(v.Specs) > 1 {
			return err
		} else if v.Specs[0].Tp == ast.AlterTableAddConstraint {
			// only take effect on `ADD INDEX`, no UNIQUE KEY and FOREIGN KEY
			// UNIQUE KEY may affect correctness, FOREIGN KEY should be filtered.
			// ref https://github.com/pingcap/tidb/blob/3cdea0dfdf28197ee65545debce8c99e6d2945e3/ddl/ddl_api.go#L1929-L1948.
			switch v.Specs[0].Constraint.Tp {
			case ast.ConstraintKey, ast.ConstraintIndex:
				handle()
				return nil // ignore the error
			}
		}
	case *ast.CreateIndexStmt:
		handle()
		return nil // ignore the error
	}
	return err
}
