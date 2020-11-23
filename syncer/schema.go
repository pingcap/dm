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

package syncer

import (
	"context"
	"strings"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/model"

	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/schema"
	"github.com/pingcap/dm/pkg/terror"
)

// OperateSchema operates schema for an upstream table.
func (s *Syncer) OperateSchema(ctx context.Context, req *pb.OperateWorkerSchemaRequest) (createTableStr string, err error) {
	switch req.Op {
	case pb.SchemaOp_GetSchema:
		// we only try to get schema from schema-tracker now.
		// in other words, we can not get the schema if any DDL/DML has been replicated, or set a schema previously.
		return s.schemaTracker.GetCreateTable(ctx, req.Database, req.Table)
	case pb.SchemaOp_SetSchema:
		// for set schema, we must ensure it's a valid `CREATE TABLE` statement.
		// now, we only set schema for schema-tracker,
		// if want to update the one in checkpoint, it should wait for the flush of checkpoint.
		parser2, err := s.fromDB.getParser(ctx)
		if err != nil {
			return "", err
		}
		node, err := parser2.ParseOneStmt(req.Schema, "", "")
		if err != nil {
			return "", terror.ErrSchemaTrackerInvalidCreateTableStmt.Delegate(err, req.Schema)
		}
		stmt, ok := node.(*ast.CreateTableStmt)
		if !ok {
			return "", terror.ErrSchemaTrackerInvalidCreateTableStmt.Generate(req.Schema)
		}
		// ensure correct table name.
		stmt.Table.Schema = model.NewCIStr(req.Database)
		stmt.Table.Name = model.NewCIStr(req.Table)
		stmt.IfNotExists = false // we must ensure drop the previous one.

		var newCreateSQLBuilder strings.Builder
		restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &newCreateSQLBuilder)
		if err = stmt.Restore(restoreCtx); err != nil {
			return "", terror.ErrSchemaTrackerRestoreStmtFail.Delegate(err)
		}
		newSQL := newCreateSQLBuilder.String()

		// drop the previous schema first.
		err = s.schemaTracker.DropTable(req.Database, req.Table)
		if err != nil && !schema.IsTableNotExists(err) {
			return "", terror.ErrSchemaTrackerCannotDropTable.Delegate(err, req.Database, req.Table)
		}
		err = s.schemaTracker.CreateSchemaIfNotExists(req.Database)
		if err != nil {
			return "", terror.ErrSchemaTrackerCannotCreateSchema.Delegate(err, req.Database)
		}
		err = s.schemaTracker.Exec(ctx, req.Database, newSQL)
		if err != nil {
			return "", terror.ErrSchemaTrackerCannotCreateTable.Delegate(err, req.Database, req.Table)
		}
		return "", nil
	case pb.SchemaOp_RemoveSchema:
		// we only drop the schema in the schema-tracker now,
		// so if we drop the schema and continue to replicate any DDL/DML, it will try to get schema from downstream again.
		return "", s.schemaTracker.DropTable(req.Database, req.Table)
	}
	return "", nil
}
