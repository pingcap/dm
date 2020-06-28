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

	"github.com/pingcap/dm/dm/pb"
)

// OperateSchema operates schema for an upstream table.
func (s *Syncer) OperateSchema(ctx context.Context, req *pb.OperateWorkerSchemaRequest) (schema string, err error) {
	switch req.Op {
	case pb.SchemaOp_GetSchema:
		// we only try to get schema from schema-tracker now.
		// in other words, we can not get the schema if any DDL/DML has been replicated, or set a schema previously.
		return s.schemaTracker.GetCreateTable(ctx, req.Database, req.Table)
	case pb.SchemaOp_SetSchema:
	case pb.SchemaOp_RemoveSchema:
		// we only drop the schema in the schema-tracker now,
		// so if we drop the schema and continue to replicate any DDL/DML, it will try to get schema from downstream again.
		return "", s.schemaTracker.DropTable(req.Database, req.Table)
	}
	return "", nil
}
