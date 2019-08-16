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
	"context"
	"time"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"go.uber.org/zap"

	parserpkg "github.com/pingcap/dm/pkg/parser"
	"github.com/pingcap/dm/pkg/terror"
)

// InjectSQLs injects ddl into syncer as binlog events while meet xid/query event
// TODO: let user to specify special xid/query event position
// TODO: inject dml sqls
func (s *Syncer) InjectSQLs(ctx context.Context, sqls []string) error {
	// verify and fetch schema name
	schemas := make([]string, 0, len(sqls))
	parser2 := parser.New()
	for _, sql := range sqls {
		node, err := parser2.ParseOneStmt(sql, "", "")
		if err != nil {
			return terror.Annotatef(terror.ErrSyncerUnitParseStmt.New(err.Error()), "sql %s", sql)
		}
		ddlNode, ok := node.(ast.DDLNode)
		if !ok {
			return terror.ErrSyncerUnitInjectDDLOnly.Generate(sql)
		}
		tableNames, err := parserpkg.FetchDDLTableNames("", ddlNode)
		if err != nil {
			return err
		}
		if len(tableNames[0].Schema) == 0 {
			return terror.ErrSyncerUnitInjectDDLWithoutSchema.Generate(sql)
		}
		schemas = append(schemas, tableNames[0].Schema)
	}

	for i, sql := range sqls {
		schema := schemas[i]
		ev := genIncompleteQueryEvent([]byte(schema), []byte(sql))
		newCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		s.tctx.L().Info("injecting sql", zap.String("sql", sql), zap.String("schema", schema))

		select {
		case s.injectEventCh <- ev:
		case <-newCtx.Done():
			cancel()
			return newCtx.Err()
		}
		cancel()
	}
	return nil
}

func (s *Syncer) tryInject(op opType, pos mysql.Position) *replication.BinlogEvent {
	if op != xid && op != ddl {
		return nil
	}

	select {
	case e := <-s.injectEventCh:
		// try receive from extra binlog event chan
		// NOTE: now we simply set EventSize to 0, make event's start / end pos are the same
		e.Header.LogPos = pos.Pos
		e.Header.EventSize = 0
		s.tctx.L().Info("inject binlog event from inject chan", zap.Reflect("header", e.Header), zap.Reflect("event", e.Event))
		return e
	default:
		return nil
	}
}

// generates an incomplete QueryEvent, only partial fields are valid
// now, it only used to generate QueryEvent to force sharding group to be synced
// NOTE: using only if you know want your are doing
func genIncompleteQueryEvent(schema, query []byte) *replication.BinlogEvent {
	header := &replication.EventHeader{
		EventType: replication.QUERY_EVENT,
	}
	queryEvent := &replication.QueryEvent{
		Schema: schema,
		Query:  query,
	}
	e := &replication.BinlogEvent{
		Header: header,
		Event:  queryEvent,
	}
	return e
}
