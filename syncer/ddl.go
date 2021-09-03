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
	"time"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"go.uber.org/zap"

	tcontext "github.com/pingcap/dm/pkg/context"
	parserpkg "github.com/pingcap/dm/pkg/parser"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
	"github.com/pingcap/dm/syncer/metrics"
)

func (s *Syncer) parseDDLSQL(qec *queryEventContext) (stmt ast.StmtNode, err error) {
	// We use Parse not ParseOneStmt here, because sometimes we got a commented out ddl which can't be parsed
	// by ParseOneStmt(it's a limitation of tidb parser.)
	stmts, err := parserpkg.Parse(qec.p, qec.originSQL, "", "")
	if err != nil {
		// log error rather than fatal, so other defer can be executed
		s.tctx.L().Error("parse ddl", zap.String("sql", qec.originSQL))
		return nil, terror.ErrSyncerParseDDL.Delegate(err, qec.originSQL)
	}
	if len(stmts) == 0 {
		return nil, nil
	}
	return stmts[0], nil
}

// preprocessDDL preprocess ddl as follow step:
// 1. track ddl whatever skip it;
// 2. skip sql by filterQueryEvent;
// 3. apply online ddl if onlineDDL is not nil:
//    * specially, if skip, apply empty string;
// 4. handle online ddl SQL by handleOnlineDDL.
func (s *Syncer) preprocessDDL(qec *queryEventContext) error {
	for _, sql := range qec.splitedDDLs {
		stmt, err := qec.p.ParseOneStmt(sql, "", "")
		if err != nil {
			return terror.Annotatef(terror.ErrSyncerUnitParseStmt.New(err.Error()), "ddl %s", sql)
		}

		tableNames, err2 := parserpkg.FetchDDLTableNames(qec.ddlSchema, stmt, s.SourceTableNamesFlavor)
		if err != nil {
			return err
		}

		// get real tableNames before apply block-allow list
		realTables := []*filter.Table{}
		if s.onlineDDL != nil {
			for _, table := range tableNames {
				realTables = append(realTables, &filter.Table{
					Schema: table.Schema,
					Name:   s.onlineDDL.RealName(table.Name),
				})
			}
		}

		shouldSkip, err := s.filterQueryEvent(realTables, stmt, sql)
		if err2 != nil {
			return err
		}
		if shouldSkip {
			metrics.SkipBinlogDurationHistogram.WithLabelValues("query", s.cfg.Name, s.cfg.SourceID).Observe(time.Since(qec.startTime).Seconds())
			qec.tctx.L().Warn("skip event", zap.String("event", "query"), zap.String("statement", sql), zap.String("schema", qec.ddlSchema))
			if s.onlineDDL != nil {
				// nolint:errcheck
				s.onlineDDL.Apply(qec.tctx, tableNames, "", stmt)
			}
			continue
		}
		if s.onlineDDL == nil {
			qec.appliedDDLs = append(qec.appliedDDLs, sql)
			continue
		}

		// filter and store ghost table ddl, transform online ddl
		ss, err := s.handleOnlineDDL(qec, tableNames, sql, stmt)
		if err != nil {
			return err
		}

		qec.appliedDDLs = append(qec.appliedDDLs, ss...)
	}
	if len(qec.onlineDDLTableNames) > 1 {
		return terror.ErrSyncerUnitOnlineDDLOnMultipleTable.Generate(qec.originSQL)
	}
	return nil
}

// routeDDL will rename table names in DDL.
func (s *Syncer) routeDDL(qec *queryEventContext, sql string) (string, [][]*filter.Table, ast.StmtNode, error) {
	stmt, err := qec.p.ParseOneStmt(sql, "", "")
	if err != nil {
		return "", nil, nil, terror.Annotatef(terror.ErrSyncerUnitParseStmt.New(err.Error()), "ddl %s", sql)
	}

	tableNames, err := parserpkg.FetchDDLTableNames(qec.ddlSchema, stmt, s.SourceTableNamesFlavor)
	if err != nil {
		return "", nil, nil, err
	}

	targetTableNames := make([]*filter.Table, 0, len(tableNames))
	for i := range tableNames {
		schema, table := s.renameShardingSchema(tableNames[i].Schema, tableNames[i].Name)
		tableName := &filter.Table{
			Schema: schema,
			Name:   table,
		}
		targetTableNames = append(targetTableNames, tableName)
	}

	ddl, err := parserpkg.RenameDDLTable(stmt, targetTableNames)
	return ddl, [][]*filter.Table{tableNames, targetTableNames}, stmt, err
}

// handleOnlineDDL checks if the input `sql` is came from online DDL tools.
// If so, it will save actual DDL or return the actual DDL depending on online DDL types of `sql`.
// If not, it returns original SQL.
func (s *Syncer) handleOnlineDDL(qec *queryEventContext, tableNames []*filter.Table, sql string, stmt ast.StmtNode) ([]string, error) {
	if s.onlineDDL == nil {
		return []string{sql}, nil
	}

	sqls, err := s.onlineDDL.Apply(qec.tctx, tableNames, sql, stmt)
	if err != nil {
		return nil, err
	}

	// skip or origin sqls
	if len(sqls) == 0 || (len(sqls) == 1 && sqls[0] == sql) {
		return sqls, nil
	}

	// remove empty sqls which inserted because online DDL is filtered
	end := 0
	for _, sql2 := range sqls {
		if sql2 != "" {
			sqls[end] = sql2
			end++
		}
	}
	sqls = sqls[:end]

	sourceTable := tableNames[0]
	// RenameDDLTable need []*filter.table
	targetTables := tableNames[1:2]
	// TODO(okJiang): seems to repeat with some logic in routeDDL
	for i := range sqls {
		stmt, err := qec.p.ParseOneStmt(sqls[i], "", "")
		if err != nil {
			return nil, terror.ErrSyncerUnitParseStmt.New(err.Error())
		}

		sqls[i], err = parserpkg.RenameDDLTable(stmt, targetTables)
		if err != nil {
			return nil, err
		}
	}
	qec.onlineDDLTableNames[sourceTable.String()] = sourceTable
	return sqls, nil
}

func (s *Syncer) dropSchemaInSharding(tctx *tcontext.Context, sourceSchema string) error {
	sources := make(map[string][][]string)
	sgs := s.sgk.Groups()
	for name, sg := range sgs {
		if sg.IsSchemaOnly {
			// in sharding group leave handling, we always process schema group,
			// we can ignore schema only group here
			continue
		}
		tables := sg.Tables()
		for _, table := range tables {
			if table[0] != sourceSchema {
				continue
			}
			sources[name] = append(sources[name], table)
		}
	}
	// delete from sharding group firstly
	for name, tables := range sources {
		targetSchema, targetTable := utils.UnpackTableID(name)
		sourceIDs := make([]string, 0, len(tables))
		for _, table := range tables {
			sourceID, _ := utils.GenTableID(table[0], table[1])
			sourceIDs = append(sourceIDs, sourceID)
		}
		err := s.sgk.LeaveGroup(targetSchema, targetTable, sourceIDs)
		if err != nil {
			return err
		}
	}
	// delete from checkpoint
	for _, tables := range sources {
		for _, table := range tables {
			// refine clear them later if failed
			// now it doesn't have problems
			if err1 := s.checkpoint.DeleteTablePoint(tctx, table[0], table[1]); err1 != nil {
				s.tctx.L().Error("fail to delete checkpoint", zap.String("schema", table[0]), zap.String("table", table[1]))
			}
		}
	}
	return nil
}

func (s *Syncer) clearOnlineDDL(tctx *tcontext.Context, targetSchema, targetTable string) error {
	group := s.sgk.Group(targetSchema, targetTable)
	if group == nil {
		return nil
	}

	// return [[schema, table]...]
	tables := group.Tables()

	for _, table := range tables {
		s.tctx.L().Info("finish online ddl", zap.String("schema", table[0]), zap.String("table", table[1]))
		err := s.onlineDDL.Finish(tctx, table[0], table[1])
		if err != nil {
			return terror.Annotatef(err, "finish online ddl on %s.%s", table[0], table[1])
		}
	}

	return nil
}

type shardingDDLInfo struct {
	name       string
	tableNames [][]*filter.Table
	stmt       ast.StmtNode
}
