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

	"github.com/pingcap/parser"
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
	s.tctx.L().Info("parse ddl", zap.String("statement", qec.originSQL))
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

// processSplitedDDL processes SplitedDDLddl as follow step:
// 1. track ddl whatever skip it except optimist shard ddl; (TODO: will implement in https://github.com/pingcap/dm/pull/1975)
// 2. skip sql by filterQueryEvent;
// 3. apply online ddl if onlineDDL is not nil:
//    * specially, if skip, apply empty string;
// 4. handle online ddl SQL by handleOnlineDDL.
func (s *Syncer) processSplitedDDL(qec *queryEventContext, sql string) ([]string, error) {
	_, sourceTables, targetTables, stmt, err := s.routeDDL(qec.p, qec.ddlSchema, sql)
	if err != nil {
		return nil, err
	}
	// TODO: add track ddl

	// get real tables before apply block-allow list
	realTables := make([]*filter.Table, 0, len(targetTables))
	for _, table := range targetTables {
		realName := table.Name
		if s.onlineDDL != nil {
			realName = s.onlineDDL.RealName(table.Name)
		}
		realTables = append(realTables, &filter.Table{
			Schema: table.Schema,
			Name:   realName,
		})
	}

	shouldSkip, err := s.skipQueryEvent(realTables, stmt, sql)
	if err != nil {
		return nil, err
	}
	if shouldSkip {
		metrics.SkipBinlogDurationHistogram.WithLabelValues("query", s.cfg.Name, s.cfg.SourceID).Observe(time.Since(qec.startTime).Seconds())
		qec.tctx.L().Warn("skip event", zap.String("event", "query"), zap.String("statement", sql), zap.String("schema", qec.ddlSchema))
		// when skip ddl, apply empty ddl for onlineDDL
		// otherwise it cause an error once meet the rename ddl of onlineDDL
		sql = ""
	}

	sqls := []string{sql}
	if s.onlineDDL != nil {
		// filter and save ghost table ddl
		sqls, err = s.onlineDDL.Apply(qec.tctx, sourceTables, sql, stmt)
		if err != nil {
			return nil, err
		}
		// len(sql) == 0: represent it should skip
		// len(sqls) == 0: represent saved in onlineDDL.Storage
		if len(sql) == 0 || len(sqls) == 0 {
			return nil, nil
		}
		// sqls[0] == sql: represent this sql is not online DDL.
		if sqls[0] == sql {
			return sqls, nil
		}
		// In there, stmt must be a `RenameTableStmt`. See details in OnlinePlugin.Apply.
		// So tables is [old1, new1], which new1 is the OnlinePlugin.RealTable. See details in FetchDDLTables.
		// Rename ddl's table to RealTable.
		sqls, err = s.renameOnlineDDLTable(qec, sourceTables[1], sqls)
		if err != nil {
			return sqls, err
		}
		if qec.onlineDDLTable == nil {
			qec.onlineDDLTable = sourceTables[0]
		} else if qec.onlineDDLTable.String() != sourceTables[0].String() {
			return nil, terror.ErrSyncerUnitOnlineDDLOnMultipleTable.Generate(qec.originSQL)
		}
	}
	return sqls, nil
}

// routeDDL route DDL from sourceTables to targetTables.
func (s *Syncer) routeDDL(p *parser.Parser, schema, sql string) (
	routedDDL string,
	sourceTables, targetTables []*filter.Table,
	stmt ast.StmtNode,
	err error) {
	stmt, err = p.ParseOneStmt(sql, "", "")
	if err != nil {
		return "", nil, nil, nil, terror.Annotatef(terror.ErrSyncerUnitParseStmt.New(err.Error()), "ddl %s", sql)
	}

	sourceTables, err = parserpkg.FetchDDLTables(schema, stmt, s.SourceTableNamesFlavor)
	if err != nil {
		return "", nil, nil, nil, err
	}

	targetTables = make([]*filter.Table, 0, len(sourceTables))
	for i := range sourceTables {
		renamedTable := s.route(sourceTables[i])
		targetTables = append(targetTables, renamedTable)
	}

	routedDDL, err = parserpkg.RenameDDLTable(stmt, targetTables)
	return
}

// renameOnlineDDLTable renames the given ddl sqls by given targetTable.
func (s *Syncer) renameOnlineDDLTable(qec *queryEventContext, targetTable *filter.Table, sqls []string) ([]string, error) {
	renamedSqls := make([]string, 0, len(sqls))
	targetTables := []*filter.Table{targetTable}
	for _, sql := range sqls {
		// remove empty sqls which inserted because online DDL is filtered
		if sql == "" {
			continue
		}
		stmt, err := qec.p.ParseOneStmt(sql, "", "")
		if err != nil {
			return nil, terror.ErrSyncerUnitParseStmt.New(err.Error())
		}

		sql, err = parserpkg.RenameDDLTable(stmt, targetTables)
		if err != nil {
			return nil, err
		}
		renamedSqls = append(renamedSqls, sql)
	}
	return renamedSqls, nil
}

func (s *Syncer) dropSchemaInSharding(tctx *tcontext.Context, sourceSchema string) error {
	sources := make(map[string][]*filter.Table)
	sgs := s.sgk.Groups()
	for name, sg := range sgs {
		if sg.IsSchemaOnly {
			// in sharding group leave handling, we always process schema group,
			// we can ignore schema only group here
			continue
		}
		tables := sg.Tables()
		for _, table := range tables {
			if table.Schema != sourceSchema {
				continue
			}
			sources[name] = append(sources[name], table)
		}
	}
	// delete from sharding group firstly
	for name, tables := range sources {
		targetTable := utils.UnpackTableID(name)
		sourceTableIDs := make([]string, 0, len(tables))
		for _, table := range tables {
			sourceTableIDs = append(sourceTableIDs, utils.GenTableID(table))
		}
		err := s.sgk.LeaveGroup(targetTable, sourceTableIDs)
		if err != nil {
			return err
		}
	}
	// delete from checkpoint
	for _, tables := range sources {
		for _, table := range tables {
			// refine clear them later if failed
			// now it doesn't have problems
			if err1 := s.checkpoint.DeleteTablePoint(tctx, table); err1 != nil {
				s.tctx.L().Error("fail to delete checkpoint", zap.Stringer("table", table))
			}
		}
	}
	return nil
}

func (s *Syncer) clearOnlineDDL(tctx *tcontext.Context, targetTable *filter.Table) error {
	group := s.sgk.Group(targetTable)
	if group == nil {
		return nil
	}

	// return [[schema, table]...]
	tables := group.Tables()

	for _, table := range tables {
		s.tctx.L().Info("finish online ddl", zap.Stringer("table", table))
		err := s.onlineDDL.Finish(tctx, table)
		if err != nil {
			return terror.Annotatef(err, "finish online ddl on %v", table)
		}
	}

	return nil
}

type shardingDDLInfo struct {
	name   string
	tables [][]*filter.Table
	stmt   ast.StmtNode
}

// TODO: use ddlInfo to flow
// fetch from routeDDL, saved in onlineDDL, used for trackDDL
// nolint: no used
type ddlInfo struct {
	sql          string
	stmt         ast.StmtNode
	sourceTables []*filter.Table
	targetTables []*filter.Table
}
