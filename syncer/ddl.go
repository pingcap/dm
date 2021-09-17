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
// 1. track ddl whatever skip it;
// 2. skip sql by filterQueryEvent;
// 3. apply online ddl if onlineDDL is not nil:
//    * specially, if skip, apply empty string;
// 4. handle online ddl SQL by handleOnlineDDL.
func (s *Syncer) processSplitedDDL(qec *queryEventContext, sql string) ([]string, error) {
	stmt, err := qec.p.ParseOneStmt(sql, "", "")
	if err != nil {
		return nil, terror.Annotatef(terror.ErrSyncerUnitParseStmt.New(err.Error()), "ddl %s", sql)
	}

	tables, err2 := parserpkg.FetchDDLTables(qec.ddlSchema, stmt, s.SourceTableNamesFlavor)
	if err != nil {
		return nil, err
	}

	// get real tableNames before apply block-allow list
	realTables := make([]*filter.Table, 0, len(tables))
	for _, table := range tables {
		realName := table.Name
		if s.onlineDDL != nil {
			realName = s.onlineDDL.RealName(table.Name)
		}
		realTables = append(realTables, &filter.Table{
			Schema: table.Schema,
			Name:   realName,
		})
	}

	shouldSkip, err := s.filterQueryEvent(realTables, stmt, sql)
	if err2 != nil {
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
		sqls, err = s.onlineDDL.Apply(qec.tctx, tables, sql, stmt)
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
		sqls, err = s.renameOnlineDDLTable(qec, tables[1], sqls)
		if err != nil {
			return sqls, err
		}
		if qec.onlineDDLTable == nil {
			qec.onlineDDLTable = tables[0]
		} else if qec.onlineDDLTable.String() != tables[0].String() {
			return nil, terror.ErrSyncerUnitOnlineDDLOnMultipleTable.Generate(qec.originSQL)
		}
	}
	return sqls, nil
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
		s.tctx.L().Info("finish online ddl", zap.String("schema", table.Schema), zap.String("table", table.Name))
		err := s.onlineDDL.Finish(tctx, table.Schema, table.Name)
		if err != nil {
			return terror.Annotatef(err, "finish online ddl on %s.%s", table.Schema, table.Name)
		}
	}

	return nil
}

type shardingDDLInfo struct {
	name   string
	tables [][]*filter.Table
	stmt   ast.StmtNode
}
