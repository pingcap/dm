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
	"fmt"
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

func parseOneStmt(qec *queryEventContext) (stmt ast.StmtNode, err error) {
	// We use Parse not ParseOneStmt here, because sometimes we got a commented out ddl which can't be parsed
	// by ParseOneStmt(it's a limitation of tidb parser.)
	qec.tctx.L().Info("parse ddl", zap.String("event", "query"), zap.Stringer("query event context", qec))
	stmts, err := parserpkg.Parse(qec.p, qec.originSQL, "", "")
	if err != nil {
		// log error rather than fatal, so other defer can be executed
		qec.tctx.L().Error("parse ddl", zap.String("event", "query"), zap.Stringer("query event context", qec))
		return nil, terror.ErrSyncerParseDDL.Delegate(err, qec.originSQL)
	}
	if len(stmts) == 0 {
		return nil, nil
	}
	return stmts[0], nil
}

// processOneDDL processes already split ddl as following step:
// 1. track ddl no matter whether skip it; (TODO: will implement in https://github.com/pingcap/dm/pull/1975)
// 2. skip sql by filterQueryEvent;
// 3. apply online ddl if onlineDDL is not nil:
//    * specially, if skip, apply empty string;
// 4. handle online ddl SQL by handleOnlineDDL.
func (s *Syncer) processOneDDL(qec *queryEventContext, sql string) ([]string, error) {
	ddlInfo, err := s.routeDDL(qec.p, qec.ddlSchema, sql)
	if err != nil {
		return nil, err
	}
	// TODO: add track ddl
	// will implement in https://github.com/pingcap/dm/pull/1975

	if s.onlineDDL != nil {
		if err = s.onlineDDL.CheckRegex(ddlInfo.originStmt, qec.ddlSchema, s.SourceTableNamesFlavor); err != nil {
			return nil, err
		}
	}

	qec.tctx.L().Debug("will skip query event", zap.String("event", "query"), zap.String("statement", sql), zap.Stringer("ddlInfo", ddlInfo))
	shouldSkip, err := s.skipQueryEvent(qec.originSQL, ddlInfo)
	if err != nil {
		return nil, err
	}
	if shouldSkip {
		metrics.SkipBinlogDurationHistogram.WithLabelValues("query", s.cfg.Name, s.cfg.SourceID).Observe(time.Since(qec.startTime).Seconds())
		qec.tctx.L().Warn("skip event", zap.String("event", "query"), zap.String("statement", sql), zap.Stringer("query event context", qec))
		if s.onlineDDL == nil || len(ddlInfo.originDDL) != 0 {
			return nil, nil
		}
	}

	if s.onlineDDL == nil {
		return []string{ddlInfo.originDDL}, nil
	}
	// filter and save ghost table ddl
	sqls, err := s.onlineDDL.Apply(qec.tctx, ddlInfo.sourceTables, ddlInfo.originDDL, ddlInfo.originStmt, qec.p)
	if err != nil {
		return nil, err
	}
	// represent saved in onlineDDL.Storage
	if len(sqls) == 0 {
		return nil, nil
	}
	// represent this sql is not online DDL.
	if sqls[0] == sql {
		return sqls, nil
	}

	if qec.onlineDDLTable == nil {
		qec.onlineDDLTable = ddlInfo.sourceTables[0]
	} else if qec.onlineDDLTable.String() != ddlInfo.sourceTables[0].String() {
		return nil, terror.ErrSyncerUnitOnlineDDLOnMultipleTable.Generate(qec.originSQL)
	}
	return sqls, nil
}

// routeDDL route DDL from sourceTables to targetTables.
func (s *Syncer) routeDDL(p *parser.Parser, schema, sql string) (*ddlInfo, error) {
	s.tctx.L().Debug("route ddl", zap.String("event", "query"), zap.String("statement", sql))
	stmt, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		return nil, terror.Annotatef(terror.ErrSyncerUnitParseStmt.New(err.Error()), "ddl %s", sql)
	}

	sourceTables, err := parserpkg.FetchDDLTables(schema, stmt, s.SourceTableNamesFlavor)
	if err != nil {
		return nil, err
	}

	targetTables := make([]*filter.Table, 0, len(sourceTables))
	for i := range sourceTables {
		renamedTable := s.route(sourceTables[i])
		targetTables = append(targetTables, renamedTable)
	}

	routedDDL, err := parserpkg.RenameDDLTable(stmt, targetTables)
	return &ddlInfo{
		originDDL:    sql,
		routedDDL:    routedDDL,
		originStmt:   stmt,
		sourceTables: sourceTables,
		targetTables: targetTables,
	}, err
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

type ddlInfo struct {
	originDDL    string
	routedDDL    string
	originStmt   ast.StmtNode
	sourceTables []*filter.Table
	targetTables []*filter.Table
}

func (d *ddlInfo) String() string {
	var sourceTables, targetTables string
	for i := range d.sourceTables {
		sourceTables += d.sourceTables[i].String() + ", "
		targetTables += d.targetTables[i].String() + ", "
	}
	return fmt.Sprintf("{originDDL: %s, routedDDL: %s, sourceTables: %s, targetTables: %s}",
		d.originDDL, d.routedDDL, sourceTables, targetTables)
}
