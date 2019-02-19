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

package ddl

import (
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
)

// Parse wraps parser.Parse(), makes `parser` suitable for dm
func Parse(p *parser.Parser, sql, charset, collation string) (stmt []ast.StmtNode, err error) {
	stmts, warnings, err := p.Parse(sql, charset, collation)

	for _, warning := range warnings {
		log.Warnf("warning: parsing sql %s:%v", sql, warning)
	}

	if err != nil {
		log.Errorf("error: parsing sql %s:%v", sql, err)
	}

	return stmts, err
}
