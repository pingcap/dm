package relay

import (
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/parser"

	"github.com/pingcap/tidb-enterprise-tools/pkg/utils"
)

// checkIsDDL checks input SQL whether is a valid DDL statement
func checkIsDDL(sql string, p *parser.Parser) bool {
	sql = utils.TrimCtrlChars(sql)

	// if parse error, treat it as not a DDL
	stmts, err := p.Parse(sql, "", "")
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
