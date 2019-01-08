package syncer

import (
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"

	"github.com/pingcap/tidb-enterprise-tools/dm/pb"
)

// SetSQLOperator sets an SQL operator to syncer
func (s *Syncer) SetSQLOperator(req *pb.HandleSubTaskSQLsRequest) error {
	return errors.Trace(s.sqlOperatorHolder.Set(req))
}

// tryApplySQLOperator tries to get SQLs by applying an possible operator
// return whether applied, and the applied SQLs
func (s *Syncer) tryApplySQLOperator(pos mysql.Position, sqls []string) (bool, []string, error) {
	return s.sqlOperatorHolder.Apply(pos, sqls)
}
