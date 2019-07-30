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
	"github.com/siddontang/go-mysql/mysql"

	"github.com/pingcap/dm/dm/pb"
)

// SetSQLOperator sets an SQL operator to syncer
func (s *Syncer) SetSQLOperator(req *pb.HandleSubTaskSQLsRequest) error {
	return s.sqlOperatorHolder.Set(s.tctx, req)
}

// tryApplySQLOperator tries to get SQLs by applying an possible operator
// return whether applied, and the applied SQLs
func (s *Syncer) tryApplySQLOperator(pos mysql.Position, sqls []string) (bool, []string, error) {
	return s.sqlOperatorHolder.Apply(s.tctx, pos, sqls)
}
