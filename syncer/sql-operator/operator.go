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

package operator

import (
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/errors"
	"github.com/satori/go.uuid"
	"github.com/siddontang/go-mysql/mysql"

	"github.com/pingcap/dm/dm/command"
	"github.com/pingcap/dm/dm/pb"
)

// operator contains an operation for specified binlog pos or SQL pattern
// used by `sql-skip` and `sql-replace`
type operator struct {
	uuid    string // add a UUID, make it more friendly to be traced in log
	pos     *mysql.Position
	pattern string
	reg     *regexp.Regexp
	op      pb.SQLOp
	args    []string // if op == SQLOp_REPLACE, it has arguments
}

// newOperator creates a new Operator with a random UUID
func newOperator(pos *mysql.Position, pattern string, reg *regexp.Regexp, op pb.SQLOp, args []string) *operator {
	switch op {
	case pb.SQLOp_SKIP:
		if len(args) > 0 {
			log.Warnf("[sql-operator] for op %s, args %s ignored", op, strings.Join(args, ";"))
			args = nil
		}
	}

	return &operator{
		uuid:    uuid.NewV4().String(),
		pos:     pos,
		pattern: pattern,
		reg:     reg,
		op:      op,
		args:    args,
	}
}

// operate do the operation to return the args
func (o *operator) operate() ([]string, error) {
	switch o.op {
	case pb.SQLOp_SKIP:
		return nil, nil
	case pb.SQLOp_REPLACE:
		return o.args, nil
	default:
		return nil, errors.NotSupportedf("op %s", o.op)
	}
}

// matchPattern tries to match SQL with the regexp
func (o *operator) matchPattern(sql string) bool {
	if o.reg == nil {
		return false
	}
	return o.reg.MatchString(sql)
}

func (o *operator) String() string {
	if len(o.pattern) > 0 {
		return fmt.Sprintf("uuid: %s, pattern: %s, op: %s, args: %s", o.uuid, o.pattern, o.op, strings.Join(o.args, " "))
	}
	return fmt.Sprintf("uuid: %s, pos: %s, op: %s, args: %s", o.uuid, o.pos, o.op, strings.Join(o.args, " "))
}

// Holder holds SQL operators
type Holder struct {
	mu        sync.Mutex
	operators map[string]*operator
}

// NewHolder creates a new Holder
func NewHolder() *Holder {
	return &Holder{
		operators: make(map[string]*operator),
	}
}

// Set sets an operator according request
func (h *Holder) Set(req *pb.HandleSubTaskSQLsRequest) error {
	if req == nil {
		return errors.NotValidf("nil request")
	}
	switch req.Op {
	case pb.SQLOp_SKIP, pb.SQLOp_REPLACE:
	default:
		return errors.NotSupportedf("op %s", req.Op)
	}

	binlogPos, sqlReg, err := command.VerifySQLOperateArgs(req.BinlogPos, req.SqlPattern, false) // sharding only be used in DM-master
	if err != nil {
		return errors.Trace(err)
	}

	var key string
	if binlogPos != nil {
		key = binlogPos.String()
	} else if sqlReg != nil {
		key = req.SqlPattern // use sql-pattern as the key
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	oper := newOperator(binlogPos, req.SqlPattern, sqlReg, req.Op, req.Args)
	prev, ok := h.operators[key]
	if ok {
		log.Warnf("[sql-operator] overwrite previous operator %s by operator %s", prev, oper)
	}
	h.operators[key] = oper
	log.Infof("[sql-operator] set a new operator %s on replication unit", oper)
	return nil
}

// Apply tries to apply operator by pos or SQLs, returns applied, args, error
func (h *Holder) Apply(pos mysql.Position, sqls []string) (bool, []string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	var cause string
	key := pos.String()
	oper, ok := h.operators[key]
	if !ok {
	OUTER_FOR:
		for _, sql := range sqls {
			for key2, oper2 := range h.operators {
				if oper2.matchPattern(sql) { // matched one SQL of all is enough
					key = key2
					oper = oper2
					cause = fmt.Sprintf("sql-pattern %s matched SQL %s", key, sql)
					break OUTER_FOR
				}
			}
		}
		if oper == nil {
			return false, nil, nil
		}
	} else {
		cause = fmt.Sprintf("binlog-pos %s matched", pos)
	}

	delete(h.operators, key) // always delete the operator
	args, err := oper.operate()
	if err != nil {
		return false, nil, errors.Annotatef(err, "operator %s", oper)
	}

	log.Infof("[sql-operator] %s, applying operator %s", cause, oper)
	return true, args, nil
}
