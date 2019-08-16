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
	"sync"

	"github.com/golang/protobuf/proto"
	uuid "github.com/satori/go.uuid"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/command"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
)

// Operator contains an operation for specified binlog pos
// used by `sql-skip` and `sql-replace`
type Operator struct {
	uuid string
	Req  *pb.HandleSQLsRequest
	reg  *regexp.Regexp
}

// newOperator creates a new Operator with a random UUID
func newOperator(req *pb.HandleSQLsRequest, reg *regexp.Regexp) *Operator {
	return &Operator{
		uuid: uuid.NewV4().String(),
		Req:  req,
		reg:  reg,
	}
}

// matchPattern tries to match SQL with the regexp
func (o *Operator) matchPattern(sql string) bool {
	if o.reg == nil {
		return false
	}
	return o.reg.MatchString(sql)
}

// clone returns a deep copy
func (o *Operator) clone() *Operator {
	return &Operator{
		uuid: o.uuid,
		reg:  o.reg.Copy(),
		Req:  proto.Clone(o.Req).(*pb.HandleSQLsRequest),
	}
}

func (o *Operator) String() string {
	return fmt.Sprintf("uuid: %s, request: %s", o.uuid, o.Req)
}

// Holder holds SQL operators
type Holder struct {
	mu        sync.RWMutex
	operators map[string]map[string]*Operator // taskName -> Key(sql-pattern) -> Operator
	logger    log.Logger
}

// NewHolder creates a new Holder
func NewHolder() *Holder {
	return &Holder{
		operators: make(map[string]map[string]*Operator),
		logger:    log.With(zap.String("component", "sql operator")),
	}
}

// Set sets an operator according to request
func (h *Holder) Set(req *pb.HandleSQLsRequest) error {
	if req == nil {
		return terror.ErrMasterSQLOpNilRequest.Generate()
	}
	switch req.Op {
	case pb.SQLOp_SKIP, pb.SQLOp_REPLACE:
	default:
		return terror.ErrMasterSQLOpNotSupport.Generate(req.Op)
	}

	// now, only support --sharding operate request
	if !req.Sharding {
		return terror.ErrMasterSQLOpWithoutSharding.Generate()
	}

	_, sqlReg, err := command.VerifySQLOperateArgs(req.BinlogPos, req.SqlPattern, req.Sharding)
	if err != nil {
		return terror.WithClass(err, terror.ClassDMMaster)
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	key := req.SqlPattern
	oper := newOperator(req, sqlReg)
	operators, ok1 := h.operators[req.Name]
	if ok1 {
		prev, ok2 := operators[key]
		if ok2 {
			h.logger.Warn("overwrite operation", zap.Stringer("previous operation", prev), zap.Stringer("current operation", oper))
		}
	} else {
		operators = make(map[string]*Operator)
		h.operators[req.Name] = operators
	}
	operators[key] = oper
	h.logger.Info("set operation", zap.Stringer("operation", oper))
	return nil
}

// Get tries to get an operator by taskName and SQLs, returns key and operator
func (h *Holder) Get(taskName string, sqls []string) (string, *Operator) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	operators, ok := h.operators[taskName]
	if !ok {
		return "", nil
	}

	for _, sql := range sqls {
		for key, oper := range operators {
			if oper.matchPattern(sql) { // matched one SQL of all is enough
				h.logger.Info("get a matched operator", zap.Stringer("operation", oper), zap.String("key", key), zap.String("sql", sql))
				return key, oper.clone()
			}
		}
	}
	return "", nil
}

// Remove removes the operator with the key
func (h *Holder) Remove(taskName, key string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	operators, ok := h.operators[taskName]
	if !ok {
		return
	}

	oper, ok := operators[key]
	if ok {
		delete(operators, key)
		if len(operators) == 0 {
			delete(h.operators, taskName)
		}
		h.logger.Info("remove operator", zap.Stringer("operation", oper))
	}
}
