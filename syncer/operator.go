package syncer

import (
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-enterprise-tools/dm/pb"
	"github.com/siddontang/go-mysql/mysql"
)

//TODO: support GTID?

// Operator contains an opration for specified binlog pos.
type Operator struct {
	Pos mysql.Position
	Op  pb.SQLOp

	// if op == SQLOp_REPLACE, it will has targets
	Targets []string
}

// Operate do operation to pos and sql.
func (o *Operator) Operate() (sqls []string, err error) {
	log.Infof("[syncer] apply operator %+v", o)
	switch o.Op {
	case pb.SQLOp_SKIP:
		return []string{}, nil

	case pb.SQLOp_REPLACE:
		if len(o.Targets) == 0 {
			log.Errorf("operation replace has no target %v", o)
		}
		return o.Targets, nil

	default:
		return nil, errors.Errorf("invalid operation %v %v", o.Op, o)
	}
}

// SetOperator sets an operator to syncer.
func (s *Syncer) SetOperator(operator *Operator) {
	s.operatorsMu.Lock()

	log.Infof("[syncer] set operator %+v", operator)
	key := operator.Pos.String()
	s.operatorsMu.operators[key] = operator

	s.operatorsMu.Unlock()
}

// DelOperator deleles an operator from syncer.
func (s *Syncer) DelOperator(pos mysql.Position) {
	s.operatorsMu.Lock()

	key := pos.String()
	log.Infof("[syncer] delete operator %v", s.operatorsMu.operators[key])
	delete(s.operatorsMu.operators, key)

	s.operatorsMu.Unlock()
}

// GetOperator returns an operator by a mysql.Position.
func (s *Syncer) GetOperator(pos mysql.Position) *Operator {
	s.operatorsMu.RLock()
	defer s.operatorsMu.RUnlock()

	key := pos.String()
	operator, ok := s.operatorsMu.operators[key]
	if !ok {
		return nil
	}
	return operator
}
