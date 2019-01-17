package operator

import (
	"testing"

	. "github.com/pingcap/check"

	"github.com/pingcap/dm/dm/pb"
)

var _ = Suite(&testOperatorSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testOperatorSuite struct {
}

func (o *testOperatorSuite) TestOperator(c *C) {
	h := NewHolder()

	// nil request
	err := h.Set(nil)
	c.Assert(err, NotNil)

	// not support op
	req := &pb.HandleSQLsRequest{Name: "task-A", Op: pb.SQLOp_INJECT}
	err = h.Set(req)
	c.Assert(err, NotNil)

	// no --sharding
	req.Op = pb.SQLOp_SKIP
	err = h.Set(req)
	c.Assert(err, NotNil)

	// with --binlog-pos
	req.Sharding = true
	req.BinlogPos = "mysql-bin.000001:234"
	err = h.Set(req)
	c.Assert(err, NotNil)

	// without --sql-pattern
	req.BinlogPos = ""
	err = h.Set(req)
	c.Assert(err, NotNil)

	// valid
	req.SqlPattern = "~(?i)ALTER\\s+TABLE\\s+`db1`.`tbl1`\\s+ADD\\s+COLUMN\\s+col1\\s+INT"
	err = h.Set(req)
	c.Assert(err, IsNil)

	// get, SQLs mismatch
	sqls := []string{"INSERT INTO `d1`.`t1` VALUES (1, 2)"}
	key, oper := h.Get(req.Name, sqls)
	c.Assert(key, Equals, "")
	c.Assert(oper, IsNil)

	// get, taskName mismatch
	sqls = []string{"ALTER TABLE `db1`.`tbl1` ADD COLUMN col1 INT"}
	key, oper = h.Get("not-exist-task", sqls)
	c.Assert(key, Equals, "")
	c.Assert(oper, IsNil)

	// get, matched
	key, oper = h.Get(req.Name, sqls)
	c.Assert(key, Equals, req.SqlPattern)
	c.Assert(oper, NotNil)
	c.Assert(oper.Req, DeepEquals, req)

	// remove
	h.Remove(req.Name, key)

	// get, not exists
	key, oper = h.Get(req.Name, sqls)
	c.Assert(key, Equals, "")
	c.Assert(oper, IsNil)
}
