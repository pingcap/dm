package binlog

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/siddontang/go-mysql/mysql"
)

var _ = Suite(&testPositionSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testPositionSuite struct {
}

func (t *testPositionSuite) TestPositionFromStr(c *C) {
	emptyPos := mysql.Position{}
	cases := []struct {
		str      string
		pos      mysql.Position
		hasError bool
	}{
		{
			str:      "mysql-bin.000001",
			pos:      emptyPos,
			hasError: true,
		},
		{
			str:      "234",
			pos:      emptyPos,
			hasError: true,
		},
		{
			str:      "mysql-bin.000001:abc",
			pos:      emptyPos,
			hasError: true,
		},
		{
			str:      "mysql-bin.000001:234:567",
			pos:      emptyPos,
			hasError: true,
		},
		{
			str:      "mysql-bin.000001:234",
			pos:      mysql.Position{Name: "mysql-bin.000001", Pos: 234},
			hasError: false,
		},
	}

	for _, cs := range cases {
		pos, err := PositionFromStr(cs.str)
		if cs.hasError {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
		}
		c.Assert(pos, DeepEquals, cs.pos)
	}
}
