package utils

import (
	. "github.com/pingcap/check"
	"github.com/siddontang/go-mysql/mysql"
)

func (t *testUtilsSuite) TestDecodeBinlogPosition(c *C) {
	testCases := []struct {
		pos      string
		isErr    bool
		expecetd *mysql.Position
	}{
		{"()", true, nil},
		{"(,}", true, nil},
		{"(,)", true, nil},
		{"(mysql-bin.00001,154)", false, &mysql.Position{Name: "mysql-bin.00001", Pos: 154}},
		{"(mysql-bin.00001, 154)", false, &mysql.Position{Name: "mysql-bin.00001", Pos: 154}},
		{"(mysql-bin.00001\t,  154)", false, &mysql.Position{Name: "mysql-bin.00001", Pos: 154}},
	}

	for _, tc := range testCases {
		pos, err := DecodeBinlogPosition(tc.pos)
		if tc.isErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			c.Assert(pos, DeepEquals, tc.expecetd)
		}
	}
}

func (t *testUtilsSuite) TestCompareBinlogPos(c *C) {
	testCases := []struct {
		pos1      mysql.Position
		pos2      mysql.Position
		deviation float64
		cmp       int
	}{
		{
			mysql.Position{
				Name: "bin-000001",
				Pos:  12345,
			},
			mysql.Position{
				Name: "bin-000002",
				Pos:  4,
			},
			0,
			-1,
		},
		{
			mysql.Position{
				Name: "bin-000003",
				Pos:  4,
			},
			mysql.Position{
				Name: "bin-000002",
				Pos:  154,
			},
			0,
			1,
		},
		{
			mysql.Position{
				Name: "bin-000002",
				Pos:  154,
			},
			mysql.Position{
				Name: "bin-000002",
				Pos:  300,
			},
			190,
			0,
		},
		{
			mysql.Position{
				Name: "bin-000002",
				Pos:  300,
			},
			mysql.Position{
				Name: "bin-000002",
				Pos:  154,
			},
			190,
			0,
		},
		{
			mysql.Position{
				Name: "bin-000002",
				Pos:  30000,
			},
			mysql.Position{
				Name: "bin-000002",
				Pos:  30000,
			},
			0,
			0,
		},
		{
			mysql.Position{
				Name: "bin-000002",
				Pos:  154,
			},
			mysql.Position{
				Name: "bin-000002",
				Pos:  300,
			},
			0,
			-1,
		},
		{
			mysql.Position{
				Name: "bin-000002",
				Pos:  1540,
			},
			mysql.Position{
				Name: "bin-000002",
				Pos:  300,
			},
			0,
			1,
		},
	}

	for _, tc := range testCases {
		cmp := CompareBinlogPos(tc.pos1, tc.pos2, tc.deviation)
		c.Assert(cmp, Equals, tc.cmp)
	}

}
