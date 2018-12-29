package utils

import (
	. "github.com/pingcap/check"
	"github.com/siddontang/go-mysql/mysql"
)

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
