package syncer

import (
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-enterprise-tools/pkg/gtid"
)

func (s *testSyncerSuite) TestGTID(c *C) {
	matserUUIDs := []string{
		"53ea0ed1-9bf8-11e6-8bea-64006a897c73",
		"53ea0ed1-9bf8-11e6-8bea-64006a897c72",
		"53ea0ed1-9bf8-11e6-8bea-64006a897c71",
	}

	cases := []struct {
		flavor        string
		masterIDs     []interface{}
		selfGTIDstr   string
		masterGTIDStr string
		exepctedStr   string
	}{
		{"mariadb", []interface{}{uint32(1)}, "1-1-1,2-2-2", "1-1-12,4-4-4", "1-1-1,4-4-4"},
		{"mariadb", []interface{}{uint32(1)}, "2-2-2", "1-2-12,2-2-3,4-4-4", "2-2-2,4-4-4"},
		{"mariadb", []interface{}{uint32(1)}, "", "1-1-12,4-4-4", "4-4-4"},
		{"mariadb", []interface{}{uint32(1)}, "2-2-2", "", ""},
		{"mariadb", []interface{}{uint32(1), uint32(3)}, "1-1-1,3-3-4,2-2-2", "1-1-12,3-3-8,4-4-4", "1-1-1,3-3-4,4-4-4"},
		{"mariadb", []interface{}{uint32(1), uint32(3)}, "2-2-2", "1-2-12,2-2-3,3-3-8,4-4-4", "2-2-2,4-4-4"},
		{"mysql", []interface{}{matserUUIDs[0]}, fmt.Sprintf("%s:1-2,%s:1-2", matserUUIDs[0], matserUUIDs[1]), fmt.Sprintf("%s:1-12,%s:1-4", matserUUIDs[0], matserUUIDs[2]), fmt.Sprintf("%s:1-2,%s:1-4", matserUUIDs[0], matserUUIDs[2])},
		{"mysql", []interface{}{matserUUIDs[0]}, fmt.Sprintf("%s:1-2", matserUUIDs[1]), fmt.Sprintf("%s:1-12,%s:1-3,%s:1-4", matserUUIDs[0], matserUUIDs[1], matserUUIDs[2]), fmt.Sprintf("%s:1-2,%s:1-4", matserUUIDs[1], matserUUIDs[2])},
		{"mysql", []interface{}{matserUUIDs[0]}, "", fmt.Sprintf("%s:1-12,%s:1-4", matserUUIDs[0], matserUUIDs[1]), fmt.Sprintf("%s:1-4", matserUUIDs[1])},
		{"mysql", []interface{}{matserUUIDs[0]}, fmt.Sprintf("%s:1-2", matserUUIDs[1]), "", ""},
		{"mysql", []interface{}{matserUUIDs[0], matserUUIDs[1]}, fmt.Sprintf("%s:1-2,%s:1-2", matserUUIDs[0], matserUUIDs[1]), fmt.Sprintf("%s:1-12,%s:1-4,%s:1-4", matserUUIDs[0], matserUUIDs[1], matserUUIDs[2]), fmt.Sprintf("%s:1-2,%s:1-2,%s:1-4", matserUUIDs[0], matserUUIDs[1], matserUUIDs[2])},
		{"mysql", []interface{}{matserUUIDs[0], matserUUIDs[2]}, fmt.Sprintf("%s:1-2", matserUUIDs[1]), fmt.Sprintf("%s:1-12,%s:1-3,%s:1-4", matserUUIDs[0], matserUUIDs[1], matserUUIDs[2]), fmt.Sprintf("%s:1-2", matserUUIDs[1])},
	}

	for _, cs := range cases {
		selfGTIDSet, err := gtid.ParserGTID(cs.flavor, cs.selfGTIDstr)
		c.Assert(err, IsNil)
		newGTIDSet, err := gtid.ParserGTID(cs.flavor, cs.masterGTIDStr)
		c.Assert(err, IsNil)
		excepted, err := gtid.ParserGTID(cs.flavor, cs.exepctedStr)
		c.Assert(err, IsNil)

		err = selfGTIDSet.Replace(newGTIDSet, cs.masterIDs)
		c.Logf("%s %s %s", selfGTIDSet, newGTIDSet, excepted)
		c.Assert(err, IsNil)
		c.Assert(selfGTIDSet.Origin().Equal(excepted.Origin()), IsTrue)
		c.Assert(newGTIDSet.Origin().Equal(excepted.Origin()), IsTrue)
	}
}
