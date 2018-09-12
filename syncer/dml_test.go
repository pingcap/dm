package syncer

import (
	"math"
	"strconv"

	. "github.com/pingcap/check"
)

func (s *testSyncerSuite) TestCastUnsigned(c *C) {
	// ref: https://dev.mysql.com/doc/refman/5.7/en/integer-types.html
	cases := []struct {
		data     interface{}
		unsigned bool
		Type     string
		expected interface{}
	}{
		{int8(-math.Exp2(7)), false, "tinyint(4)", int8(-math.Exp2(7))}, // TINYINT
		{int8(-math.Exp2(7)), true, "tinyint(3) unsigned", uint8(math.Exp2(7))},
		{int16(-math.Exp2(15)), false, "smallint(6)", int16(-math.Exp2(15))}, //SMALLINT
		{int16(-math.Exp2(15)), true, "smallint(5) unsigned", uint16(math.Exp2(15))},
		{int32(-math.Exp2(23)), false, "mediumint(9)", int32(-math.Exp2(23))}, //MEDIUMINT
		{int32(-math.Exp2(23)), true, "mediumint(8) unsigned", uint32(math.Exp2(23))},
		{int32(-math.Exp2(31)), false, "int(11)", int32(-math.Exp2(31))}, // INT
		{int32(-math.Exp2(31)), true, "int(10) unsigned", uint32(math.Exp2(31))},
		{int64(-math.Exp2(63)), false, "bigint(20)", int64(-math.Exp2(63))},                                 // BIGINT
		{int64(-math.Exp2(63)), true, "bigint(20) unsigned", strconv.FormatUint(uint64(math.Exp2(63)), 10)}, // special case use string to represent uint64
	}
	for _, cs := range cases {
		obtained := castUnsigned(cs.data, cs.unsigned, cs.Type)
		c.Assert(obtained, Equals, cs.expected)
	}
}
