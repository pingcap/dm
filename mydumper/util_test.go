package mydumper

import (
	. "github.com/pingcap/check"
)

func (m *testMydumperSuite) TestParseArgs(c *C) {
	var tests = []struct {
		args     []string
		expected []string
	}{
		{[]string{"--regex", "'^(?!(mysql|information_schema|performance_schema))'"},
			[]string{"--regex", "^(?!(mysql|information_schema|performance_schema))"}},
		{[]string{"--regex", "\"^(?!(mysql|information_schema|performance_schema))\""},
			[]string{"--regex", "^(?!(mysql|information_schema|performance_schema))"}},
		{[]string{"--regex", "^(?!(mysql|information_schema|performance_schema))"},
			[]string{"--regex", "^(?!(mysql|information_schema|performance_schema))"}},
		{[]string{"--regex", "'sys"},
			[]string{"--regex", "'sys"}},
	}

	for _, t := range tests {
		parsed := ParseArgLikeBash(t.args)
		c.Assert(parsed, DeepEquals, t.expected)
	}
}
