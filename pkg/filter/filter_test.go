package filter

import (
	. "github.com/pingcap/check"
)

func (s *testFilterSuite) TestFilterOnSchema(c *C) {
	cases := []struct {
		rules  *Rules
		Input  []*Table
		Output []*Table
	}{
		// empty rules
		{
			rules: &Rules{
				IgnoreDBs: nil,
				DoDBs:     nil,
			},
			Input:  nil,
			Output: nil,
		},
		{
			rules: &Rules{
				IgnoreDBs: nil,
				DoDBs:     nil,
			},
			Input:  []*Table{{"foo", "bar"}, {"foo", ""}},
			Output: []*Table{{"foo", "bar"}, {"foo", ""}},
		},
		// schema-only rules
		{
			rules: &Rules{
				IgnoreDBs: []string{"foo"},
				DoDBs:     []string{"foo"},
			},
			Input:  []*Table{{"foo", "bar"}, {"foo", ""}, {"foo1", "bar"}, {"foo1", ""}},
			Output: []*Table{{"foo", "bar"}, {"foo", ""}},
		}, {
			rules: &Rules{
				IgnoreDBs: []string{"foo1"},
				DoDBs:     nil,
			},
			Input:  []*Table{{"foo", "bar"}, {"foo", ""}, {"foo1", "bar"}, {"foo1", ""}},
			Output: []*Table{{"foo", "bar"}, {"foo", ""}},
		},
		// DoTable rules
		{
			rules: &Rules{
				DoTables: []*Table{{"foo", "bar1"}},
			},
			Input:  []*Table{{"foo", "bar"}, {"foo", "bar1"}, {"foo", ""}, {"fff", "bar1"}},
			Output: []*Table{{"foo", "bar1"}, {"foo", ""}},
		},
		// ignoreTable rules
		{
			rules: &Rules{
				IgnoreTables: []*Table{{"foo", "bar"}},
				DoTables:     nil,
			},
			Input:  []*Table{{"foo", "bar"}, {"foo", "bar1"}, {"foo", ""}, {"fff", "bar1"}},
			Output: []*Table{{"foo", "bar1"}, {"foo", ""}, {"fff", "bar1"}},
		}, {
			// regexp
			rules: &Rules{
				IgnoreDBs:    nil,
				DoDBs:        []string{"~^foo"},
				IgnoreTables: []*Table{{"~^foo", "~^sbtest-\\d"}},
			},
			Input:  []*Table{{"foo", "sbtest"}, {"foo1", "sbtest-1"}, {"foo2", ""}, {"fff", "bar"}},
			Output: []*Table{{"foo", "sbtest"}, {"foo2", ""}},
		},
	}

	for _, t := range cases {
		ft := New(false, t.rules)
		got := ft.ApplyOn(t.Input)
		c.Logf("got %+v, expected %+v", got, t.Output)
		c.Assert(got, DeepEquals, t.Output)
	}
}

func (s *testFilterSuite) TestMaxBox(c *C) {
	rules := &Rules{
		DoTables: []*Table{
			{"test1", "t1"},
		},
		IgnoreTables: []*Table{
			{"test1", "t2"},
		},
	}

	r := New(false, rules)

	x := &Table{"test1", ""}
	res := r.ApplyOn([]*Table{x})
	c.Assert(res, HasLen, 1)
	c.Assert(res[0], DeepEquals, x)
}
