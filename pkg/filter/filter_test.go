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
		// ensure empty rules won't crash
		{
			rules: &Rules{
				IgnoreDBs: []string{""},
			},
			Input:  []*Table{{"", "a"}, {"a", ""}},
			Output: []*Table{{"a", ""}},
		},
		// ensure the patterns without `~` won't be accidentally parsed as regexp
		{
			rules: &Rules{
				IgnoreDBs: []string{"foo[bar]", "foo?", "special\\"},
			},
			Input:  []*Table{{"foor", "a"}, {"foo[bar]", "b"}, {"fo", "c"}, {"foo?", "d"}, {"special\\", "e"}},
			Output: []*Table{{"foor", "a"}, {"fo", "c"}},
		},
		// ensure the rules are really case-insensitive
		{
			rules: &Rules{
				IgnoreDBs:    []string{"~^FOO"},
				IgnoreTables: []*Table{{"~.*", "~FoO$"}},
			},
			Input:  []*Table{{"FOO1", "a"}, {"foo2", "b"}, {"BoO3", "cFoO"}, {"Foo4", "dfoo"}, {"5", "5"}},
			Output: []*Table{{"5", "5"}},
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

func (s *testFilterSuite) TestCaseSensitive(c *C) {
	// ensure case-sensitive rules are really case-sensitive
	rules := &Rules{
		IgnoreDBs:    []string{"~^FOO"},
		IgnoreTables: []*Table{{"~.*", "~FoO$"}},
	}
	r := New(true, rules)

	input := []*Table{{"FOO1", "a"}, {"foo2", "b"}, {"BoO3", "cFoO"}, {"Foo4", "dfoo"}, {"5", "5"}}
	actual := r.ApplyOn(input)
	expected := []*Table{{"foo2", "b"}, {"Foo4", "dfoo"}, {"5", "5"}}
	c.Logf("got %+v, expected %+v", actual, expected)
	c.Assert(actual, DeepEquals, expected)
}
