package filter

import (
	. "github.com/pingcap/check"
)

func (s *testFilterSuite) TestWhiteFilter(c *C) {
	cases := []struct {
		DoTables []*Table
		DoDBs    []string
		Input    []*Table
		Output   []*Table
	}{
		{
			DoTables: nil,
			DoDBs:    nil,
			Input:    nil,
			Output:   nil,
		},
		{
			DoTables: nil,
			DoDBs:    nil,
			Input:    []*Table{{Schema: "foo", Name: "bar"}},
			Output:   []*Table{{Schema: "foo", Name: "bar"}},
		}, {
			DoTables: nil,
			DoDBs:    []string{"foo"},
			Input:    []*Table{{Schema: "foo", Name: "bar"}},
			Output:   []*Table{{Schema: "foo", Name: "bar"}},
		}, {
			DoTables: nil,
			DoDBs:    []string{"foo"},
			Input:    []*Table{{Schema: "foo1", Name: "bar"}},
			Output:   nil,
		}, {
			DoTables: nil,
			DoDBs:    []string{"foo"},
			Input:    []*Table{{Schema: "foo", Name: "bar"}, {Schema: "foo1", Name: "bar"}},
			Output:   []*Table{{Schema: "foo", Name: "bar"}},
		}, {
			DoTables: []*Table{{Schema: "foo", Name: "bar"}},
			DoDBs:    nil,
			Input:    []*Table{{Schema: "foo", Name: "bar"}, {Schema: "foo1", Name: "bar1"}},
			Output:   []*Table{{Schema: "foo", Name: "bar"}},
		}, {
			DoTables: []*Table{{Schema: "foo", Name: "bar"}},
			DoDBs:    []string{"foo1"},
			Input:    []*Table{{Schema: "foo"}, {Schema: "foo1"}, {Schema: "foo2"}},
			Output:   []*Table{{Schema: "foo"}, {Schema: "foo1"}},
		}, {
			// regexp
			DoTables: nil,
			DoDBs:    []string{"~^foo"},
			Input:    []*Table{{Schema: "foo", Name: "bar"}, {Schema: "foo1", Name: "bar"}, {Schema: "fff", Name: ""}},
			Output:   []*Table{{Schema: "foo", Name: "bar"}, {Schema: "foo1", Name: "bar"}},
		}, {
			DoTables: []*Table{{Schema: "foo", Name: "~^sbtest-\\d"}},
			DoDBs:    nil,
			Input:    []*Table{{Schema: "foo", Name: "sbtest"}, {Schema: "foo", Name: "sbtest-1"}, {Schema: "foo", Name: "sbtest-0001"}},
			Output:   []*Table{{Schema: "foo", Name: "sbtest-1"}, {Schema: "foo", Name: "sbtest-0001"}},
		},
	}

	for _, t := range cases {
		ft := New(&Rules{
			DoDBs:    t.DoDBs,
			DoTables: t.DoTables,
		})
		got := ft.WhiteFilter(t.Input)
		c.Logf("got %+v, expected %+v", got, t.Output)
		c.Assert(got, DeepEquals, t.Output)
	}
}

func (s *testFilterSuite) TestBlackFilter(c *C) {
	cases := []struct {
		IgnoreTables []*Table
		IgnoreDBs    []string
		Input        []*Table
		Output       []*Table
	}{
		{
			IgnoreTables: nil,
			IgnoreDBs:    nil,
			Input:        nil,
			Output:       nil,
		}, {
			IgnoreTables: nil,
			IgnoreDBs:    nil,
			Input:        []*Table{{Schema: "foo", Name: "bar"}},
			Output:       []*Table{{Schema: "foo", Name: "bar"}},
		}, {
			IgnoreTables: nil,
			IgnoreDBs:    []string{"foo"},
			Input:        []*Table{{Schema: "foo", Name: "bar"}, {Schema: "foo1", Name: "bar"}},
			Output:       []*Table{{Schema: "foo1", Name: "bar"}},
		}, {
			IgnoreTables: []*Table{{Schema: "foo", Name: "bar"}, {Schema: "foo1", Name: "bar1"}},
			IgnoreDBs:    nil,
			Input:        []*Table{{Schema: "foo", Name: "bar1"}, {Schema: "foo1", Name: "bar"}, {Schema: "foo", Name: "bar"}, {Schema: "foo1", Name: "bar1"}},
			Output:       []*Table{{Schema: "foo", Name: "bar1"}, {Schema: "foo1", Name: "bar"}},
		}, {
			IgnoreTables: []*Table{{Schema: "foo", Name: "bar"}},
			IgnoreDBs:    []string{"foo1"},
			Input:        []*Table{{Schema: "foo", Name: "bar1"}, {Schema: "foo1", Name: "bar"}, {Schema: "foo", Name: "bar"}, {Schema: "foo1", Name: "bar1"}},
			Output:       []*Table{{Schema: "foo", Name: "bar1"}},
		}, {
			//regexp
			IgnoreTables: []*Table{{Schema: "foo", Name: "~^bar"}},
			IgnoreDBs:    []string{"~^foo1"},
			Input:        []*Table{{Schema: "foo", Name: "bar1"}, {Schema: "foo1", Name: "bar"}, {Schema: "foo", Name: "bar"}, {Schema: "foo1", Name: "bar1"}},
			Output:       nil,
		},
	}

	for _, t := range cases {
		ft := New(&Rules{
			IgnoreDBs:    t.IgnoreDBs,
			IgnoreTables: t.IgnoreTables,
		})

		got := ft.BlackFilter(t.Input)
		c.Logf("got %+v, expected %+v", got, t.Output)
		c.Assert(got, DeepEquals, t.Output)
	}

}

func (s *testFilterSuite) TestMixedFilter(c *C) {
	cases := []struct {
		DoDBs        []string
		IgnoreDBs    []string
		DoTables     []*Table
		IgnoreTables []*Table
		Input        []*Table
		Output       []*Table
	}{
		{
			DoDBs:     []string{"foo"},
			IgnoreDBs: []string{"foo"},
			Input:     []*Table{{Schema: "foo"}, {Schema: "foo", Name: "bar"}},
			Output:    nil, // is it right?
		}, {
			DoDBs:        []string{"foo", "foo1"},
			IgnoreTables: []*Table{{Schema: "foo", Name: "bar"}, {Schema: "foo1", Name: "bar1"}},
			Input:        []*Table{{Schema: "foo", Name: "bar"}, {Schema: "foo", Name: "bar1"}, {Schema: "foo1", Name: "bar1"}, {Schema: "foo1", Name: "bar"}},
			Output:       []*Table{{Schema: "foo", Name: "bar1"}, {Schema: "foo1", Name: "bar"}},
		},
	}

	for _, t := range cases {
		ft := New(&Rules{
			DoDBs:        t.DoDBs,
			DoTables:     t.DoTables,
			IgnoreDBs:    t.IgnoreDBs,
			IgnoreTables: t.IgnoreTables,
		})

		got := ft.WhiteFilter(t.Input)
		c.Logf("got from whitelist %+v", got)
		got = ft.BlackFilter(got)
		c.Logf("got %+v, expected %+v", got, t.Output)
		c.Assert(got, DeepEquals, t.Output)
	}
}
