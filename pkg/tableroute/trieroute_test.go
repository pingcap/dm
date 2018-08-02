package route

import (
	"fmt"
	"testing"

	. "github.com/pingcap/check"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testRouteSuite{})

type testRouteSuite struct{}

func (t *testRouteSuite) TestRoute(c *C) {
	r := NewTrieRouter()
	t.testInsert(c, r)
	t.testMatch(c, r)
}

func (t *testRouteSuite) testInsert(c *C, r TableRouter) {
	var err error
	cases := map[string]map[string][]string{
		"?bc":  {"abc*": {"abc", "abc1"}, "xyz*": {"abc", "abc2"}},
		"a?c":  {"xyz*": {"abc", "abc2"}, "abc*": {"abc", "abc2"}},
		"ab*":  {"abc*": {"abc", "abc3"}},
		"a*":   {"": {"abc", ""}},
		"xyz":  {"xyz*": {"abc", "abc4"}},
		"xyy*": {"xyz*": {"xyz", "abc5"}},
	}
	for schema, targets := range cases {
		for table, target := range targets {
			err = r.Insert(schema, table, target[0], target[1])
			c.Assert(err, IsNil)
		}
	}
	rules := r.AllRules()
	c.Assert(rules, DeepEquals, cases)
	// test error
	err = r.Insert("ab**", "", "error", "")
	c.Assert(err, NotNil)
	err = r.Insert("ab*", "", "error", "")
	c.Assert(err, NotNil)
	// check all rules again
	rules = r.AllRules()
	c.Assert(rules, DeepEquals, cases)
}

func (t *testRouteSuite) testMatch(c *C, r TableRouter) {
	cases := [][]string{
		{"dbc", "abc1", "abc", "abc1"},
		{"dbc", "abc2", "abc", "abc1"},
		{"adc", "abc1", "abc", "abc2"},
		{"adc", "xyz1", "abc", "abc2"},
		{"axc", "xyz1", "abc", "abc2"},
		{"dbc", "xxx", "abc", ""},
	}
	cache := make(map[string][]string)
	for _, tc := range cases {
		s, t := r.Match(tc[0], tc[1])
		c.Assert(s, Equals, tc[2])
		c.Assert(t, Equals, tc[3])
		cache[fmt.Sprintf("`%s`.`%s`", tc[0], tc[1])] = []string{tc[2], tc[3]}
	}

	// test cache
	trie, ok := r.(*trieRouter)
	c.Assert(ok, IsTrue)
	c.Assert(trie.cache, DeepEquals, cache)
	// test schema mathced
	shema, table := r.Match("dbc", "")
	c.Assert(shema, Equals, "abc")
	c.Assert(table, Equals, "")
	c.Assert(trie.cache["`dbc`"], DeepEquals, []string{"abc", ""})
}
