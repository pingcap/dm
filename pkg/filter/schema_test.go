package filter

import (
	"testing"

	. "github.com/pingcap/check"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

type testFilterSuite struct{}

var _ = Suite(&testFilterSuite{})

func (s *testFilterSuite) TestIsSystemSchema(c *C) {
	cases := []struct {
		name     string
		expected bool
	}{
		{"information_schema", true},
		{"performance_schema", true},
		{"mysql", true},
		{"sys", true},
		{"INFORMATION_SCHEMA", true},
		{"PERFORMANCE_SCHEMA", true},
		{"MYSQL", true},
		{"SYS", true},
		{"not_system_schema", false},
	}

	for _, t := range cases {
		c.Assert(IsSystemSchema(t.name), Equals, t.expected)
	}

}
