package loader

import (
	"testing"

	. "github.com/pingcap/check"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testUtilSuite{})

type testUtilSuite struct{}

func (t *testUtilSuite) TestSQLReplace(c *C) {

	var replaceTests = []struct {
		in       string
		old, new string
		out      string
	}{
		{"create database `xyz`", "xyz", "abc", "create database `abc`"},
		{"create database `xyz`", "crea", "abc", "create database `xyz`"},
		{"create database `xyz`", "create", "abc", "create database `xyz`"},
		{"create database `xyz`", "data", "abc", "create database `xyz`"},
		{"create database `xyz`", "database", "abc", "create database `xyz`"},
		{"create table `xyz`", "xyz", "abc", "create table `abc`"},
		{"create table `xyz`", "crea", "abc", "create table `xyz`"},
		{"create table `xyz`", "create", "abc", "create table `xyz`"},
		{"create table `xyz`", "tab", "abc", "create table `xyz`"},
		{"create table `xyz`", "table", "abc", "create table `xyz`"},
		{"insert into `xyz`", "xyz", "abc", "insert into `abc`"},
		{"insert into `xyz`", "ins", "abc", "insert into `xyz`"},
		{"insert into `xyz`", "insert", "abc", "insert into `xyz`"},
		{"insert into `xyz`", "in", "abc", "insert into `xyz`"},
		{"insert into `xyz`", "into", "abc", "insert into `xyz`"},
		{"INSERT INTO `xyz`", "xyz", "abc", "INSERT INTO `abc`"},
	}

	for _, tt := range replaceTests {
		c.Assert(SQLReplace(tt.in, tt.old, tt.new), Equals, tt.out)
	}

}

func (t *testUtilSuite) TestShortSha1(c *C) {
	c.Assert(shortSha1("/tmp/test_sha1_short_6"), Equals, "97b645")
}
