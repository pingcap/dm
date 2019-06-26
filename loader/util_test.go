// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package loader

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
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

func (t *testUtilSuite) TestGenerateSchemaCreateFile(c *C) {
	dir := c.MkDir()
	testCases := []struct {
		schema    string
		createSQL string
	}{
		{
			"loader_test",
			"CREATE DATABASE `loader_test`;\n",
		}, {
			"loader`test",
			"CREATE DATABASE `loader``test`;\n",
		},
	}
	for _, testCase := range testCases {
		err := generateSchemaCreateFile(dir, testCase.schema)
		c.Assert(err, IsNil)

		file, err := os.Open(path.Join(dir, fmt.Sprintf("%s-schema-create.sql", testCase.schema)))
		c.Assert(err, IsNil)

		data, err := ioutil.ReadAll(file)
		c.Assert(err, IsNil)
		c.Assert(string(data), Equals, testCase.createSQL)
	}
}
