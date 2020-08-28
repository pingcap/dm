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

package utils

import (
	"time"

	. "github.com/pingcap/check"
	"github.com/siddontang/go-mysql/mysql"
)

func (t *testUtilsSuite) TestDecodeBinlogPosition(c *C) {
	testCases := []struct {
		pos      string
		isErr    bool
		expecetd *mysql.Position
	}{
		{"()", true, nil},
		{"(,}", true, nil},
		{"(,)", true, nil},
		{"(mysql-bin.00001,154)", false, &mysql.Position{Name: "mysql-bin.00001", Pos: 154}},
		{"(mysql-bin.00001, 154)", false, &mysql.Position{Name: "mysql-bin.00001", Pos: 154}},
		{"(mysql-bin.00001\t,  154)", false, &mysql.Position{Name: "mysql-bin.00001", Pos: 154}},
	}

	for _, tc := range testCases {
		pos, err := DecodeBinlogPosition(tc.pos)
		if tc.isErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			c.Assert(pos, DeepEquals, tc.expecetd)
		}
	}
}

func (t *testUtilsSuite) TestCompareBinlogPos(c *C) {
	testCases := []struct {
		pos1      mysql.Position
		pos2      mysql.Position
		deviation float64
		cmp       int
	}{
		{
			mysql.Position{
				Name: "bin-000001",
				Pos:  12345,
			},
			mysql.Position{
				Name: "bin-000002",
				Pos:  4,
			},
			0,
			-1,
		},
		{
			mysql.Position{
				Name: "bin-000003",
				Pos:  4,
			},
			mysql.Position{
				Name: "bin-000002",
				Pos:  154,
			},
			0,
			1,
		},
		{
			mysql.Position{
				Name: "bin-000002",
				Pos:  154,
			},
			mysql.Position{
				Name: "bin-000002",
				Pos:  300,
			},
			190,
			0,
		},
		{
			mysql.Position{
				Name: "bin-000002",
				Pos:  300,
			},
			mysql.Position{
				Name: "bin-000002",
				Pos:  154,
			},
			190,
			0,
		},
		{
			mysql.Position{
				Name: "bin-000002",
				Pos:  30000,
			},
			mysql.Position{
				Name: "bin-000002",
				Pos:  30000,
			},
			0,
			0,
		},
		{
			mysql.Position{
				Name: "bin-000002",
				Pos:  154,
			},
			mysql.Position{
				Name: "bin-000002",
				Pos:  300,
			},
			0,
			-1,
		},
		{
			mysql.Position{
				Name: "bin-000002",
				Pos:  1540,
			},
			mysql.Position{
				Name: "bin-000002",
				Pos:  300,
			},
			0,
			1,
		},
	}

	for _, tc := range testCases {
		cmp := CompareBinlogPos(tc.pos1, tc.pos2, tc.deviation)
		c.Assert(cmp, Equals, tc.cmp)
	}

}

func (t *testUtilsSuite) TestWaitSomething(c *C) {
	var (
		backoff  = 10
		waitTime = 10 * time.Millisecond
		count    = 0
	)

	// wait fail
	f1 := func() bool {
		count++
		return false
	}
	c.Assert(WaitSomething(backoff, waitTime, f1), IsFalse)
	c.Assert(count, Equals, backoff)

	count = 0 // reset
	// wait success
	f2 := func() bool {
		count++
		return count >= 5
	}

	c.Assert(WaitSomething(backoff, waitTime, f2), IsTrue)
	c.Assert(count, Equals, 5)
}

func (t *testUtilsSuite) TestHidePassword(c *C) {
	strs := []struct {
		old string
		new string
	}{
		{ // operate source
			`from:\n  host: 127.0.0.1\n  user: root\n  password: /Q7B9DizNLLTTfiZHv9WoEAKamfpIUs=\n  port: 3306\n`,
			`from:\n  host: 127.0.0.1\n  user: root\n  password: ******\n  port: 3306\n`,
		}, { // operate source empty password
			`from:\n  host: 127.0.0.1\n  user: root\n  password: \n  port: 3306\n`,
			`from:\n  host: 127.0.0.1\n  user: root\n  password: ******\n  port: 3306\n`,
		}, { // start task
			`\n\ntarget-database:\n  host: \"127.0.0.1\"\n  port: 4000\n  user: \"test\"\n  password: \"/Q7B9DizNLLTTfiZHv9WoEAKamfpIUs=\"\n\nmysql-instances:\n  - source-id: \"mysql-replica-01\"\n`,
			`\n\ntarget-database:\n  host: \"127.0.0.1\"\n  port: 4000\n  user: \"test\"\n  password: \"******\"\n\nmysql-instances:\n  - source-id: \"mysql-replica-01\"\n`,
		}, { // start task empty passowrd
			`\n\ntarget-database:\n  host: \"127.0.0.1\"\n  port: 4000\n  user: \"test\"\n  password: \"\"\n\nmysql-instances:\n  - source-id: \"mysql-replica-01\"\n`,
			`\n\ntarget-database:\n  host: \"127.0.0.1\"\n  port: 4000\n  user: \"test\"\n  password: \"******\"\n\nmysql-instances:\n  - source-id: \"mysql-replica-01\"\n`,
		},
	}
	for _, str := range strs {
		c.Assert(HidePassword(str.old), Equals, str.new)
	}
}

func (t *testUtilsSuite) TestUnwrapScheme(c *C) {
	cases := []struct {
		old string
		new string
	}{
		{
			"http://0.0.0.0:123",
			"0.0.0.0:123",
		},
		{
			"https://0.0.0.0:123",
			"0.0.0.0:123",
		},
		{
			"http://abc.com:123",
			"abc.com:123",
		},
		{
			"httpsdfpoje.com",
			"httpsdfpoje.com",
		},
		{
			"",
			"",
		},
	}
	for _, ca := range cases {
		c.Assert(UnwrapScheme(ca.old), Equals, ca.new)
	}
}

func (t *testUtilsSuite) TestWrapSchemes(c *C) {
	cases := []struct {
		old   string
		http  string
		https string
	}{
		{
			"0.0.0.0:123",
			"http://0.0.0.0:123",
			"https://0.0.0.0:123",
		},
		{
			"abc.com:123",
			"http://abc.com:123",
			"https://abc.com:123",
		},
		{
			// if input has wrong scheme, don't correct it (maybe user deliberately?)
			"abc.com:123,http://abc.com:123,0.0.0.0:123,https://0.0.0.0:123",
			"http://abc.com:123,http://abc.com:123,http://0.0.0.0:123,https://0.0.0.0:123",
			"https://abc.com:123,http://abc.com:123,https://0.0.0.0:123,https://0.0.0.0:123",
		},
		{
			"",
			"",
			"",
		},
	}
	for _, ca := range cases {
		c.Assert(WrapSchemes(ca.old, false), Equals, ca.http)
		c.Assert(WrapSchemes(ca.old, true), Equals, ca.https)
	}
}
