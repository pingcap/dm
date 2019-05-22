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

package binlog

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
)

var _ = Suite(&testFilenameSuite{})

type testFilenameSuite struct {
}

func (t *testFilenameSuite) TestVerifyBinlogFilename(c *C) {
	cases := []struct {
		filename string
		valid    bool
	}{
		{
			// valid
			filename: "mysql-bin.666666",
			valid:    true,
		},
		{
			// empty filename
			filename: "",
		},
		{
			// negative seq number
			filename: "mysql-bin.-666666",
		},
		{
			// zero seq number
			filename: "mysql-bin.000000",
		},
		{
			// too many separators
			filename: "mysql.bin.666666",
		},
		{
			// too less separators
			filename: "mysql-bin",
		},
		{
			// invalid seq number
			filename: "mysql-bin.666abc",
		},
		{
			// invalid seq number
			filename: "mysql-bin.def666",
		},
	}

	for _, cs := range cases {
		err := VerifyBinlogFilename(cs.filename)
		if cs.valid {
			c.Assert(err, IsNil)
		} else {
			c.Assert(errors.Cause(err), Equals, ErrInvalidBinlogFilename)
		}
	}
}
