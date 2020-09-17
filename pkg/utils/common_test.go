// Copyright 2020 PingCAP, Inc.
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
	"bytes"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
)

var _ = Suite(&testCommonSuite{})

type testCommonSuite struct {
}

func (s *testCommonSuite) TestTrimCtrlChars(c *C) {
	ddl := "create table if not exists foo.bar(id int)"
	controlChars := make([]byte, 0, 33)
	nul := byte(0x00)
	for i := 0; i < 32; i++ {
		controlChars = append(controlChars, nul)
		nul++
	}
	controlChars = append(controlChars, 0x7f)

	parser2 := parser.New()
	var buf bytes.Buffer
	for _, char := range controlChars {
		buf.WriteByte(char)
		buf.WriteByte(char)
		buf.WriteString(ddl)
		buf.WriteByte(char)
		buf.WriteByte(char)

		newDDL := TrimCtrlChars(buf.String())
		c.Assert(newDDL, Equals, ddl)

		_, err := parser2.ParseOneStmt(newDDL, "", "")
		c.Assert(err, IsNil)
		buf.Reset()
	}
}
