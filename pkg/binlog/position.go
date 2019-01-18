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
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/mysql"
)

// PositionFromStr constructs a mysql.Position from a string representation like `mysql-bin.000001:2345`
func PositionFromStr(s string) (mysql.Position, error) {
	parsed := strings.Split(s, ":")
	if len(parsed) != 2 {
		return mysql.Position{}, errors.New("the format should be filename:pos")
	}
	pos, err := strconv.ParseUint(parsed[1], 10, 32)
	if err != nil {
		return mysql.Position{}, errors.New("the pos should be digital")
	}

	return mysql.Position{
		Name: parsed[0],
		Pos:  uint32(pos),
	}, nil
}
