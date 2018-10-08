// Copyright 2017 PingCAP, Inc.
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

package syncer

import (
	"math"
	"strings"

	"github.com/siddontang/go-mysql/mysql"
)

// Compare binlog positions.
// The result will be 0 if |a-b| < deviation, otherwise -1 if a < b, and +1 if a > b.
func compareBinlogPos(a, b mysql.Position, deviation float64) int {
	if a.Name < b.Name {
		return -1
	}

	if a.Name > b.Name {
		return 1
	}

	if math.Abs(float64(a.Pos-b.Pos)) <= deviation {
		return 0
	}

	if a.Pos < b.Pos {
		return -1
	}

	return 1
}

func toBinlogType(bt string) BinlogType {
	bt = strings.ToLower(bt)
	switch bt {
	case "local":
		return LocalBinlog
	case "remote":
		return RemoteBinlog
	default:
		return RemoteBinlog
	}
}
