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
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/siddontang/go-mysql/mysql"

	"github.com/pingcap/dm/pkg/terror"
)

// ParseMetaData parses mydumper's output meta file and returns binlog position and GTID
func ParseMetaData(filename string) (*mysql.Position, string, error) {
	fd, err := os.Open(filename)
	if err != nil {
		return nil, "", terror.ErrParseMydumperMeta.Generate(err)
	}
	defer fd.Close()

	pos := new(mysql.Position)
	gtid := ""

	br := bufio.NewReader(fd)
	for {
		line, err := br.ReadString('\n')
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, "", terror.ErrParseMydumperMeta.Generate(err)
		}
		line = strings.TrimSpace(line[:len(line)-1])
		if len(line) == 0 {
			continue
		}
		// ref: https://github.com/maxbube/mydumper/blob/master/mydumper.c#L434
		if strings.Contains(line, "SHOW SLAVE STATUS") {
			// now, we only parse log / pos for `SHOW MASTER STATUS`
			break
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		switch key {
		case "Log":
			pos.Name = value
		case "Pos":
			pos64, err := strconv.ParseUint(parts[1], 10, 32)
			if err != nil {
				return nil, "", terror.ErrParseMydumperMeta.Generate(err)
			}
			pos.Pos = uint32(pos64)
		case "GTID":
			gtid = value
		}
	}

	if len(pos.Name) == 0 || pos.Pos == uint32(0) {
		return nil, "", terror.ErrParseMydumperMeta.Generate(fmt.Sprintf("file %s invalid format", filename))
	}

	return pos, gtid, nil
}
