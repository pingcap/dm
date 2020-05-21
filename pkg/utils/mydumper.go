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

	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/terror"
)

// ParseMetaData parses mydumper's output meta file and returns binlog position and GTID
func ParseMetaData(filename, flavor string) (mysql.Position, string, error) {
	fd, err := os.Open(filename)
	if err != nil {
		return mysql.Position{}, "", terror.ErrParseMydumperMeta.Generate(err)
	}
	defer fd.Close()

	pos := mysql.Position{}
	gtid := ""

	br := bufio.NewReader(fd)
	for {
		line, err := br.ReadString('\n')
		if err == io.EOF {
			break
		} else if err != nil {
			return mysql.Position{}, "", terror.ErrParseMydumperMeta.Generate(err)
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
			pos64, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return mysql.Position{}, "", terror.ErrParseMydumperMeta.Generate(err)
			}
			pos.Pos = uint32(pos64)
		case "GTID":
			// multiple GTID sets may cross multiple lines, continue to read them.
			following, err := readFollowingGTIDs(br, flavor)
			if err != nil {
				return mysql.Position{}, "", terror.ErrParseMydumperMeta.Generate(err)
			}
			gtid = value + following
		}
	}

	if len(pos.Name) == 0 || pos.Pos == uint32(0) {
		return mysql.Position{}, "", terror.ErrParseMydumperMeta.Generate(fmt.Sprintf("file %s invalid format", filename))
	}

	return pos, gtid, nil
}

func readFollowingGTIDs(br *bufio.Reader, flavor string) (string, error) {
	var following strings.Builder
	for {
		line, err := br.ReadString('\n')
		if err == io.EOF {
			return following.String(), nil // return the previous, not including the last line.
		} else if err != nil {
			return "", err
		}

		line = strings.TrimSpace(line[:len(line)-1])
		if len(line) == 0 {
			return following.String(), nil // end with empty line.
		}

		end := len(line)
		if strings.HasSuffix(line, ",") {
			end = len(line) - 1
		}

		// try parse to verify it
		_, err = gtid.ParserGTID(flavor, line[:end])
		if err != nil {
			return following.String(), nil // return the previous, not including this non-GTID line.
		}

		following.WriteString(line)
	}
}
