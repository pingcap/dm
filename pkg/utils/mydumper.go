package utils

import (
	"bufio"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"
)

// ParseMetaData parses mydumper's output meta file and returns binlog position
func ParseMetaData(filename string) (*mysql.Position, error) {
	fd, err := os.Open(filename)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer fd.Close()

	var logName = ""
	br := bufio.NewReader(fd)
	for {
		line, err := br.ReadString('\n')
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, errors.Trace(err)
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
		parts := strings.Split(line, ": ")
		if len(parts) != 2 {
			continue
		}
		if parts[0] == "Log" {
			logName = parts[1]
		} else if parts[0] == "Pos" {
			pos64, err := strconv.ParseUint(parts[1], 10, 32)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if len(logName) > 0 {
				return &mysql.Position{Name: logName, Pos: uint32(pos64)}, nil
			}
			break // Pos extracted, but no Log, error occurred
		}
	}

	return nil, errors.Errorf("parse metadata for %s fail", filename)
}
