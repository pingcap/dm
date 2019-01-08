package binlog

import (
	"strconv"
	"strings"

	"github.com/juju/errors"
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
