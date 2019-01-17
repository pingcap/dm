package command

import (
	"regexp"
	"strings"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"

	"github.com/pingcap/dm/pkg/binlog"
)

// TrimQuoteMark tries to trim leading and tailing quote(") mark if exists
// only trim if leading and tailing quote matched as a pair
func TrimQuoteMark(s string) string {
	if len(s) > 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}

// VerifySQLOperateArgs verify args for sql operation, including sql-skip, sql-replace.
// NOTE: only part of args verified in here and others need to be verified from outer.
func VerifySQLOperateArgs(binlogPosStr, sqlPattern string, sharding bool) (*mysql.Position, *regexp.Regexp, error) {
	binlogPosStr = TrimQuoteMark(binlogPosStr)
	sqlPattern = TrimQuoteMark(sqlPattern)

	var (
		pos *mysql.Position
		reg *regexp.Regexp
		err error
	)

	if len(binlogPosStr) > 0 && len(sqlPattern) > 0 {
		return nil, nil, errors.New("cannot specify both --binlog-pos and --sql-pattern in sql operation")
	}

	if len(binlogPosStr) > 0 {
		pos2, err := binlog.PositionFromStr(binlogPosStr)
		if err != nil {
			return nil, nil, errors.Annotatef(err, "invalid --binlog-pos %s in sql operation", binlogPosStr)
		}
		pos = &pos2
	} else if len(sqlPattern) > 0 {
		var pattern string
		if strings.HasPrefix(sqlPattern, "~") {
			pattern = sqlPattern[1:]
		} else {
			pattern = "^" + regexp.QuoteMeta(sqlPattern) + "$"
		}

		reg, err = regexp.Compile(pattern)
		if err != nil {
			return nil, nil, errors.Annotatef(err, "invalid --sql-pattern %s in sql operation", sqlPattern)
		}
	} else {
		return nil, nil, errors.New("must specify one of --binlog-pos and --sql-pattern in sql operation")
	}

	if sharding && len(binlogPosStr) > 0 {
		return nil, nil, errors.New("cannot specify --binlog-pos with --sharding in sql operation")
	}

	return pos, reg, nil
}
