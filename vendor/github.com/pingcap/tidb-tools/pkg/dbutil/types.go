package dbutil

import (
	"github.com/pingcap/tidb/mysql"
)

// IsNumberType returns true if tp is number type
func IsNumberType(tp byte) bool {
	switch tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24, mysql.TypeYear:
		return true
	}

	return false
}

// IsFloatType returns true if tp is float type
func IsFloatType(tp byte) bool {
	switch tp {
	case mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal:
		return true
	}

	return false
}
