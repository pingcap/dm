package utils

import (
	"fmt"
	"hash/crc32"
)

// GenHashKey generates key with crc32 algorithm
func GenHashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// GenTableKey generates a key for table
func GenTableKey(schema, table string) string {
	return fmt.Sprintf("`%s`.`%s`", schema, table)
}
