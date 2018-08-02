// Copyright 2018 PingCAP, Inc.
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

package config

import (
	"regexp"
	"strings"
	"sync"
)

type dmlType byte

const (
	dmlInvalid dmlType = iota
	dmlInsert
	dmlUpdate
	dmlDelete
)

func (dt dmlType) String() string {
	switch dt {
	case dmlInvalid:
		return "invalid dml type"
	case dmlInsert:
		return "insert"
	case dmlUpdate:
		return "update"
	case dmlDelete:
		return "delete"
	default:
		return "invalid dml type"
	}
}

func toDmlType(dml string) dmlType {
	dml = strings.ToLower(dml)
	switch dml {
	case "insert":
		return dmlInsert
	case "update":
		return dmlUpdate
	case "delete":
		return dmlDelete
	default:
		return dmlInvalid
	}
}

// SkipDMLRules contain rules about skipping DML.
type SkipDMLRules struct {
	skipByDML    map[dmlType]struct{}
	skipBySchema map[string]map[dmlType]struct{}
	skipByTable  map[string]map[string]map[dmlType]struct{}
}

/*
CREATE [TEMPORARY] TABLE [IF NOT EXISTS] tbl_name
    { LIKE old_tbl_name | (LIKE old_tbl_name) }
*/
var (
	// https://dev.mysql.com/doc/refman/5.7/en/create-database.html
	createDatabaseRegex = regexp.MustCompile("(?i)CREATE\\s+(DATABASE|SCHEMA)\\s+(IF NOT EXISTS\\s+)?\\S+")
	// https://dev.mysql.com/doc/refman/5.7/en/drop-database.html
	dropDatabaseRegex = regexp.MustCompile("(?i)DROP\\s+(DATABASE|SCHEMA)\\s+(IF EXISTS\\s+)?\\S+")
	// https://dev.mysql.com/doc/refman/5.7/en/create-index.html
	// https://dev.mysql.com/doc/refman/5.7/en/drop-index.html
	createIndexDDLRegex = regexp.MustCompile("(?i)ON\\s+\\S+\\s*\\(")
	dropIndexDDLRegex   = regexp.MustCompile("(?i)ON\\s+\\S+")
	// https://dev.mysql.com/doc/refman/5.7/en/create-table.html
	createTableRegex     = regexp.MustCompile("(?i)^CREATE\\s+(TEMPORARY\\s+)?TABLE\\s+(IF NOT EXISTS\\s+)?\\S+")
	createTableLikeRegex = regexp.MustCompile("(?i)^CREATE\\s+(TEMPORARY\\s+)?TABLE\\s+(IF NOT EXISTS\\s+)?\\S+\\s*\\(?\\s*LIKE\\s+\\S+")
	// https://dev.mysql.com/doc/refman/5.7/en/drop-table.html
	dropTableRegex = regexp.MustCompile("^(?i)DROP\\s+(TEMPORARY\\s+)?TABLE\\s+(IF EXISTS\\s+)?\\S+")
	// https://dev.mysql.com/doc/refman/5.7/en/alter-table.html
	alterTableRegex = regexp.MustCompile("^(?i)ALTER\\s+TABLE\\s+\\S+")
	// https://dev.mysql.com/doc/refman/5.7/en/create-trigger.html
	builtInSkipDDLs = []string{
		// For mariadb, for query event, like `# Dumm`
		// But i don't know what is the meaning of this event.
		"^#",

		// transaction
		"^SAVEPOINT",

		// skip all flush sqls
		"^FLUSH",

		// table maintenance
		"^OPTIMIZE\\s+TABLE",
		"^ANALYZE\\s+TABLE",
		"^REPAIR\\s+TABLE",

		// temporary table
		"^DROP\\s+(\\/\\*\\!40005\\s+)?TEMPORARY\\s+(\\*\\/\\s+)?TABLE",

		// trigger
		"^CREATE\\s+(DEFINER\\s?=.+?)?TRIGGER",
		"^DROP\\s+TRIGGER",

		// procedure
		"^DROP\\s+PROCEDURE",
		"^CREATE\\s+(DEFINER\\s?=.+?)?PROCEDURE",
		"^ALTER\\s+PROCEDURE",

		// view
		"^CREATE\\s*(OR REPLACE)?\\s+(ALGORITHM\\s?=.+?)?(DEFINER\\s?=.+?)?\\s+(SQL SECURITY DEFINER)?VIEW",
		"^DROP\\s+VIEW",
		"^ALTER\\s+(ALGORITHM\\s?=.+?)?(DEFINER\\s?=.+?)?(SQL SECURITY DEFINER)?VIEW",

		// function
		// user-defined function
		"^CREATE\\s+(AGGREGATE)?\\s*?FUNCTION",
		// stored function
		"^CREATE\\s+(DEFINER\\s?=.+?)?FUNCTION",
		"^ALTER\\s+FUNCTION",
		"^DROP\\s+FUNCTION",

		// tableSpace
		"^CREATE\\s+TABLESPACE",
		"^ALTER\\s+TABLESPACE",
		"^DROP\\s+TABLESPACE",

		// account management
		"^GRANT",
		"^REVOKE",
		"^CREATE\\s+USER",
		"^ALTER\\s+USER",
		"^RENAME\\s+USER",
		"^DROP\\s+USER",
		"^SET\\s+PASSWORD",

		// alter database
		"^ALTER DATABASE",
	}
)

var (
	compileOnce     sync.Once
	skipDDLPatterns *regexp.Regexp

	builtInSkipDDLPatterns *regexp.Regexp
)

func init() {
	builtInSkipDDLPatterns = regexp.MustCompile("(?i)" + strings.Join(builtInSkipDDLs, "|"))
}

func skipQueryEvent(skipDDLs []string, sql string) bool {
	if builtInSkipDDLPatterns.FindStringIndex(sql) != nil {
		return true
	}

	if len(skipDDLs) == 0 {
		return false
	}

	// compatibility with previous version of syncer
	for _, skipSQL := range skipDDLs {
		if strings.HasPrefix(strings.ToUpper(sql), strings.ToUpper(skipSQL)) {
			return true
		}
	}

	compileOnce.Do(func() {
		skipDDLPatterns = regexp.MustCompile("(?i)" + strings.Join(skipDDLs, "|"))
	})

	if skipDDLPatterns.FindStringIndex(sql) != nil {
		return true
	}

	return false
}
