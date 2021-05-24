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

package loader

import (
	"crypto/sha1"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/pingcap/dm/pkg/terror"
)

// SQLReplace works like strings.Replace but only supports one replacement.
// It uses backquote pairs to quote the old and new word.
func SQLReplace(s, oldStr, newStr string, ansiquote bool) string {
	var quote string
	if ansiquote {
		quote = "\""
	} else {
		quote = "`"
	}
	quoteF := func(s string) string {
		var b strings.Builder
		b.WriteString(quote)
		b.WriteString(s)
		b.WriteString(quote)
		return b.String()
	}

	oldStr = quoteF(oldStr)
	newStr = quoteF(newStr)
	return strings.Replace(s, oldStr, newStr, 1)
}

// shortSha1 returns the first 6 characters of sha1 value.
func shortSha1(s string) string {
	h := sha1.New()

	h.Write([]byte(s))
	return fmt.Sprintf("%x", h.Sum(nil))[:6]
}

// percent calculates percentage of a/b.
func percent(a int64, b int64, finish bool) string {
	if b == 0 {
		if finish {
			return "100.00 %"
		}
		return "0.00 %"
	}
	return fmt.Sprintf("%.2f %%", float64(a)/float64(b)*100)
}

// progress calculates progress of a/b.
func progress(a int64, b int64, finish bool) float64 {
	if b == 0 {
		if finish {
			return 1
		}
		return 0
	}
	return float64(a) / float64(b)
}

func generateSchemaCreateFile(dir string, schema string) error {
	file, err := os.Create(path.Join(dir, fmt.Sprintf("%s-schema-create.sql", schema)))
	if err != nil {
		return terror.ErrLoadUnitCreateSchemaFile.Delegate(err)
	}
	defer file.Close()

	_, err = fmt.Fprintf(file, "CREATE DATABASE `%s`;\n", escapeName(schema))
	return terror.ErrLoadUnitCreateSchemaFile.Delegate(err)
}

func escapeName(name string) string {
	return strings.ReplaceAll(name, "`", "``")
}

// input filename is like `all_mode.t1.0.sql` or `all_mode.t1.sql`.
func getDBAndTableFromFilename(filename string) (string, string, error) {
	idx := strings.LastIndex(filename, ".sql")
	if idx < 0 {
		return "", "", fmt.Errorf("%s doesn't have a `.sql` suffix", filename)
	}
	fields := strings.Split(filename[:idx], ".")
	if len(fields) != 2 && len(fields) != 3 {
		return "", "", fmt.Errorf("%s doesn't have correct `.` separator", filename)
	}
	return fields[0], fields[1], nil
}
