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
	"path/filepath"
	"strings"

	"github.com/pingcap/dm/pkg/terror"
)

// CollectDirFiles gets files in path
func CollectDirFiles(path string) (map[string]struct{}, error) {
	files := make(map[string]struct{})
	err := filepath.Walk(path, func(_ string, f os.FileInfo, err error) error {
		if err != nil {
			return nil
		}

		if f == nil {
			return nil
		}

		if f.IsDir() {
			return nil
		}

		name := strings.TrimSpace(f.Name())
		files[name] = struct{}{}
		return nil
	})

	return files, err
}

// SQLReplace works like strings.Replace but only supports one replacement.
// It uses backquote pairs to quote the old and new word.
func SQLReplace(s, old, new string, ansiquote bool) string {
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

	old = quoteF(old)
	new = quoteF(new)
	return strings.Replace(s, old, new, 1)
}

// shortSha1 returns the first 6 characters of sha1 value
func shortSha1(s string) string {
	h := sha1.New()
	//nolint:errcheck
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
	return strings.Replace(name, "`", "``", -1)
}

// input filename is like `all_mode.t1.0.sql` or `all_mode.t1.sql`
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
