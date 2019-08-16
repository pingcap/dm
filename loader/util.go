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
	"bytes"
	"crypto/sha1"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/pingcap/dm/pkg/terror"
)

// CollectDirFiles gets files in path
func CollectDirFiles(path string) map[string]struct{} {
	files := make(map[string]struct{})
	filepath.Walk(path, func(path string, f os.FileInfo, err error) error {
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

	return files
}

// SQLReplace works like strings.Replace but only supports one replacement.
// It uses backquote pairs to quote the old and new word.
func SQLReplace(s, old, new string) string {
	old = backquote(old)
	new = backquote(new)
	return strings.Replace(s, old, new, 1)
}

func backquote(s string) string {
	buf := bytes.Buffer{}
	buf.WriteByte('`')
	buf.WriteString(s)
	buf.WriteByte('`')
	return buf.String()
}

// shortSha1 returns the first 6 characters of sha1 value
func shortSha1(s string) string {
	h := sha1.New()
	h.Write([]byte(s))
	return fmt.Sprintf("%x", h.Sum(nil))[:6]
}

// percent calculates percentage of a/b.
func percent(a int64, b int64) string {
	return fmt.Sprintf("%.2f %%", float64(a)/float64(b)*100)
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
