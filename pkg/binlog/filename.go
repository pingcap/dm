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

package binlog

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
)

const (
	// the binlog file name format is `base + '.' + seq`.
	binlogFilenameSep = "."
)

var (
	// ErrInvalidBinlogFilename means error about invalid binlog filename.
	ErrInvalidBinlogFilename = errors.New("invalid binlog filename")
)

// Filename represents a binlog filename.
type Filename struct {
	BaseName string
	Seq      string
}

// LessThan checks whether this filename < other filename.
func (f Filename) LessThan(other Filename) bool {
	return f.BaseName == other.BaseName && f.Seq < other.Seq
}

// GreaterOrEqualThan checks whether this filename >= other filename.
func (f Filename) GreaterOrEqualThan(other Filename) bool {
	return f.BaseName == other.BaseName && f.Seq >= other.Seq
}

// GreaterThan checks whether this filename > other filename.
func (f Filename) GreaterThan(other Filename) bool {
	return f.BaseName == other.BaseName && f.Seq > other.Seq
}

// ParseFilename parses a string representation binlog filename into a `Filename`.
func ParseFilename(filename string) (Filename, error) {
	var fn Filename
	parts := strings.Split(filename, binlogFilenameSep)
	if len(parts) != 2 {
		return fn, errors.Annotatef(ErrInvalidBinlogFilename, "filename %s", filename)
	} else if n, err := strconv.Atoi(parts[1]); err != nil || n <= 0 {
		return fn, errors.Annotatef(ErrInvalidBinlogFilename, "filename %s", filename)
	}
	fn.BaseName = parts[0]
	fn.Seq = parts[1]
	return fn, nil
}

// VerifyFilename verify whether is a valid MySQL/MariaDB binlog filename.
// valid format is `base + '.' + seq`.
func VerifyFilename(filename string) error {
	_, err := ParseFilename(filename)
	return errors.Trace(err)
}

// GetFilenameIndex return a float64 index value (seq number) of the filename.
func GetFilenameIndex(filename string) (float64, error) {
	fn, err := ParseFilename(filename)
	if err != nil {
		return 0, errors.Trace(err)
	}
	idx, err := strconv.ParseFloat(fn.Seq, 64)
	if err != nil {
		return 0, errors.Annotatef(err, "binlog filename %s", filename)
	}
	return idx, nil
}

// ConstructFilename constructs a binlog filename from the basename and seq.
func ConstructFilename(baseName, seq string) string {
	return fmt.Sprintf("%s%s%s", baseName, binlogFilenameSep, seq)
}
