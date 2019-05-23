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

// VerifyBinlogFilename verify whether is a valid MySQL/MariaDB binlog filename.
// valid format is `base + '.' + seq`.
func VerifyBinlogFilename(filename string) error {
	parts := strings.Split(filename, binlogFilenameSep)
	if len(parts) != 2 {
		return errors.Annotatef(ErrInvalidBinlogFilename, "filename %s", filename)
	} else if n, err := strconv.Atoi(parts[1]); err != nil || n <= 0 {
		return errors.Annotatef(ErrInvalidBinlogFilename, "filename %s", filename)
	}
	return nil
}
