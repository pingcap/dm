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

package writer

import (
	"github.com/siddontang/go-mysql/replication"
)

// Result represents a write result.
type Result struct {
	Ignore bool // whether the event ignored by the writer
}

// Writer writes binlog events into disk or any other memory structure.
// The writer should support:
//   1. write binlog events and report the operation result
//   2. skip any obsolete binlog events
//   3. generate dummy events to fill the gap if needed
//   4. rotate binlog(relay) file if needed
//   5. rollback/discard unfinished binlog entries(events or transactions)
type Writer interface {
	// Start prepares the writer for writing binlog events.
	Start() error

	// Close closes the writer and release the resource.
	Close() error

	// WriteEvent writes an binlog event's data into disk or any other places.
	// It is not safe for concurrent use by multiple goroutines.
	WriteEvent(ev *replication.BinlogEvent) (*Result, error)

	// Flush flushes the buffered data to a stable storage or sends through the network.
	// It is not safe for concurrent use by multiple goroutines.
	Flush() error
}
