// Copyright 2020 PingCAP, Inc.
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

package syncer

import (
	"github.com/siddontang/go-mysql/replication"
)

// Plugin is a struct of plugin used in syncer unit
type Plugin struct {
	// the plugin's .so file path
	path string

	// conn used to creating connection for double write or other usage
	conn interface{}

	// Init do some init job
	Init func() error

	// HandleDDLJobResult handles the result of ddl job
	HandleDDLJobResult func(ev *replication.QueryEvent, ec eventContext, err error) error

	// HandleDMLJobResult handles the result of dml job
	HandleDMLJobResult func(ev *replication.RowsEvent, ec eventContext, err error) error
}
