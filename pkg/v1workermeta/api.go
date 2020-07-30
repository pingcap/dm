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

package v1workermeta

import (
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/utils"
)

// `var` rather than `const` for testing.
var (
	// metaPath is the meta data path in v1.0.x.
	metaPath = "./dm_worker_meta"
	// dbPath is the levelDB path in v1.0.x.
	dbPath = "./dm_worker_meta/kv"
)

// GetSubtasksMeta gets all subtasks' meta (config and status) from `dm_worker_meta` in v1.0.x.
func GetSubtasksMeta() (map[string]*pb.TaskMeta, error) {
	// check if meta data exist.
	if !utils.IsDirExists(metaPath) || !utils.IsDirExists(dbPath) {
		return nil, nil
	}

	// open levelDB to get subtasks meta.
	db, err := openDB(dbPath, defaultKVConfig)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	// load subtasks' meta from levelDB.
	meta, err := newMeta(db)
	if err != nil {
		return nil, err
	}

	return meta.TasksMeta(), nil
}
