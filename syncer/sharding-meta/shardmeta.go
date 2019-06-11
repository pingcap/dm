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

package shardmeta

import (
	"encoding/json"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/mysql"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/utils"
)

const (
	// MetaTableFormat is used in meta table name constructor
	MetaTableFormat = "%s_syncer_sharding_meta"
)

// DDLItem records ddl information used in sharding sequence organization
type DDLItem struct {
	FirstPos mysql.Position `json:"first-pos"` // first DDL's binlog pos
	DDLs     []string       `json:"ddls"`      // DDLs
	Source   string         `json:"source"`    // source table ID
}

// ShardingSequence records a list of DDLItem
type ShardingSequence struct {
	Items []*DDLItem `json:"Items"`
}

// ShardingMeta stores sharding ddl sequence
// including global sequence and each source's own sequence
type ShardingMeta struct {
	activeIdx int                          // the first unsynced DDL index
	global    *ShardingSequence            // merged sharding sequence of all source tables
	sources   map[string]*ShardingSequence // source table ID -> its sharding sequence
}

// Compare compares the first DDL binlog position of DDLItem with given position
func (item *DDLItem) Compare(pos mysql.Position) int {
	return item.FirstPos.Compare(pos)
}

// IsSubsequence checks whether a ShardingSequence is the sub sequence of other.
func (seq *ShardingSequence) IsSubsequence(other *ShardingSequence) bool {
	if len(seq.Items) > len(other.Items) {
		return false
	}
	for idx := range seq.Items {
		if !utils.CompareShardingDDLs(seq.Items[idx].DDLs, other.Items[idx].DDLs) {
			return false
		}
	}
	return true
}

// String returns the ShardingSequence's json string
func (seq *ShardingSequence) String() string {
	cfg, err := json.Marshal(seq.Items)
	if err != nil {
		log.Errorf("marshal ShardingSequence to json error %v", err)
	}
	return string(cfg)
}

// NewDDLItem creates a new DDLItem
func NewDDLItem(pos mysql.Position, ddls []string, source string) *DDLItem {
	return &DDLItem{
		FirstPos: pos,
		DDLs:     ddls,
		Source:   source,
	}
}

// NewShardingMeta creates a new ShardingMeta
func NewShardingMeta() *ShardingMeta {
	return &ShardingMeta{
		global:  &ShardingSequence{Items: make([]*DDLItem, 0)},
		sources: make(map[string]*ShardingSequence),
	}
}

func (meta *ShardingMeta) reinitialize() {
	meta.activeIdx = 0
	meta.global = &ShardingSequence{make([]*DDLItem, 0)}
	meta.sources = make(map[string]*ShardingSequence)
}

// checkItemExists checks whether DDLItem exists in its source sequence
// if exists, return the index of DDLItem in source sequence.
// if not exists, return the next index in source sequence.
func (meta *ShardingMeta) checkItemExists(item *DDLItem) (int, bool) {
	source, ok := meta.sources[item.Source]
	if !ok {
		return 0, false
	}
	for idx, ddlItem := range source.Items {
		if item.FirstPos.Compare(ddlItem.FirstPos) == 0 {
			return idx, true
		}
	}
	return len(source.Items), false
}

// AddItem adds a new comming DDLItem into ShardingMeta
// 1. if DDLItem already exists in source sequence, does nothing
// 2. add the DDLItem into its related source sequence
// 3. if it is a new DDL in global sequence, add it into global sequence
// 4. check the source sequence is the subsequence of global sequence, if not, return an error
// returns:
//   active: whether the DDL will be processed in this round
func (meta *ShardingMeta) AddItem(item *DDLItem) (active bool, err error) {
	index, exists := meta.checkItemExists(item)
	if exists {
		return index == meta.activeIdx, nil
	}

	if source, ok := meta.sources[item.Source]; !ok {
		meta.sources[item.Source] = &ShardingSequence{Items: []*DDLItem{item}}
	} else {
		source.Items = append(source.Items, item)
	}

	found := false
	for _, globalItem := range meta.global.Items {
		if utils.CompareShardingDDLs(item.DDLs, globalItem.DDLs) {
			found = true
			break
		}
	}
	if !found {
		meta.global.Items = append(meta.global.Items, item)
	}

	global, source := meta.global, meta.sources[item.Source]
	if len(global.Items) < len(source.Items) {
		// should not happen
		return false, errors.Errorf("global sharding ddl Items %v less than source %v", global.Items, source.Items)
	}
	if !source.IsSubsequence(global) {
		return false, errors.Errorf("wrong sql sequence of source %+v, global is %+v", source.Items, global.Items)
	}

	return index == meta.activeIdx, nil
}

// GetActiveDDLItem returns the source table's active DDLItem
// if in DDL unsynced procedure, the active DDLItem means the syncing DDL
// if in re-sync procedure, the active DDLItem means the next syncing DDL in DDL syncing sequence, may be nil
func (meta *ShardingMeta) GetActiveDDLItem(tableSource string) *DDLItem {
	source, ok := meta.sources[tableSource]
	if !ok {
		return nil
	}
	if meta.activeIdx < len(source.Items) {
		return source.Items[meta.activeIdx]
	}
	return nil
}

// InSequenceSharding returns whether in sequence sharding
func (meta *ShardingMeta) InSequenceSharding() bool {
	return meta.activeIdx < len(meta.global.Items)
}

// ResolveShardingDDL resolves one sharding DDL and increase activeIdx
// if activeIdx equals to the length of global sharding sequence, it means all
// sharding DDL in this ShardingMeta sequence is resolved and will reinitialize
// the ShardingMeta, return true if all DDLs are resolved.
func (meta *ShardingMeta) ResolveShardingDDL() bool {
	meta.activeIdx++
	if meta.activeIdx == len(meta.global.Items) {
		meta.reinitialize()
		return true
	}
	return false
}

// NextShardingDDLFirstPos returns the first binlog position of next sharding DDL in sequence
func (meta *ShardingMeta) NextShardingDDLFirstPos() (*mysql.Position, error) {
	if meta.activeIdx >= len(meta.global.Items) {
		return nil, errors.Errorf("activeIdx %d larger than global DDLItems: %v", meta.activeIdx, meta.global.Items)
	}
	return &meta.global.Items[meta.activeIdx].FirstPos, nil
}

// FlushData returns sharding meta flush SQL and args
func (meta *ShardingMeta) FlushData(schema, table, sourceID, tableID string) ([]string, [][]interface{}) {
	if len(meta.global.Items) == 0 {
		sql2 := fmt.Sprintf("DELETE FROM `%s`.`%s` where source_id=? and table_id=?", schema, table)
		args2 := []interface{}{sourceID, tableID}
		return []string{sql2}, [][]interface{}{args2}
	}
	var (
		sqls    = make([]string, 1+len(meta.sources))
		args    = make([][]interface{}, 0, 1+len(meta.sources))
		baseSQL = fmt.Sprintf("INSERT INTO `%s`.`%s` (`source_id`, `table_id`, `source`, `is_global`, `data`) VALUES(?,?,?,?,?) ON DUPLICATE KEY UPDATE `data`=?", schema, table)
	)
	for i := range sqls {
		sqls[i] = baseSQL
	}
	args = append(args, []interface{}{sourceID, tableID, "", true, meta.global.String(), meta.global.String()})
	for source, seq := range meta.sources {
		args = append(args, []interface{}{sourceID, tableID, source, false, seq.String(), seq.String()})
	}
	return sqls, args
}
