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

package syncer

/*
 * sharding DDL sync for Syncer
 *
 * assumption:
 *   all tables in a sharding group execute same DDLs in the same order
 *   when the first staring of the task, same DDL for the whole sharding group should not in partial executed
 *      the startup point can not be in the middle of the first-pos and last-pos of a sharding group's DDL
 *   do not support to modify router-rules online (when unresolved)
 *   do not support to rename table or database in a sharding group, another solution for it
 *   do not support to sync using GTID mode (GTID is supported by relay unit now)
 *   do not support same <schema-name, table-name> pair for upstream and downstream when merging sharding group
 *   ignore all drop schema/table and truncate table ddls
 *
 * checkpoint mechanism (ref: checkpoint.go):
 *   save checkpoint for every upstream table, and also global checkpoint
 *   global checkpoint can be used to re-sync after restarted
 *   per-table's checkpoint can be used to check whether the binlog has synced before
 *
 * normal work flow
 * 1. use the global streamer to sync regular binlog events as before
 *    update per-table's and global checkpoint
 * 2. the first sharding DDL encountered for a table in a sharding group
 *    save this DDL's binlog pos as first pos
 *    save this table's name
 * 3. continue the syncing with global streamer
 *    ignore binlog events for the table in step.2
 *    stop the updating for global checkpoint and this table's checkpoint
 * 4. more sharding DDLs encountered for tables in some sharding groups
 *    save these tables' name
 * 5. continue the syncing with global streamer
 *    ignore binlog events for tables which encountered sharding DDLs (step.2 and step.4)
 * 6. the last sharding DDL encountered for table in a sharding group
 *    save this DDL's next binlog pos as last pos for the sharding group
 *    execute this DDL
 *    reset the sharding group
 * 7. create a new streamer to re-sync ignored binlog events in the sharding group before
 *    start re-syncing from first DDL's binlog pos in step.2
 *    pause the global streamer's syncing
 * 8. continue the syncing with the sharding group special streamer
 *    ignore binlog events which not belong to the sharding group
 *    ignore binlog events have synced (obsolete) for the sharding group
 *    synced ignored binlog events in the sharding group from step.3 to step.5
 *    update per-table's and global checkpoint
 * 9. last pos in step.6 arrived
 * 10. close the sharding group special streamer
 * 11. use the global streamer to continue the syncing
 *
 * all binlogs executed at least once:
 *    no sharding group and no restart: syncing as previous
 *    sharding group: binlog events ignored between first-pos and last-pos will be re-sync using a special streamer
 *    restart: global checkpoint never upper than any ignored binlogs
 *
 * all binlogs executed at most once:
 *    NO guarantee for this even though every table records checkpoint dependently
 *    because execution of binlog and update of checkpoint are in different goroutines concurrently
 *    so when re-starting the process or recovering from errors, safe-mode must be enabled
 *
 */

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/mysql"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/log"
	shardmeta "github.com/pingcap/dm/syncer/sharding-meta"
)

// ShardingGroup represents a sharding DDL sync group
type ShardingGroup struct {
	sync.RWMutex
	// remain count waiting for syncing
	// == len(sources):  DDL syncing not started or resolved
	// == 0: all DDLs synced, will be reset to len(sources) after resolved combining with other dm-workers
	// (0, len(sources)): waiting for syncing
	// NOTE: we can make remain to be configurable if needed
	remain       int
	sources      map[string]bool // source table ID -> whether source table's DDL synced
	IsSchemaOnly bool            // whether is a schema (database) only DDL TODO: zxc add schema-level syncing support later

	sourceID string                  // associate dm-worker source ID
	schema   string                  // schema name, set through task config
	table    string                  // table name, used for save sharding meta
	meta     *shardmeta.ShardingMeta // sharding sequence meta storage

	firstPos    *mysql.Position // first DDL's binlog pos, used to restrain the global checkpoint when un-resolved
	firstEndPos *mysql.Position // first DDL's binlog End_log_pos, used to re-direct binlog streamer after synced
	ddls        []string        // DDL which current in syncing
}

// NewShardingGroup creates a new ShardingGroup
func NewShardingGroup(cfg *config.SubTaskConfig, sources []string, isSchemaOnly bool) *ShardingGroup {
	sg := &ShardingGroup{
		remain:       len(sources),
		sources:      make(map[string]bool, len(sources)),
		IsSchemaOnly: isSchemaOnly,
		sourceID:     cfg.SourceID,
		schema:       cfg.MetaSchema,
		table:        fmt.Sprintf(shardmeta.MetaTableFormat, cfg.Name),
		meta:         shardmeta.NewShardingMeta(),
		firstPos:     nil,
		firstEndPos:  nil,
	}
	for _, source := range sources {
		sg.sources[source] = false
	}
	return sg
}

// Merge merges new sources to exists
// used cases
//   * add a new database / table to exists sharding group
//   * add new table(s) to parent database's sharding group
//  if group is un-resolved, we add it in sources and set it true
//  othereise add it in source, set it false and increment remain
func (sg *ShardingGroup) Merge(sources []string) (bool, bool, int, error) {
	sg.Lock()
	defer sg.Unlock()

	// need to check whether source is exist? but we maybe re-sync more times
	isResolving := sg.remain != len(sg.sources)
	// ddls := []string{"create table"}

	for _, source := range sources {
		synced, exist := sg.sources[source]
		if isResolving && !synced {
			if exist {
				if !synced {
					sg.remain--
				}
				/*
					else if !utils.CompareShardingDDLs(sg.sourceDDLs[source], ddls) {
						return isResolving, sg.remain <= 0, sg.remain, errors.NotSupportedf("execute multiple ddls: previous ddl %s and current ddls %q for source table %s", sg.sourceDDLs[source], ddls, source)
					}
				*/
			}

			sg.sources[source] = true
			// sg.sourceDDLs[source] = ddls // fake create table ddl
		} else {
			if !exist {
				sg.remain++
			}
			sg.sources[source] = false
		}
	}

	return isResolving, sg.remain <= 0, sg.remain, nil
}

// Leave leaves from sharding group
// it, doesn't affect in syncing process
// used cases
//   * drop a database
//   * drop table
func (sg *ShardingGroup) Leave(sources []string) error {
	sg.Lock()
	defer sg.Unlock()

	//  if group is un-resolved, we can't do drop (DROP DATABASE / TABLE)
	if sg.remain != len(sg.sources) {
		return errors.NotSupportedf("group's sharding DDL %v is un-resolved, try drop sources %v", sg.ddls, sources)
	}

	for _, source := range sources {
		if synced, ok := sg.sources[source]; ok && !synced {
			sg.remain--
		}
		delete(sg.sources, source)
	}

	return nil
}

// Reset resets all sources to un-synced state
// when the previous sharding DDL synced and resolved, we need reset it
func (sg *ShardingGroup) Reset() {
	sg.remain = len(sg.sources)
	for source := range sg.sources {
		sg.sources[source] = false
	}
	sg.firstPos = nil
	sg.firstEndPos = nil
	sg.ddls = nil
}

// TrySync tries to sync the sharding group
// if source not in sharding group before, it will be added
// returns
//   synced: whether the source table's sharding group synced
//   active: whether the DDL will be processed in this round
//   remain: remain un-synced source table's count
func (sg *ShardingGroup) TrySync(source string, pos, endPos mysql.Position, ddls []string) (bool, bool, int, error) {
	sg.Lock()
	defer sg.Unlock()

	ddlItem := shardmeta.NewDDLItem(pos, ddls, source)
	active, err := sg.meta.AddItem(ddlItem)
	if err != nil {
		return sg.remain <= 0, active, sg.remain, errors.Trace(err)
	}
	if active {
		sg.sources[source] = true
		sg.remain--
	}

	if sg.firstPos == nil {
		sg.firstPos = &pos // save first DDL's pos
		sg.firstEndPos = &endPos
		sg.ddls = ddls
	}
	return sg.remain <= 0, active, sg.remain, nil
}

// CheckSyncing checks the source table syncing status
// returns
//   beforeDDL: whether the position is before active DDL
func (sg *ShardingGroup) CheckSyncing(source string, pos mysql.Position) (beforeDDL bool) {
	sg.RLock()
	defer sg.RUnlock()
	activeDDLItem := sg.meta.GetActiveDDLItem(source)
	if activeDDLItem == nil {
		return true
	}
	return activeDDLItem.Compare(pos) > 0
}

// UnresolvedGroupInfo returns pb.ShardingGroup if is unresolved, else returns nil
func (sg *ShardingGroup) UnresolvedGroupInfo() *pb.ShardingGroup {
	sg.RLock()
	defer sg.RUnlock()

	if sg.remain == len(sg.sources) {
		return nil
	}

	group := &pb.ShardingGroup{
		DDLs:     sg.ddls,
		FirstPos: sg.firstPos.String(),
		Synced:   make([]string, 0, len(sg.sources)-sg.remain),
		Unsynced: make([]string, 0, sg.remain),
	}
	for source, synced := range sg.sources {
		if synced {
			group.Synced = append(group.Synced, source)
		} else {
			group.Unsynced = append(group.Unsynced, source)
		}
	}
	return group
}

// Sources returns all sources (and whether synced)
func (sg *ShardingGroup) Sources() map[string]bool {
	sg.RLock()
	defer sg.RUnlock()
	ret := make(map[string]bool, len(sg.sources))
	for k, v := range sg.sources {
		ret[k] = v
	}
	return ret
}

// Tables returns all source tables' <schema, table> pair
func (sg *ShardingGroup) Tables() [][]string {
	sources := sg.Sources()
	tables := make([][]string, 0, len(sources))
	for id := range sources {
		schema, table := UnpackTableID(id)
		tables = append(tables, []string{schema, table})
	}
	return tables
}

// IsUnresolved return whether it's unresolved
func (sg *ShardingGroup) IsUnresolved() bool {
	sg.RLock()
	defer sg.RUnlock()

	return sg.remain != len(sg.sources)
}

// UnresolvedTables returns all source tables' <schema, table> pair if is unresolved, else returns nil
func (sg *ShardingGroup) UnresolvedTables() [][]string {
	sg.RLock()
	defer sg.RUnlock()

	// TODO: if we have sharding ddl sequence, and partial ddls synced, we treat
	// all the of the tables as unresolved
	if sg.remain == len(sg.sources) {
		return nil
	}

	tables := make([][]string, 0, len(sg.sources))
	for id := range sg.sources {
		schema, table := UnpackTableID(id)
		tables = append(tables, []string{schema, table})
	}
	return tables
}

// FirstPosUnresolved returns the first DDL pos if un-resolved, else nil
func (sg *ShardingGroup) FirstPosUnresolved() *mysql.Position {
	sg.RLock()
	defer sg.RUnlock()
	if sg.remain < len(sg.sources) && sg.firstPos != nil {
		// create a new pos to return
		return &mysql.Position{
			Name: sg.firstPos.Name,
			Pos:  sg.firstPos.Pos,
		}
	}
	return nil
}

// FirstEndPosUnresolved returns the first DDL End_log_pos if un-resolved, else nil
func (sg *ShardingGroup) FirstEndPosUnresolved() *mysql.Position {
	sg.RLock()
	defer sg.RUnlock()
	if sg.remain < len(sg.sources) && sg.firstEndPos != nil {
		// create a new pos to return
		return &mysql.Position{
			Name: sg.firstEndPos.Name,
			Pos:  sg.firstEndPos.Pos,
		}
	}
	return nil
}

// String implements Stringer.String
func (sg *ShardingGroup) String() string {
	return fmt.Sprintf("IsSchemaOnly:%v remain:%d, sources:%+v", sg.IsSchemaOnly, sg.remain, sg.sources)
}

// InSequenceSharding returns whether this sharding group is in sequence sharding
func (sg *ShardingGroup) InSequenceSharding() bool {
	return sg.meta.InSequenceSharding()
}

// ResolveShardingDDL resolves sharding DDL in sharding group
func (sg *ShardingGroup) ResolveShardingDDL() bool {
	sg.Lock()
	defer sg.Unlock()
	reset := sg.meta.ResolveShardingDDL()
	// reset sharding group after DDL is exexuted
	return reset
}

// NextShardingDDLFirstPos returns the first binlog position of next sharding DDL in sequence
func (sg *ShardingGroup) NextShardingDDLFirstPos() (*mysql.Position, error) {
	sg.RLock()
	defer sg.RUnlock()
	pos, err := sg.meta.NextShardingDDLFirstPos()
	return pos, errors.Trace(err)
}

// FlushData returns sharding meta flush SQLs and args
func (sg *ShardingGroup) FlushData(tableID string) ([]string, [][]interface{}) {
	return sg.meta.FlushData(sg.schema, sg.table, sg.sourceID, tableID)
}

// GenTableID generates table ID
func GenTableID(schema, table string) (ID string, isSchemaOnly bool) {
	if len(table) == 0 {
		return fmt.Sprintf("`%s`", schema), true
	}
	return fmt.Sprintf("`%s`.`%s`", schema, table), false
}

// UnpackTableID unpacks table ID to <schema, table> pair
func UnpackTableID(id string) (string, string) {
	parts := strings.Split(id, "`.`")
	schema := strings.TrimLeft(parts[0], "`")
	table := strings.TrimRight(parts[1], "`")
	return schema, table
}

// ShardingGroupKeeper used to keep ShardingGroup
type ShardingGroupKeeper struct {
	sync.RWMutex
	groups map[string]*ShardingGroup // target table ID -> ShardingGroup
	cfg    *config.SubTaskConfig

	schema string
	table  string
	db     *Conn
}

// NewShardingGroupKeeper creates a new ShardingGroupKeeper
func NewShardingGroupKeeper(cfg *config.SubTaskConfig) *ShardingGroupKeeper {
	k := &ShardingGroupKeeper{
		groups: make(map[string]*ShardingGroup),
		cfg:    cfg,
	}
	k.schema = cfg.MetaSchema
	k.table = fmt.Sprintf(shardmeta.MetaTableFormat, cfg.Name)
	return k
}

// AddGroup adds new group(s) according to target schema, table and source IDs
func (k *ShardingGroupKeeper) AddGroup(targetSchema, targetTable string, sourceIDs []string, merge bool) (needShardingHandle bool, group *ShardingGroup, synced bool, remain int, err error) {
	// if need to support target table-level sharding DDL
	// we also need to support target schema-level sharding DDL
	schemaID, _ := GenTableID(targetSchema, "")
	tableID, _ := GenTableID(targetSchema, targetTable)

	k.Lock()
	defer k.Unlock()

	if schemaGroup, ok := k.groups[schemaID]; !ok {
		k.groups[schemaID] = NewShardingGroup(k.cfg, sourceIDs, true)
	} else {
		schemaGroup.Merge(sourceIDs)
	}

	var ok bool
	if group, ok = k.groups[tableID]; !ok {
		group = NewShardingGroup(k.cfg, sourceIDs, false)
		k.groups[tableID] = group
	} else if merge {
		needShardingHandle, synced, remain, err = k.groups[tableID].Merge(sourceIDs)
	} else {
		err = errors.AlreadyExistsf("table group %s", tableID)
		return
	}

	return
}

// Init does initialization staff
func (k *ShardingGroupKeeper) Init(conn *Conn) error {
	k.clear()
	if conn != nil {
		k.db = conn
	} else {
		db, err := createDB(k.cfg, k.cfg.To, "1m")
		if err != nil {
			return errors.Trace(err)
		}
		k.db = db
	}
	err := k.prepare()
	return errors.Trace(err)
}

// clear clears all sharding groups
func (k *ShardingGroupKeeper) clear() {
	k.Lock()
	defer k.Unlock()
	k.groups = make(map[string]*ShardingGroup)
}

// ResetGroups resets group's sync status
func (k *ShardingGroupKeeper) ResetGroups() {
	k.RLock()
	defer k.RUnlock()
	for _, group := range k.groups {
		group.Lock()
		group.Reset()
		group.Unlock()
	}
}

// LeaveGroup leaves group according to target schema, table and source IDs
// LeaveGroup doesn't affect in syncing process
func (k *ShardingGroupKeeper) LeaveGroup(targetSchema, targetTable string, sources []string) error {
	schemaID, _ := GenTableID(targetSchema, "")
	tableID, _ := GenTableID(targetSchema, targetTable)
	k.Lock()
	defer k.Unlock()
	if group, ok := k.groups[tableID]; ok {
		if err := group.Leave(sources); err != nil {
			return errors.Trace(err)
		}
	}
	if schemaGroup, ok := k.groups[schemaID]; ok {
		if err := schemaGroup.Leave(sources); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// TrySync tries to sync the sharding group
// returns
//   isSharding: whether the source table is in a sharding group
//   group: the sharding group
//   synced: whether the source table's sharding group synced
//   active: whether is active DDL in sequence sharding DDL
//   remain: remain un-synced source table's count
func (k *ShardingGroupKeeper) TrySync(
	targetSchema, targetTable, source string, pos, endPos mysql.Position, ddls []string) (
	needShardingHandle bool, group *ShardingGroup, synced, active bool, remain int, err error) {

	tableID, schemaOnly := GenTableID(targetSchema, targetTable)
	if schemaOnly {
		// NOTE: now we don't support syncing for schema only sharding DDL
		return false, nil, true, false, 0, nil
	}

	k.Lock()
	defer k.Unlock()

	group, ok := k.groups[tableID]
	if !ok {
		return false, group, true, false, 0, nil
	}
	synced, active, remain, err = group.TrySync(source, pos, endPos, ddls)
	return true, group, synced, active, remain, errors.Trace(err)
}

// CheckSyncing checks the source table syncing status
// returns
//   needShardingHandle: whether the source table in is a sharding group
//   beforeDDL: whether the position is before active DDL
func (k *ShardingGroupKeeper) CheckSyncing(targetSchema, targetTable, source string, pos mysql.Position) (needShardingHandle, beforeDDL bool) {
	group := k.Group(targetSchema, targetTable)
	if group == nil {
		return false, true
	}
	return true, group.CheckSyncing(source, pos)
}

// UnresolvedTables returns
//   all `target-schema.target-table` that has unresolved sharding DDL
//   all source tables which with DDLs are un-resolved
// NOTE: this func only ensure the returned tables are current un-resolved
// if passing the returned tables to other func (like checkpoint),
// must ensure their sync state not changed in this progress
func (k *ShardingGroupKeeper) UnresolvedTables() ([]string, [][]string) {
	ids := make([]string, 0)
	tables := make([][]string, 0, 10)
	k.RLock()
	defer k.RUnlock()
	for id, group := range k.groups {
		unresolved := group.UnresolvedTables()
		if len(unresolved) > 0 {
			ids = append(ids, id)
			tables = append(tables, unresolved...)
		}
	}
	return ids, tables
}

// Group returns target table's group, nil if not exist
func (k *ShardingGroupKeeper) Group(targetSchema, targetTable string) *ShardingGroup {
	tableID, _ := GenTableID(targetSchema, targetTable)
	k.RLock()
	defer k.RUnlock()
	return k.groups[tableID]
}

// lowestFirstPosInGroups returns the lowest pos in all groups which are unresolved
func (k *ShardingGroupKeeper) lowestFirstPosInGroups() *mysql.Position {
	k.RLock()
	defer k.RUnlock()
	var lowest *mysql.Position
	for _, group := range k.groups {
		pos := group.FirstPosUnresolved()
		if pos == nil {
			continue
		}
		if lowest == nil {
			lowest = pos
		} else if lowest.Compare(*pos) > 0 {
			lowest = pos
		}
	}
	return lowest
}

// AdjustGlobalPoint adjusts globalPoint with sharding groups' lowest first point
func (k *ShardingGroupKeeper) AdjustGlobalPoint(globalPoint mysql.Position) mysql.Position {
	lowestFirstPos := k.lowestFirstPosInGroups()
	if lowestFirstPos != nil && lowestFirstPos.Compare(globalPoint) < 0 {
		return *lowestFirstPos
	}
	return globalPoint
}

// Groups returns all sharding groups, often used for debug
// caution: do not modify the returned groups directly
func (k *ShardingGroupKeeper) Groups() map[string]*ShardingGroup {
	k.RLock()
	defer k.RUnlock()

	// do a copy
	groups := make(map[string]*ShardingGroup, len(k.groups))
	for key, value := range k.groups {
		groups[key] = value
	}
	return groups
}

// UnresolvedGroups returns sharding groups which are un-resolved
// caution: do not modify the returned groups directly
func (k *ShardingGroupKeeper) UnresolvedGroups() []*pb.ShardingGroup {
	groups := make([]*pb.ShardingGroup, 0)
	k.RLock()
	defer k.RUnlock()
	for target, group := range k.groups {
		gi := group.UnresolvedGroupInfo()
		if gi != nil {
			gi.Target = target // set target
			groups = append(groups, gi)
		}
	}
	return groups
}

// InSequenceSharding returns whether exists sharding group in unfinished sequence sharding
func (k *ShardingGroupKeeper) InSequenceSharding() bool {
	k.RLock()
	defer k.RUnlock()
	for _, group := range k.groups {
		if group.InSequenceSharding() {
			return true
		}
	}
	return false
}

// ResolveShardingDDL resolves one sharding DDL in specific group
func (k *ShardingGroupKeeper) ResolveShardingDDL(targetSchema, targetTable string) (bool, error) {
	group := k.Group(targetSchema, targetTable)
	if group != nil {
		return group.ResolveShardingDDL(), nil
	}
	return false, errors.NotFoundf("sharding group for %s.%s", targetSchema, targetTable)
}

// NextShardingDDLFirstPos returns the next binlog postion in sharding DDL sequence
func (k *ShardingGroupKeeper) NextShardingDDLFirstPos(targetSchema, targetTable string) (*mysql.Position, error) {
	group := k.Group(targetSchema, targetTable)
	k.Lock()
	defer k.Unlock()
	if group != nil {
		pos, err := group.NextShardingDDLFirstPos()
		return pos, errors.Trace(err)
	}
	return nil, errors.NotFoundf("sharding group for %s.%s", targetSchema, targetTable)
}

// PrepareFlushSQLs returns all sharding meta flushed SQLs execpt for given table IDs
func (k *ShardingGroupKeeper) PrepareFlushSQLs(exceptTableIDs []string) ([]string, [][]interface{}) {
	var (
		sqls = make([]string, 0, len(k.groups))
		args = make([][]interface{}, 0, len(k.groups))
	)
	k.RLock()
	defer k.RUnlock()
	sort.Strings(exceptTableIDs)
	for id, group := range k.groups {
		if group.IsSchemaOnly {
			continue
		}
		i := sort.Search(len(exceptTableIDs), func(i int) bool { return id <= exceptTableIDs[i] })
		if i < len(exceptTableIDs) && exceptTableIDs[i] == id {
			continue
		}
		sqls2, args2 := group.FlushData(id)
		sqls = append(sqls, sqls2...)
		args = append(args, args2...)
	}
	return sqls, args
}

// Prepare inits sharding meta schema and tables if not exists
func (k *ShardingGroupKeeper) prepare() error {
	if err := k.createSchema(); err != nil {
		return errors.Trace(err)
	}

	if err := k.createTable(); err != nil {
		return errors.Trace(err)
	}

	return nil
}

// Close closes sharding group keeper
func (k *ShardingGroupKeeper) Close() {
	closeDBs(k.db)
}

func (k *ShardingGroupKeeper) createSchema() error {
	sql2 := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS `%s`", k.schema)
	args := make([]interface{}, 0)
	err := k.db.executeSQL([]string{sql2}, [][]interface{}{args}, maxRetryCount)
	log.Infof("[ShardingGroupKeeper] execute sql %s", sql2)
	return errors.Trace(err)
}

func (k *ShardingGroupKeeper) createTable() error {
	tableName := fmt.Sprintf("`%s`.`%s`", k.schema, k.table)
	sql2 := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		source_id VARCHAR(32) NOT NULL,
		table_id VARCHAR(128) NOT NULL,
		source  VARCHAR(128) NOT NULL,
		is_global BOOLEAN,
		data JSON,
		create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		UNIQUE KEY uk_source_id_table_id_source (source_id, table_id, source)
	)`, tableName)
	err := k.db.executeSQL([]string{sql2}, [][]interface{}{{}}, maxRetryCount)
	log.Infof("[ShardingGroupKeeper] execute sql %s", sql2)
	return errors.Trace(err)
}

// ShardingReSync represents re-sync info for a sharding DDL group
type ShardingReSync struct {
	currPos      mysql.Position // current DDL's binlog pos, initialize to first DDL's pos
	latestPos    mysql.Position // latest DDL's binlog pos
	targetSchema string
	targetTable  string
	allResolved  bool
}
