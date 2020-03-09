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
 * 7. redirect global streamer to the first DDL's binlog pos in step.2
 * 8. continue the syncing with the global streamer
 *    ignore binlog events which not belong to the sharding group
 *    ignore binlog events have synced (obsolete) for the sharding group
 *    synced ignored binlog events in the sharding group from step.3 to step.5
 *    update per-table's and global checkpoint
 * 9. last pos in step.6 arrived
 * 10. redirect global streamer to the active DDL in sequence sharding if needed
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
	"strings"
	"sync"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/binlog"
	"github.com/pingcap/dm/pkg/conn"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/terror"
	shardmeta "github.com/pingcap/dm/syncer/sharding-meta"

	"go.uber.org/zap"
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
	meta     *shardmeta.ShardingMeta // sharding sequence meta storage

	firstLocation    *binlog.Location // first DDL's binlog pos and gtid, used to restrain the global checkpoint when un-resolved
	firstEndLocation *binlog.Location // first DDL's binlog End_log_pos and gtid, used to re-direct binlog streamer after synced
	ddls             []string         // DDL which current in syncing

	flavor string
}

// NewShardingGroup creates a new ShardingGroup
func NewShardingGroup(sourceID, shardMetaSchema, shardMetaTable string, sources []string, meta *shardmeta.ShardingMeta, isSchemaOnly bool, flavor string) *ShardingGroup {
	sg := &ShardingGroup{
		remain:           len(sources),
		sources:          make(map[string]bool, len(sources)),
		IsSchemaOnly:     isSchemaOnly,
		sourceID:         sourceID,
		firstLocation:    nil,
		firstEndLocation: nil,
		flavor:           flavor,
	}
	if meta != nil {
		sg.meta = meta
	} else {
		sg.meta = shardmeta.NewShardingMeta(shardMetaSchema, shardMetaTable)
	}
	for _, source := range sources {
		sg.sources[source] = false
	}
	return sg
}

// Merge merges new sources to exists
// used cases
//   * add a new table to exists sharding group
//   * add new table(s) to parent database's sharding group
//  if group is in sequence sharding, return error directly
//  othereise add it in source, set it false and increment remain
func (sg *ShardingGroup) Merge(sources []string) (bool, bool, int, error) {
	sg.Lock()
	defer sg.Unlock()

	// NOTE: we don't support add shard table when in sequence sharding
	if sg.meta.InSequenceSharding() {
		return true, sg.remain <= 0, sg.remain, terror.ErrSyncUnitAddTableInSharding.Generate(sg.meta.GetGlobalActiveDDL(), sg.meta.GetGlobalItems())
	}

	for _, source := range sources {
		_, exist := sg.sources[source]
		if !exist {
			sg.remain++
			sg.sources[source] = false
		}
	}

	return false, sg.remain <= 0, sg.remain, nil
}

// Leave leaves from sharding group
// it, doesn't affect in syncing process
// used cases
//   * drop a database
//   * drop table
func (sg *ShardingGroup) Leave(sources []string) error {
	sg.Lock()
	defer sg.Unlock()

	// NOTE: if group is in sequence sharding, we can't do drop (DROP DATABASE / TABLE)
	if sg.meta.InSequenceSharding() {
		return terror.ErrSyncUnitDropSchemaTableInSharding.Generate(sources, sg.meta.GetGlobalActiveDDL(), sg.meta.GetGlobalItems())
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
	sg.Lock()
	defer sg.Unlock()

	sg.remain = len(sg.sources)
	for source := range sg.sources {
		sg.sources[source] = false
	}
	sg.firstLocation = nil
	sg.firstEndLocation = nil
	sg.ddls = nil
}

// TrySync tries to sync the sharding group
// returns
//   synced: whether the source table's sharding group synced
//   active: whether the DDL will be processed in this round
//   remain: remain un-synced source table's count
func (sg *ShardingGroup) TrySync(source string, location, endLocation binlog.Location, ddls []string) (bool, bool, int, error) {
	sg.Lock()
	defer sg.Unlock()

	ddlItem := shardmeta.NewDDLItem(location, ddls, source)
	active, err := sg.meta.AddItem(ddlItem)
	if err != nil {
		return sg.remain <= 0, active, sg.remain, err
	}
	if active && !sg.sources[source] {
		sg.sources[source] = true
		sg.remain--
	}

	if sg.firstLocation == nil {
		sg.firstLocation = &location // save first DDL's pos
		sg.firstEndLocation = &endLocation
		sg.ddls = ddls
	}
	return sg.remain <= 0, active, sg.remain, nil
}

// CheckSyncing checks the source table syncing status
// returns
//   beforeActiveDDL: whether the position is before active DDL
func (sg *ShardingGroup) CheckSyncing(source string, location binlog.Location) (beforeActiveDDL bool) {
	sg.RLock()
	defer sg.RUnlock()
	activeDDLItem := sg.meta.GetActiveDDLItem(source)
	if activeDDLItem == nil {
		return true
	}
	return binlog.CompareLocation(activeDDLItem.FirstLocation, location) > 0
}

// UnresolvedGroupInfo returns pb.ShardingGroup if is unresolved, else returns nil
func (sg *ShardingGroup) UnresolvedGroupInfo() *pb.ShardingGroup {
	sg.RLock()
	defer sg.RUnlock()

	if sg.remain == len(sg.sources) {
		return nil
	}

	group := &pb.ShardingGroup{
		DDLs:          sg.ddls,
		FirstLocation: sg.firstLocation.String(),
		Synced:        make([]string, 0, len(sg.sources)-sg.remain),
		Unsynced:      make([]string, 0, sg.remain),
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

// FirstLocationUnresolved returns the first DDL pos if un-resolved, else nil
func (sg *ShardingGroup) FirstLocationUnresolved() *binlog.Location {
	sg.RLock()
	defer sg.RUnlock()
	if sg.remain < len(sg.sources) && sg.firstLocation != nil {
		// create a new location to return
		location := sg.firstLocation.CloneWithFlavor(sg.flavor)
		return &location
	}
	item := sg.meta.GetGlobalActiveDDL()
	if item != nil {
		// make a new copy
		location := item.FirstLocation.CloneWithFlavor(sg.flavor)
		return &location
	}
	return nil
}

// FirstEndPosUnresolved returns the first DDL End_log_pos if un-resolved, else nil
func (sg *ShardingGroup) FirstEndPosUnresolved() *binlog.Location {
	sg.RLock()
	defer sg.RUnlock()
	if sg.remain < len(sg.sources) && sg.firstEndLocation != nil {
		// create a new location to return
		location := sg.firstEndLocation.CloneWithFlavor(sg.flavor)
		return &location
	}
	return nil
}

// String implements Stringer.String
func (sg *ShardingGroup) String() string {
	return fmt.Sprintf("IsSchemaOnly:%v remain:%d, sources:%+v", sg.IsSchemaOnly, sg.remain, sg.sources)
}

// InSequenceSharding returns whether this sharding group is in sequence sharding
func (sg *ShardingGroup) InSequenceSharding() bool {
	sg.RLock()
	defer sg.RUnlock()
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

// ActiveDDLFirstLocation returns the first binlog position of active DDL
func (sg *ShardingGroup) ActiveDDLFirstLocation() (binlog.Location, error) {
	sg.RLock()
	defer sg.RUnlock()
	location, err := sg.meta.ActiveDDLFirstLocation()
	return location, err
}

// FlushData returns sharding meta flush SQLs and args
func (sg *ShardingGroup) FlushData(targetTableID string) ([]string, [][]interface{}) {
	sg.RLock()
	defer sg.RUnlock()
	return sg.meta.FlushData(sg.sourceID, targetTableID)
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

	shardMetaSchema string
	shardMetaTable  string

	db     *conn.BaseDB
	dbConn *DBConn

	tctx *tcontext.Context
}

// NewShardingGroupKeeper creates a new ShardingGroupKeeper
func NewShardingGroupKeeper(tctx *tcontext.Context, cfg *config.SubTaskConfig) *ShardingGroupKeeper {
	k := &ShardingGroupKeeper{
		groups: make(map[string]*ShardingGroup),
		cfg:    cfg,
		tctx:   tctx.WithLogger(tctx.L().WithFields(zap.String("component", "shard group keeper"))),
	}
	k.shardMetaSchema = cfg.MetaSchema
	k.shardMetaTable = fmt.Sprintf(shardmeta.MetaTableFormat, cfg.Name)
	return k
}

// AddGroup adds new group(s) according to target schema, table and source IDs
func (k *ShardingGroupKeeper) AddGroup(targetSchema, targetTable string, sourceIDs []string, meta *shardmeta.ShardingMeta, merge bool) (needShardingHandle bool, group *ShardingGroup, synced bool, remain int, err error) {
	// if need to support target table-level sharding DDL
	// we also need to support target schema-level sharding DDL
	schemaID, _ := GenTableID(targetSchema, "")
	targetTableID, _ := GenTableID(targetSchema, targetTable)

	k.Lock()
	defer k.Unlock()

	if schemaGroup, ok := k.groups[schemaID]; !ok {
		k.groups[schemaID] = NewShardingGroup(k.cfg.SourceID, k.shardMetaSchema, k.shardMetaTable, sourceIDs, meta, true, k.cfg.Flavor)
	} else {
		_, _, _, err = schemaGroup.Merge(sourceIDs)
		if err != nil {
			return
		}
	}

	var ok bool
	if group, ok = k.groups[targetTableID]; !ok {
		group = NewShardingGroup(k.cfg.SourceID, k.shardMetaSchema, k.shardMetaTable, sourceIDs, meta, false, k.cfg.Flavor)
		k.groups[targetTableID] = group
	} else if merge {
		needShardingHandle, synced, remain, err = k.groups[targetTableID].Merge(sourceIDs)
	} else {
		err = terror.ErrSyncUnitDupTableGroup.Generate(targetTableID)
	}

	return
}

// Init does initialization staff
func (k *ShardingGroupKeeper) Init() error {
	k.clear()
	sgkDB := k.cfg.To
	sgkDB.RawDBCfg = config.DefaultRawDBConfig().SetReadTimeout(maxCheckPointTimeout)
	db, dbConns, err := createConns(k.tctx, k.cfg, sgkDB, 1)
	if err != nil {
		return err
	}
	k.db = db
	k.dbConn = dbConns[0]
	return k.prepare()
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
		group.Reset()
	}
}

// LeaveGroup leaves group according to target schema, table and source IDs
// LeaveGroup doesn't affect in syncing process
func (k *ShardingGroupKeeper) LeaveGroup(targetSchema, targetTable string, sources []string) error {
	schemaID, _ := GenTableID(targetSchema, "")
	targetTableID, _ := GenTableID(targetSchema, targetTable)
	k.Lock()
	defer k.Unlock()
	if group, ok := k.groups[targetTableID]; ok {
		if err := group.Leave(sources); err != nil {
			return err
		}
	}
	if schemaGroup, ok := k.groups[schemaID]; ok {
		if err := schemaGroup.Leave(sources); err != nil {
			return err
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
	targetSchema, targetTable, source string, location, endLocation binlog.Location, ddls []string) (
	needShardingHandle bool, group *ShardingGroup, synced, active bool, remain int, err error) {

	targetTableID, schemaOnly := GenTableID(targetSchema, targetTable)
	if schemaOnly {
		// NOTE: now we don't support syncing for schema only sharding DDL
		return false, nil, true, false, 0, nil
	}

	k.Lock()
	defer k.Unlock()

	group, ok := k.groups[targetTableID]
	if !ok {
		return false, group, true, false, 0, nil
	}
	synced, active, remain, err = group.TrySync(source, location, endLocation, ddls)
	return true, group, synced, active, remain, err
}

// InSyncing checks whether the source is in sharding syncing
func (k *ShardingGroupKeeper) InSyncing(targetSchema, targetTable, source string, location binlog.Location) bool {
	group := k.Group(targetSchema, targetTable)
	if group == nil {
		return false
	}
	return !group.CheckSyncing(source, location)
}

// UnresolvedTables returns
//   all `target-schema.target-table` that has unresolved sharding DDL
//   all source tables which with DDLs are un-resolved
// NOTE: this func only ensure the returned tables are current un-resolved
// if passing the returned tables to other func (like checkpoint),
// must ensure their sync state not changed in this progress
func (k *ShardingGroupKeeper) UnresolvedTables() (map[string]bool, [][]string) {
	ids := make(map[string]bool)
	tables := make([][]string, 0, 10)
	k.RLock()
	defer k.RUnlock()
	for id, group := range k.groups {
		unresolved := group.UnresolvedTables()
		if len(unresolved) > 0 {
			ids[id] = true
			tables = append(tables, unresolved...)
		}
	}
	return ids, tables
}

// Group returns target table's group, nil if not exist
func (k *ShardingGroupKeeper) Group(targetSchema, targetTable string) *ShardingGroup {
	targetTableID, _ := GenTableID(targetSchema, targetTable)
	k.RLock()
	defer k.RUnlock()
	return k.groups[targetTableID]
}

// lowestFirstLocationInGroups returns the lowest pos in all groups which are unresolved
func (k *ShardingGroupKeeper) lowestFirstLocationInGroups() *binlog.Location {
	k.RLock()
	defer k.RUnlock()
	var lowest *binlog.Location
	for _, group := range k.groups {
		location := group.FirstLocationUnresolved()
		if location == nil {
			continue
		}
		if lowest == nil {
			lowest = location
		} else if binlog.CompareLocation(*lowest, *location) > 0 {
			lowest = location
		}
	}
	return lowest
}

// AdjustGlobalLocation adjusts globalLocation with sharding groups' lowest first point
func (k *ShardingGroupKeeper) AdjustGlobalLocation(globalLocation binlog.Location) binlog.Location {
	lowestFirstLocation := k.lowestFirstLocationInGroups()
	if lowestFirstLocation != nil && binlog.CompareLocation(*lowestFirstLocation, globalLocation) < 0 {
		return lowestFirstLocation.Clone()
	}
	return globalLocation.Clone()
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
	return false, terror.ErrSyncUnitShardingGroupNotFound.Generate(targetSchema, targetTable)
}

// ActiveDDLFirstLocation returns the binlog position of active DDL
func (k *ShardingGroupKeeper) ActiveDDLFirstLocation(targetSchema, targetTable string) (binlog.Location, error) {
	group := k.Group(targetSchema, targetTable)
	k.Lock()
	defer k.Unlock()
	if group != nil {
		location, err := group.ActiveDDLFirstLocation()
		return location, err
	}
	return binlog.Location{}, terror.ErrSyncUnitShardingGroupNotFound.Generate(targetSchema, targetTable)
}

// PrepareFlushSQLs returns all sharding meta flushed SQLs execpt for given table IDs
func (k *ShardingGroupKeeper) PrepareFlushSQLs(exceptTableIDs map[string]bool) ([]string, [][]interface{}) {
	k.RLock()
	defer k.RUnlock()
	var (
		sqls = make([]string, 0, len(k.groups))
		args = make([][]interface{}, 0, len(k.groups))
	)
	for id, group := range k.groups {
		if group.IsSchemaOnly {
			continue
		}
		_, ok := exceptTableIDs[id]
		if ok {
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
		return err
	}

	if err := k.createTable(); err != nil {
		return err
	}

	return nil
}

// Close closes sharding group keeper
func (k *ShardingGroupKeeper) Close() {
	closeBaseDB(k.tctx, k.db)
}

func (k *ShardingGroupKeeper) createSchema() error {
	stmt := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS `%s`", k.shardMetaSchema)
	_, err := k.dbConn.executeSQL(k.tctx, []string{stmt})
	k.tctx.L().Info("execute sql", zap.String("statement", stmt))
	return terror.WithScope(err, terror.ScopeDownstream)
}

func (k *ShardingGroupKeeper) createTable() error {
	tableName := fmt.Sprintf("`%s`.`%s`", k.shardMetaSchema, k.shardMetaTable)
	stmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		source_id VARCHAR(32) NOT NULL COMMENT 'replica source id, defined in task.yaml',
		target_table_id VARCHAR(144) NOT NULL,
		source_table_id  VARCHAR(144) NOT NULL,
		active_index INT,
		is_global BOOLEAN,
		data JSON,
		create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		UNIQUE KEY uk_source_id_table_id_source (source_id, target_table_id, source_table_id)
	)`, tableName)
	_, err := k.dbConn.executeSQL(k.tctx, []string{stmt})
	k.tctx.L().Info("execute sql", zap.String("statement", stmt))
	return terror.WithScope(err, terror.ScopeDownstream)
}

// LoadShardMeta implements CheckPoint.LoadShardMeta
func (k *ShardingGroupKeeper) LoadShardMeta(flavor string) (map[string]*shardmeta.ShardingMeta, error) {
	query := fmt.Sprintf("SELECT `target_table_id`, `source_table_id`, `active_index`, `is_global`, `data` FROM `%s`.`%s` WHERE `source_id`='%s'", k.shardMetaSchema, k.shardMetaTable, k.cfg.SourceID)
	rows, err := k.dbConn.querySQL(k.tctx, query)
	if err != nil {
		return nil, terror.WithScope(err, terror.ScopeDownstream)
	}
	defer rows.Close()

	var (
		targetTableID string
		sourceTableID string
		activeIndex   int
		isGlobal      bool
		data          string
		meta          = make(map[string]*shardmeta.ShardingMeta)
	)
	for rows.Next() {
		err := rows.Scan(&targetTableID, &sourceTableID, &activeIndex, &isGlobal, &data)
		if err != nil {
			return nil, terror.WithScope(terror.DBErrorAdapt(err, terror.ErrDBDriverError), terror.ScopeDownstream)
		}
		if _, ok := meta[targetTableID]; !ok {
			meta[targetTableID] = shardmeta.NewShardingMeta(k.shardMetaSchema, k.shardMetaTable)
		}
		err = meta[targetTableID].RestoreFromData(sourceTableID, activeIndex, isGlobal, []byte(data), flavor)
		if err != nil {
			return nil, err
		}
	}
	return meta, terror.WithScope(terror.DBErrorAdapt(rows.Err(), terror.ErrDBDriverError), terror.ScopeDownstream)
}

// ShardingReSync represents re-sync info for a sharding DDL group
type ShardingReSync struct {
	currLocation   binlog.Location // current DDL's binlog location, initialize to first DDL's location
	latestLocation binlog.Location // latest DDL's binlog location
	targetSchema   string
	targetTable    string
	allResolved    bool
}
