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

package terror

// Error codes list
const (
	codeDBDriverError ErrCode = iota + 1001
	codeDBBadConn
	codeDBInvalidConn
	codeDBUnExpect
	codeDBQueryFailed
	codeDBExecuteFailed

	// Functional error code list
	codeParseMydumperMeta = iota + 1101
	codeGetFileSize
	codeDropMultipleTables
	codeRenameMultipleTables
	codeAlterMultipleTables
	codeParseSQL
	codeUnknownTypeDDL
	codeRestoreASTNode
	codeParseGTID
	codeNotSupportedFlavor
	codeNotMySQLGTID
	codeNotMariaDBGTID
	codeNotUUIDString
	codeMariaDBDomainID
	codeInvalidServerID
	codeGetSQLModeFromStr
	codeVerifySQLOperateArgs

	// Binlog operation error code list
	codeBinlogExtractPosition = iota + 1201
	codeBinlogInvalidFilename
	codeBinlogParsePosFromStr

	codeCheckpointInvalidTaskMode = iota + 1301
	codeCheckpointSaveInvalidPos
	codeCheckpointInvalidTableFile
	codeCheckpointDBNotExistInFile
	codeCheckpointTableNotExistInFile
	codeCheckpointRestoreCountGreater

	codeTaskCheckSameTableName = iota + 1401
	codeTaskCheckFailedOpenDB
	codeTaskCheckNewTableRouter
	codeTaskCheckNewColumnMapping

	codeRelayParseUUIDIndex = iota + 1501
	codeRelayParseUUIDSuffix
	codeRelayUUIDWithSuffixNotFound
	codeRelayGenFakeRotateEvent
	codeRelayNoValidRelaySubDir

	codeDumpUnitRuntime = iota + 2001

	codeLoadUnitCreateSchemaFile = iota + 2101
	codeLoadUnitInvalidFileEnding
	codeLoadUnitParseQuoteValues
	codeLoadUnitDoColumnMapping
	codeLoadUnitReadSchemaFile
	codeLoadUnitParseStatement
	codeLoadUnitNotCreateTable
	codeLoadUnitDispatchSQLFromFile
	codeLoadUnitInvalidInsertSQL
	codeLoadUnitGenTableRouter
	codeLoadUnitNewColumnMapping
	codeLoadUnitNoDBFile
	codeLoadUnitNoTableFile
	codeLoadUnitDumpDirNotFound
	codeLoadUnitDuplicateTableFile

	codeSyncUnitInvalidTableName = iota + 2201
	codeSyncUnitTableNameQuery
	codeSyncUnitNotSupportedDML
	codeSyncUnitAddTableInSharding
	codeSyncUnitDropSchemaTableInSharding
	codeSyncUnitInvalidShardMeta
	codeSyncUnitDDLWrongSequence
	codeSyncUnitDDLActiveIndexLarger
	codeSyncUnitDupTableGroup
	codeSyncUnitShardingGroupNotFound
	codeSyncUnitSafeModeSetCount
	codeSyncUnitCausalityConflict
	codeSyncUnitDMLStatementFound
	codeSyncerUnitOnlineDDLInvalidMeta
	codeSyncerUnitBinlogEventFilter
	codeSyncerUnitInvalidReplicaEvent
	codeSyncerUnitParseStmt
	codeSyncerUnitUUIDNotLatest
	codeSyncerUnitDDLExecChanCloseOrBusy
	codeSyncerUnitDDLChanDone
	codeSyncerUnitDDLChanCanceled
	codeSyncerUnitInjectDDLOnly
	codeSyncerUnitInjectDDLWithoutSchema
	codeSyncerUnitNotSupportedOperate
	codeSyncerUnitNilOperatorReq
	codeSyncerUnitDMLColumnNotMatch
	codeSyncerUnitDMLOldNewValueMismatch
	codeSyncerUnitDMLPruneColumnMismatch
	codeSyncerUnitDoColumnMapping
	codeSyncerUnitCacheKeyNotFound
	codeSyncerUnitHeartbeatCheckConfig
	codeSyncerUnitHeartbeatRecordExists
	codeSyncerUnitHeartbeatRecordNotFound
	codeSyncerUnitHeartbeatRecordNotValid
)

// Error instances
var (
	// Database operation related error
	ErrDBDriverError = New(codeDBDriverError, ClassDatabase, ScopeNotSet, LevelHigh, "database driver error")
	ErrDBBadConn     = New(codeDBBadConn, ClassDatabase, ScopeNotSet, LevelMedium, "database driver")
	ErrDBInvalidConn = New(codeDBInvalidConn, ClassDatabase, ScopeNotSet, LevelMedium, "database driver")

	ErrDBUnExpect      = New(codeDBUnExpect, ClassDatabase, ScopeNotSet, LevelMedium, "unexpect database error: %s")
	ErrDBQueryFailed   = New(codeDBQueryFailed, ClassDatabase, ScopeNotSet, LevelHigh, "query statement failed: %s")
	ErrDBExecuteFailed = New(codeDBExecuteFailed, ClassDatabase, ScopeNotSet, LevelHigh, "execute statement failed: %s")

	// Functional error
	ErrParseMydumperMeta    = New(codeParseMydumperMeta, ClassFunctional, ScopeInternal, LevelHigh, "parse metadata error: %s")
	ErrGetFileSize          = New(codeGetFileSize, ClassFunctional, ScopeInternal, LevelHigh, "get file size")
	ErrDropMultipleTables   = New(codeDropMultipleTables, ClassFunctional, ScopeInternal, LevelHigh, "not allow operation: drop multiple tables in one statement")
	ErrRenameMultipleTables = New(codeRenameMultipleTables, ClassFunctional, ScopeInternal, LevelHigh, "not allow operation: rename multiple tables in one statement")
	ErrAlterMultipleTables  = New(codeAlterMultipleTables, ClassFunctional, ScopeInternal, LevelHigh, "not allow operation: alter multiple tables in one statement")
	ErrParseSQL             = New(codeAlterMultipleTables, ClassFunctional, ScopeInternal, LevelHigh, "parse statement")
	ErrUnknownTypeDDL       = New(codeUnknownTypeDDL, ClassFunctional, ScopeInternal, LevelHigh, "unknown type ddl %s")
	ErrRestoreASTNode       = New(codeRestoreASTNode, ClassFunctional, ScopeInternal, LevelHigh, "restore ast node")
	ErrParseGTID            = New(codeParseGTID, ClassFunctional, ScopeInternal, LevelHigh, "parse GTID")
	ErrNotSupportedFlavor   = New(codeNotSupportedFlavor, ClassFunctional, ScopeInternal, LevelHigh, "flavor %s and gtid %s")
	ErrNotMySQLGTID         = New(codeNotMySQLGTID, ClassFunctional, ScopeInternal, LevelHigh, "%s is not mysql GTID set")
	ErrNotMariaDBGTID       = New(codeNotMariaDBGTID, ClassFunctional, ScopeInternal, LevelHigh, "%s is not mariadb GTID set")
	ErrNotUUIDString        = New(codeNotUUIDString, ClassFunctional, ScopeInternal, LevelHigh, "%v is not string")
	ErrMariaDBDomainID      = New(codeMariaDBDomainID, ClassFunctional, ScopeInternal, LevelHigh, "%v is not uint32")
	ErrInvalidServerID      = New(codeInvalidServerID, ClassFunctional, ScopeInternal, LevelHigh, "invalid server id")
	ErrGetSQLModeFromStr    = New(codeGetSQLModeFromStr, ClassFunctional, ScopeInternal, LevelHigh, "get sql from from string literal")
	ErrVerifySQLOperateArgs = New(codeVerifySQLOperateArgs, ClassFunctional, ScopeInternal, LevelLow, "")

	// Binlog operation error
	ErrBinlogExtractPosition = New(codeBinlogExtractPosition, ClassBinlogOp, ScopeInternal, LevelHigh, "")
	ErrBinlogInvalidFilename = New(codeBinlogInvalidFilename, ClassBinlogOp, ScopeInternal, LevelHigh, "invalid binlog filename")
	ErrBinlogParsePosFromStr = New(codeBinlogParsePosFromStr, ClassBinlogOp, ScopeInternal, LevelHigh, "")

	// Checkpoint error
	ErrCheckpointInvalidTaskMode     = New(codeCheckpointInvalidTaskMode, ClassCheckpoint, ScopeInternal, LevelMedium, "invalid task mode: %s")
	ErrCheckpointSaveInvalidPos      = New(codeCheckpointSaveInvalidPos, ClassCheckpoint, ScopeInternal, LevelHigh, "save point %v is older than current pos %v")
	ErrCheckpointInvalidTableFile    = New(codeCheckpointInvalidTableFile, ClassCheckpoint, ScopeInternal, LevelMedium, "invalid db table sql file - %s")
	ErrCheckpointDBNotExistInFile    = New(codeCheckpointDBNotExistInFile, ClassCheckpoint, ScopeInternal, LevelMedium, "db (%s) not exist in data files, but in checkpoint")
	ErrCheckpointTableNotExistInFile = New(codeCheckpointTableNotExistInFile, ClassCheckpoint, ScopeInternal, LevelMedium, "table (%s) not exist in db (%s) data files, but in checkpoint")
	ErrCheckpointRestoreCountGreater = New(codeCheckpointRestoreCountGreater, ClassCheckpoint, ScopeInternal, LevelMedium, "restoring count greater than total count for table[%v]")

	// Task check error
	ErrTaskCheckSameTableName    = New(codeTaskCheckSameTableName, ClassTaskCheck, ScopeInternal, LevelMedium, "same table name in case-sensitive %v")
	ErrTaskCheckFailedOpenDB     = New(codeTaskCheckFailedOpenDB, ClassTaskCheck, ScopeInternal, LevelHigh, "failed to open DSN %s:***@%s:%d")
	ErrTaskCheckNewTableRouter   = New(codeTaskCheckNewTableRouter, ClassTaskCheck, ScopeInternal, LevelMedium, "new table router error")
	ErrTaskCheckNewColumnMapping = New(codeTaskCheckNewColumnMapping, ClassTaskCheck, ScopeInternal, LevelMedium, "new column mapping error")

	// Relay log basic API error
	ErrRelayParseUUIDIndex         = New(codeRelayParseUUIDIndex, ClassRelayAPI, ScopeInternal, LevelHigh, "parse server-uuid.index")
	ErrRelayParseUUIDSuffix        = New(codeRelayParseUUIDSuffix, ClassRelayAPI, ScopeInternal, LevelHigh, "UUID (with suffix) %s not valid")
	ErrRelayUUIDWithSuffixNotFound = New(codeRelayUUIDWithSuffixNotFound, ClassRelayAPI, ScopeInternal, LevelHigh, "no UUID (with suffix) matched %s found in %s, all UUIDs are %v")
	ErrRelayGenFakeRotateEvent     = New(codeRelayGenFakeRotateEvent, ClassRelayAPI, ScopeInternal, LevelHigh, "generate fake rotate event")
	ErrRelayNoValidRelaySubDir     = New(codeRelayNoValidRelaySubDir, ClassRelayAPI, ScopeInternal, LevelHigh, "no valid relay sub directory exists")

	// Dump unit error
	ErrDumpUnitRuntime = New(codeDumpUnitRuntime, ClassDumpUnit, ScopeInternal, LevelHigh, "mydumper runs with error")

	// Load unit error
	ErrLoadUnitCreateSchemaFile    = New(codeLoadUnitCreateSchemaFile, ClassLoadUnit, ScopeInternal, LevelMedium, "generate schema file")
	ErrLoadUnitInvalidFileEnding   = New(codeLoadUnitInvalidFileEnding, ClassLoadUnit, ScopeInternal, LevelHigh, "cooresponding ending of sql: ')' not found")
	ErrLoadUnitParseQuoteValues    = New(codeLoadUnitParseQuoteValues, ClassLoadUnit, ScopeInternal, LevelHigh, "parse quote values error")
	ErrLoadUnitDoColumnMapping     = New(codeLoadUnitDoColumnMapping, ClassLoadUnit, ScopeInternal, LevelHigh, "mapping row data %v for table %+v")
	ErrLoadUnitReadSchemaFile      = New(codeLoadUnitReadSchemaFile, ClassLoadUnit, ScopeInternal, LevelHigh, "read schema from sql file")
	ErrLoadUnitParseStatement      = New(codeLoadUnitParseStatement, ClassLoadUnit, ScopeInternal, LevelHigh, "parse statement %s")
	ErrLoadUnitNotCreateTable      = New(codeLoadUnitNotCreateTable, ClassLoadUnit, ScopeInternal, LevelHigh, "statement %s for %s/%s is not create table statement")
	ErrLoadUnitDispatchSQLFromFile = New(codeLoadUnitDispatchSQLFromFile, ClassLoadUnit, ScopeInternal, LevelHigh, "dispatch sql")
	ErrLoadUnitInvalidInsertSQL    = New(codeLoadUnitInvalidInsertSQL, ClassLoadUnit, ScopeInternal, LevelHigh, "invalid insert sql %s")
	ErrLoadUnitGenTableRouter      = New(codeLoadUnitGenTableRouter, ClassLoadUnit, ScopeInternal, LevelHigh, "generate table router")
	ErrLoadUnitNewColumnMapping    = New(codeLoadUnitNewColumnMapping, ClassLoadUnit, ScopeInternal, LevelHigh, "new column mapping")
	ErrLoadUnitNoDBFile            = New(codeLoadUnitNoDBFile, ClassLoadUnit, ScopeInternal, LevelHigh, "invalid data sql file, cannot find db - %s")
	ErrLoadUnitNoTableFile         = New(codeLoadUnitNoTableFile, ClassLoadUnit, ScopeInternal, LevelHigh, "invalid data sql file, cannot find table - %s")
	ErrLoadUnitDumpDirNotFound     = New(codeLoadUnitDumpDirNotFound, ClassLoadUnit, ScopeInternal, LevelHigh, "%s does not exist or it's not a dir")
	ErrLoadUnitDuplicateTableFile  = New(codeLoadUnitDuplicateTableFile, ClassLoadUnit, ScopeInternal, LevelHigh, "invalid table schema file, duplicated item - %s")

	// Sync unit error
	ErrSyncUnitInvalidTableName          = New(codeSyncUnitInvalidTableName, ClassSyncUnit, ScopeInternal, LevelHigh, "extract table name for DML error: %s")
	ErrSyncUnitTableNameQuery            = New(codeSyncUnitTableNameQuery, ClassSyncUnit, ScopeInternal, LevelHigh, "table name parse error: %s")
	ErrSyncUnitNotSupportedDML           = New(codeSyncUnitNotSupportedDML, ClassSyncUnit, ScopeInternal, LevelHigh, "DMLNode %v not supported")
	ErrSyncUnitAddTableInSharding        = New(codeSyncUnitAddTableInSharding, ClassSyncUnit, ScopeInternal, LevelMedium, "in sequence sharding, add table, activeDDL: %s, sharding sequence: %s not supported")
	ErrSyncUnitDropSchemaTableInSharding = New(codeSyncUnitDropSchemaTableInSharding, ClassSyncUnit, ScopeInternal, LevelMedium, "in sequence sharding try drop sources %v not supported, activeDDL: %s, sharding sequence: %s")
	ErrSyncUnitInvalidShardMeta          = New(codeSyncUnitInvalidShardMeta, ClassSyncUnit, ScopeInternal, LevelHigh, "invalid sharding meta data")
	ErrSyncUnitDDLWrongSequence          = New(codeSyncUnitDDLWrongSequence, ClassSyncUnit, ScopeInternal, LevelHigh, "detect inconsistent DDL sequence from source %+v, right DDL sequence should be %+v")
	ErrSyncUnitDDLActiveIndexLarger      = New(codeSyncUnitDDLActiveIndexLarger, ClassSyncUnit, ScopeInternal, LevelHigh, "activeIdx %d larger than length of global DDLItems: %v")
	ErrSyncUnitDupTableGroup             = New(codeSyncUnitDupTableGroup, ClassSyncUnit, ScopeInternal, LevelHigh, "table group %s exists")
	ErrSyncUnitShardingGroupNotFound     = New(codeSyncUnitShardingGroupNotFound, ClassSyncUnit, ScopeInternal, LevelHigh, "sharding group for `%s`.`%s` not found")
	ErrSyncUnitSafeModeSetCount          = New(codeSyncUnitSafeModeSetCount, ClassSyncUnit, ScopeInternal, LevelHigh, "")
	ErrSyncUnitCausalityConflict         = New(codeSyncUnitCausalityConflict, ClassSyncUnit, ScopeInternal, LevelHigh, "some conflicts in causality, must be resolved")
	// ErrSyncUnitDMLStatementFound defines an error which means we found unexpected dml statement found in query event
	ErrSyncUnitDMLStatementFound         = New(codeSyncUnitDMLStatementFound, ClassSyncUnit, ScopeInternal, LevelHigh, "only support ROW format binlog, unexpected DML statement found in query event")
	ErrSyncerUnitOnlineDDLInvalidMeta    = New(codeSyncerUnitOnlineDDLInvalidMeta, ClassSyncUnit, ScopeInternal, LevelHigh, "online ddl meta invalid")
	ErrSyncerUnitBinlogEventFilter       = New(codeSyncerUnitBinlogEventFilter, ClassSyncUnit, ScopeInternal, LevelHigh, "")
	ErrSyncerUnitInvalidReplicaEvent     = New(codeSyncerUnitInvalidReplicaEvent, ClassSyncUnit, ScopeInternal, LevelHigh, "[syncer] invalid replication event type %v")
	ErrSyncerUnitParseStmt               = New(codeSyncerUnitParseStmt, ClassSyncUnit, ScopeInternal, LevelHigh, "")
	ErrSyncerUnitUUIDNotLatest           = New(codeSyncerUnitUUIDNotLatest, ClassSyncUnit, ScopeInternal, LevelHigh, "UUID %s not the latest one in UUIDs %v")
	ErrSyncerUnitDDLExecChanCloseOrBusy  = New(codeSyncerUnitDDLExecChanCloseOrBusy, ClassSyncUnit, ScopeInternal, LevelHigh, "the chan has closed or already in sending")
	ErrSyncerUnitDDLChanDone             = New(codeSyncerUnitDDLChanDone, ClassSyncUnit, ScopeInternal, LevelHigh, "canceled from external")
	ErrSyncerUnitDDLChanCanceled         = New(codeSyncerUnitDDLChanCanceled, ClassSyncUnit, ScopeInternal, LevelHigh, "canceled by Close or Renew")
	ErrSyncerUnitInjectDDLOnly           = New(codeSyncerUnitInjectDDLOnly, ClassSyncUnit, ScopeInternal, LevelLow, "only support inject DDL for sharding group to be synced currently, but got %s")
	ErrSyncerUnitInjectDDLWithoutSchema  = New(codeSyncerUnitInjectDDLWithoutSchema, ClassSyncUnit, ScopeInternal, LevelLow, "injected DDL %s without schema name not valid")
	ErrSyncerUnitNotSupportedOperate     = New(codeSyncerUnitNotSupportedOperate, ClassSyncUnit, ScopeInternal, LevelMedium, "op %s not supported")
	ErrSyncerUnitNilOperatorReq          = New(codeSyncerUnitNilOperatorReq, ClassSyncUnit, ScopeInternal, LevelMedium, "nil request not valid")
	ErrSyncerUnitDMLColumnNotMatch       = New(codeSyncerUnitDMLColumnNotMatch, ClassSyncUnit, ScopeInternal, LevelHigh, "Column count doesn't match value count: %d (columns) vs %d (values)")
	ErrSyncerUnitDMLOldNewValueMismatch  = New(codeSyncerUnitDMLOldNewValueMismatch, ClassSyncUnit, ScopeInternal, LevelHigh, "Old value count doesn't match new value count: %d (old) vs %d (new)")
	ErrSyncerUnitDMLPruneColumnMismatch  = New(codeSyncerUnitDMLPruneColumnMismatch, ClassSyncUnit, ScopeInternal, LevelHigh, "prune DML columns and data mismatch in length: %d (columns) %d (data)")
	ErrSyncerUnitDoColumnMapping         = New(codeSyncerUnitDoColumnMapping, ClassTaskCheck, ScopeInternal, LevelHigh, "mapping row data %v for table `%s`.`%s`")
	ErrSyncerUnitCacheKeyNotFound        = New(codeSyncerUnitCacheKeyNotFound, ClassTaskCheck, ScopeInternal, LevelHigh, "cache key %s in %s not found")
	ErrSyncerUnitHeartbeatCheckConfig    = New(codeSyncerUnitHeartbeatCheckConfig, ClassTaskCheck, ScopeInternal, LevelMedium, "")
	ErrSyncerUnitHeartbeatRecordExists   = New(codeSyncerUnitHeartbeatRecordExists, ClassTaskCheck, ScopeInternal, LevelMedium, "heartbeat slave record for task %s already exists")
	ErrSyncerUnitHeartbeatRecordNotFound = New(codeSyncerUnitHeartbeatRecordNotFound, ClassTaskCheck, ScopeInternal, LevelMedium, "heartbeat slave record for task %s not found")
	ErrSyncerUnitHeartbeatRecordNotValid = New(codeSyncerUnitHeartbeatRecordNotValid, ClassTaskCheck, ScopeInternal, LevelMedium, "heartbeat record %s not valid")
)
