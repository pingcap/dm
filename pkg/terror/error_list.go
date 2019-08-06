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
	codeStatFileSize
	// pkg/streamer
	codeEmptyRelayDir
	codeReadDir
	codeBaseFileNotFound
	codeBinFileCmpCondNotSupport
	codeBinlogFileNotValid
	codeBinlogFilesNotFound
	codeGetRelayLogStat
	codeAddWatchForRelayLogDir
	codeWatcherStart
	codeWatcherChanClosed
	codeWatcherChanRecvError
	codeRelayLogFileSizeSmaller
	codeReaderAlreadyRunning
	codeBinlogFileNotSpecified
	codeNoRelayLogMatchPos
	codeFirstRelayLogNotMatchPos
	codeParserParseRelayLog
	codeNoSubdirToSwitch
	codeNeedSyncAgain
	codeSyncClosed
	// pkg/utils
	codeSchemaTableNameNotValid
	codeGenTableRouter
	codeEncryptSecretKeyNotValid
	codeEncryptNewCiphter
	codeEncryptGenIV
	codeCiphertextLenNotValid
	codeCiphertextContextNotValid
	codeInvalidBinlogPosStr
	codeEncCipherTextBase64Decode
	// pkg/binlog
	codeBinlogWriteBinaryData
	codeBinlogHeaderLengthNotValid
	codeBinlogEventDecode
	codeBinlogEmptyNextBinName
	codeBinlogParseSID
	codeBinlogEmptyGTID
	codeBinlogGTIDNotValid
	codeBinlogEmptyQuery
	codeBinlogTableMapEvNotValid
	codeBinlogExpectFormatDescEv
	codeBinlogExpectTableMapEv
	codeBinlogExpectRowsEv
	codeBinlogUnexpectedEV
	codeBinlogParseSingleEv
	codeBinlogEventTypeNotValid
	codeBinlogEventNoRows
	codeBinlogEventNoColumns
	codeBinlogEventRowLengthNotEq
	codeBinlogColumnTypeNotSupport
	codeBinlogGoMySQLTypeNotSupport
	codeBinlogColumnTypeMisMatch

	// Config related error code list
	codeConfigCheckItemNotSupport = iota + 1201
	codeConfigTomlTransform
	codeConfigTaskYamlTransform
	codeConfigTaskNameEmpty
	codeConfigEmptySourceID
	codeConfigTooLongSourceID
	codeConfigOnlineSchemeNotSupport
	codeConfigInvalidTimezone
	codeConfigParseFlagSet
	codeConfigDecryptDBPassword
	codeConfigMetaNoBinlogName
	codeConfigMySQLInstNotFound
	codeConfigMySQLInstsAtLeastOne
	codeConfigMySQLInstSameSourceID
	codeConfigMydumperCfgConflict
	codeConfigLoaderCfgConflict
	codeConfigSyncerCfgConflict
	codeConfigReadTaskCfgFromFile
	codeConfigNeedUniqueTaskName
	codeConfigInvalidTaskMode
	codeConfigNeedTargetDB
	codeConfigMetadataNotSet
	codeConfigRouteRuleNotFound
	codeConfigFilterRuleNotFound
	codeConfigColumnMappingNotFound
	codeConfigBWListNotFound
	codeConfigMydumperCfgNotFound
	codeConfigMydumperPathNotValid
	codeConfigLoaderCfgNotFound
	codeConfigSyncerCfgNotFound
	codeConfigSourceIDNotFound

	// Binlog operation error code list
	codeBinlogExtractPosition = iota + 1301
	codeBinlogInvalidFilename
	codeBinlogParsePosFromStr

	// Checkpoint error code
	codeCheckpointInvalidTaskMode = iota + 1401
	codeCheckpointSaveInvalidPos
	codeCheckpointInvalidTableFile
	codeCheckpointDBNotExistInFile
	codeCheckpointTableNotExistInFile
	codeCheckpointRestoreCountGreater

	// Task check error code
	codeTaskCheckSameTableName = iota + 1501
	codeTaskCheckFailedOpenDB
	codeTaskCheckNewTableRouter
	codeTaskCheckNewColumnMapping

	// Relay log basic API error
	codeRelayParseUUIDIndex = iota + 1601
	codeRelayParseUUIDSuffix
	codeRelayUUIDWithSuffixNotFound
	codeRelayGenFakeRotateEvent
	codeRelayNoValidRelaySubDir

	codeRelayUUIDSuffixNotValid = iota + 1701
	codeRelayUUIDSuffixLessThanPrev
	codeRelayLoadMetaData
	codeRelayBinlogNameNotValid
	codeRelayNoCurrentUUID
	codeRelayFlushLocalMeta
	codeRelayUpdateIndexFile
	codeRelayLogDirpathEmpty
	codeRelayReaderNotStateNew
	codeRelayReaderStateCannotClose
	codeRelayReaderNeedStart
	codeRelayTCPReaderStartSync
	codeRelayTCPReaderNilGTID
	codeRelayTCPReaderStartSyncGTID
	codeRelayTCPReaderGetEvent
	codeRelayWriterNotStateNew
	codeRelayWriterStateCannotClose
	codeRelayWriterNeedStart
	codeRelayWriterNotOpened
	codeRelayWriterExpectRotateEv
	codeRelayWriterRotateEvWithNoWriter
	codeRelayWriterStatusNotValid
	codeRelayWriterGetFileStat
	codeRelayWriterLatestPosGTFileSize
	codeRelayWriterFileOperate
	codeRelayCheckBinlogFileHeaderExist
	codeRelayCheckFormatDescEventExist
	codeRelayCheckFormatDescEventParseEv
	codeRelayCheckIsDuplicateEvent
	codeRelayUpdateGTID
	codeRelayNeedPrevGTIDEvBeforeGTIDEv
	codeRelayNeedMaGTIDListEvBeforeGTIDEv
	codeRelayMkdir
	codeRelaySwitchMasterNeedGTID
	codeRelayThisStrategyIsPurging
	codeRelayOtherStrategyIsPurging
	codeRelayPurgeIsForbidden
	codeRelayNoActiveRelayLog
	codeRelayPurgeRequestNotValid
	codeRelayTrimUUIDNotFound
	codeRelayRemoveFileFail
	codeRelayPurgeArgsNotValid

	// Dump unit error code
	codeDumpUnitRuntime = iota + 2001

	// Load unit error code
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

	// Sync unit error code
	codeSyncerUnitPanic = iota + 2201
	codeSyncUnitInvalidTableName
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
	codeSyncerUnitBinlogEventFilter
	codeSyncerUnitInvalidReplicaEvent
	codeSyncerUnitParseStmt
	codeSyncerUnitUUIDNotLatest
	codeSyncerUnitDDLExecChanCloseOrBusy
	codeSyncerUnitDDLChanDone
	codeSyncerUnitDDLChanCanceled
	codeSyncerUnitDDLOnMultipleTable
	codeSyncerUnitInjectDDLOnly
	codeSyncerUnitInjectDDLWithoutSchema
	codeSyncerUnitNotSupportedOperate
	codeSyncerUnitNilOperatorReq
	codeSyncerUnitDMLColumnNotMatch
	codeSyncerUnitDMLOldNewValueMismatch
	codeSyncerUnitDMLPruneColumnMismatch
	codeSyncerUnitNewBinlogEventFilter
	codeSyncerUnitGenTableRouter
	codeSyncerUnitNewColumnMapping
	codeSyncerUnitDoColumnMapping
	codeSyncerUnitCacheKeyNotFound
	codeSyncerUnitHeartbeatCheckConfig
	codeSyncerUnitHeartbeatRecordExists
	codeSyncerUnitHeartbeatRecordNotFound
	codeSyncerUnitHeartbeatRecordNotValid
	codeSyncerUnitOnlineDDLInvalidMeta
	codeSyncerUnitOnlineDDLSchemeNotSupport
	codeSyncerUnitOnlineDDLOnMultipleTable
	codeSyncerUnitGhostApplyEmptyTable
	codeSyncerUnitGhostRenameTableNotValid
	codeSyncerUnitGhostRenameToGhostTable
	codeSyncerUnitGhostRenameGhostTblToOther
	codeSyncerUnitGhostOnlineDDLOnGhostTbl
	codeSyncerUnitPTApplyEmptyTable
	codeSyncerUnitPTRenameTableNotValid
	codeSyncerUnitPTRenameToGhostTable
	codeSyncerUnitPTRenameGhostTblToOther
	codeSyncerUnitPTOnlineDDLOnGhostTbl
	codeSyncerUnitRemoteSteamerWithGTID
	codeSyncerUnitRemoteSteamerStartSync
	codeSyncerUnitGetTableFromDB
	codeSyncerUnitFirstEndPosNotFound
	codeSyncerUnitResolveCasualityFail
	codeSyncerUnitReopenStreamNotSupport
	codeSyncerUnitUpdateConfigInSharding
	codeSyncerUnitExecWithNoBlockingDDL

	// TODO: DM-master error code

	// TODO: DM-worker error code
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
	ErrStatFileSize         = New(codeStatFileSize, ClassFunctional, ScopeInternal, LevelHigh, "statfs")
	// pkg/streamer
	ErrEmptyRelayDir            = New(codeEmptyRelayDir, ClassFunctional, ScopeInternal, LevelHigh, "empty relay dir")
	ErrReadDir                  = New(codeReadDir, ClassFunctional, ScopeInternal, LevelHigh, "read dir: %s")
	ErrBaseFileNotFound         = New(codeBaseFileNotFound, ClassFunctional, ScopeInternal, LevelHigh, "base file %s in directory %s not found")
	ErrBinFileCmpCondNotSupport = New(codeBinFileCmpCondNotSupport, ClassFunctional, ScopeInternal, LevelHigh, "cmp condition %v not supported")
	ErrBinlogFileNotValid       = New(codeBinlogFileNotValid, ClassFunctional, ScopeInternal, LevelHigh, "binlog file %s not valid")
	ErrBinlogFilesNotFound      = New(codeBinlogFilesNotFound, ClassFunctional, ScopeInternal, LevelHigh, "binlog files in dir %s not found")
	ErrGetRelayLogStat          = New(codeGetRelayLogStat, ClassFunctional, ScopeInternal, LevelHigh, "get stat for relay log %s")
	ErrAddWatchForRelayLogDir   = New(codeAddWatchForRelayLogDir, ClassFunctional, ScopeInternal, LevelHigh, "add watch for relay log dir %s")
	ErrWatcherStart             = New(codeWatcherStart, ClassFunctional, ScopeInternal, LevelHigh, "add watch for relay log dir %s")
	ErrWatcherChanClosed        = New(codeWatcherChanClosed, ClassFunctional, ScopeInternal, LevelHigh, "watcher's %s chan for relay log dir %s closed")
	ErrWatcherChanRecvError     = New(codeWatcherChanRecvError, ClassFunctional, ScopeInternal, LevelHigh, "relay log dir %s")
	ErrRelayLogFileSizeSmaller  = New(codeRelayLogFileSizeSmaller, ClassFunctional, ScopeInternal, LevelHigh, "file size of relay log %s become smaller")
	ErrReaderAlreadyRunning     = New(codeReaderAlreadyRunning, ClassFunctional, ScopeInternal, LevelHigh, "binlog reader is already running")
	ErrBinlogFileNotSpecified   = New(codeBinlogFileNotSpecified, ClassFunctional, ScopeInternal, LevelHigh, "binlog file must be specified")
	ErrNoRelayLogMatchPos       = New(codeNoRelayLogMatchPos, ClassFunctional, ScopeInternal, LevelHigh, "no relay log files in dir %s match pos %")
	ErrFirstRelayLogNotMatchPos = New(codeFirstRelayLogNotMatchPos, ClassFunctional, ScopeInternal, LevelHigh, "the first relay log %s not match the start pos %v")
	ErrParserParseRelayLog      = New(codeParserParseRelayLog, ClassFunctional, ScopeInternal, LevelHigh, "relay log file %s")
	ErrNoSubdirToSwitch         = New(codeNoSubdirToSwitch, ClassFunctional, ScopeInternal, LevelHigh, "parse for previous sub relay directory finished, but no next sub directory need to switch not supported")
	ErrNeedSyncAgain            = New(codeNeedSyncAgain, ClassFunctional, ScopeInternal, LevelHigh, "Last sync error or closed, try sync and get event again")
	ErrSyncClosed               = New(codeSyncClosed, ClassFunctional, ScopeInternal, LevelHigh, "Sync was closed")
	// pkg/utils
	ErrSchemaTableNameNotValid   = New(codeSchemaTableNameNotValid, ClassFunctional, ScopeInternal, LevelHigh, "table name %s not valid")
	ErrGenTableRouter            = New(codeGenTableRouter, ClassFunctional, ScopeInternal, LevelHigh, "generate table router")
	ErrEncryptSecretKeyNotValid  = New(codeEncryptSecretKeyNotValid, ClassFunctional, ScopeInternal, LevelHigh, "key size should be 16, 24 or 32, but input key's size is %d")
	ErrEncryptNewCiphter         = New(codeEncryptNewCiphter, ClassFunctional, ScopeInternal, LevelHigh, "new ciphter")
	ErrEncryptGenIV              = New(codeEncryptGenIV, ClassFunctional, ScopeInternal, LevelHigh, "generate iv")
	ErrCiphertextLenNotValid     = New(codeCiphertextLenNotValid, ClassFunctional, ScopeInternal, LevelHigh, "ciphertext's length should be greater than %d, but got %d not valid")
	ErrCiphertextContextNotValid = New(codeCiphertextContextNotValid, ClassFunctional, ScopeInternal, LevelHigh, "ciphertext's content not valid")
	ErrInvalidBinlogPosStr       = New(codeInvalidBinlogPosStr, ClassFunctional, ScopeInternal, LevelHigh, "invalid mysql position string: %s")
	ErrEncCipherTextBase64Decode = New(codeEncCipherTextBase64Decode, ClassFunctional, ScopeInternal, LevelHigh, "decode base64 encoded password %s")
	// pkg/binlog
	ErrBinlogWriteBinaryData       = New(codeBinlogWriteBinaryData, ClassFunctional, ScopeInternal, LevelHigh, "")
	ErrBinlogHeaderLengthNotValid  = New(codeBinlogHeaderLengthNotValid, ClassFunctional, ScopeInternal, LevelHigh, "header length should be %d, but got %d not valid")
	ErrBinlogEventDecode           = New(codeBinlogEventDecode, ClassFunctional, ScopeInternal, LevelHigh, "decode % X")
	ErrBinlogEmptyNextBinName      = New(codeBinlogEmptyNextBinName, ClassFunctional, ScopeInternal, LevelHigh, "empty next binlog name not valid")
	ErrBinlogParseSID              = New(codeBinlogParseSID, ClassFunctional, ScopeInternal, LevelHigh, "")
	ErrBinlogEmptyGTID             = New(codeBinlogEmptyGTID, ClassFunctional, ScopeInternal, LevelHigh, "empty GTID set not valid")
	ErrBinlogGTIDNotValid          = New(codeBinlogGTIDNotValid, ClassFunctional, ScopeInternal, LevelHigh, "GTID set string %s fro MySQL not valid")
	ErrBinlogEmptyQuery            = New(codeBinlogEmptyQuery, ClassFunctional, ScopeInternal, LevelHigh, "empty query not valid")
	ErrBinlogTableMapEvNotValid    = New(codeBinlogTableMapEvNotValid, ClassFunctional, ScopeInternal, LevelHigh, "empty schema (% X) or table (% X) or column type (% X)")
	ErrBinlogExpectFormatDescEv    = New(codeBinlogExpectFormatDescEv, ClassFunctional, ScopeInternal, LevelHigh, "expect FormatDescriptionEvent, but got %+v")
	ErrBinlogExpectTableMapEv      = New(codeBinlogExpectTableMapEv, ClassFunctional, ScopeInternal, LevelHigh, "expect TableMapEvent, but got %+v")
	ErrBinlogExpectRowsEv          = New(codeBinlogExpectRowsEv, ClassFunctional, ScopeInternal, LevelHigh, "expect event with type (%d), but got %+v")
	ErrBinlogUnexpectedEV          = New(codeBinlogUnexpectedEV, ClassFunctional, ScopeInternal, LevelHigh, "unexpected event %+v")
	ErrBinlogParseSingleEv         = New(codeBinlogParseSingleEv, ClassFunctional, ScopeInternal, LevelHigh, "")
	ErrBinlogEventTypeNotValid     = New(codeBinlogEventTypeNotValid, ClassFunctional, ScopeInternal, LevelHigh, "event type %d not valid")
	ErrBinlogEventNoRows           = New(codeBinlogEventNoRows, ClassFunctional, ScopeInternal, LevelHigh, "no rows not valid")
	ErrBinlogEventNoColumns        = New(codeBinlogEventNoColumns, ClassFunctional, ScopeInternal, LevelHigh, "no columns not valid")
	ErrBinlogEventRowLengthNotEq   = New(codeBinlogEventRowLengthNotEq, ClassFunctional, ScopeInternal, LevelHigh, "length of row (%d) not equal to length of column-type (%d)")
	ErrBinlogColumnTypeNotSupport  = New(codeBinlogColumnTypeNotSupport, ClassFunctional, ScopeInternal, LevelHigh, "column type %d in binlog not supported")
	ErrBinlogGoMySQLTypeNotSupport = New(codeBinlogGoMySQLTypeNotSupport, ClassFunctional, ScopeInternal, LevelHigh, "go-mysql type %d in event generator not supported")
	ErrBinlogColumnTypeMisMatch    = New(codeBinlogColumnTypeMisMatch, ClassFunctional, ScopeInternal, LevelHigh, "value %+v (type %v) with column type %v not valid")

	// Config related error
	ErrConfigCheckItemNotSupport    = New(codeConfigCheckItemNotSupport, ClassConfig, ScopeInternal, LevelMedium, "checking item %s is not supported\n%s")
	ErrConfigTomlTransform          = New(codeConfigTomlTransform, ClassConfig, ScopeInternal, LevelMedium, "%s")
	ErrConfigTaskYamlTransform      = New(codeConfigTaskYamlTransform, ClassConfig, ScopeInternal, LevelMedium, "%s")
	ErrConfigTaskNameEmpty          = New(codeConfigTaskNameEmpty, ClassConfig, ScopeInternal, LevelMedium, "task name should not be empty")
	ErrConfigEmptySourceID          = New(codeConfigEmptySourceID, ClassConfig, ScopeInternal, LevelMedium, "empty source-id not valid")
	ErrConfigTooLongSourceID        = New(codeConfigTooLongSourceID, ClassConfig, ScopeInternal, LevelMedium, "too long source-id not valid")
	ErrConfigOnlineSchemeNotSupport = New(codeConfigOnlineSchemeNotSupport, ClassConfig, ScopeInternal, LevelMedium, "online scheme %s not supported")
	ErrConfigInvalidTimezone        = New(codeConfigInvalidTimezone, ClassConfig, ScopeInternal, LevelMedium, "invalid timezone string: %s")
	ErrConfigParseFlagSet           = New(codeConfigParseFlagSet, ClassConfig, ScopeInternal, LevelMedium, "parse subtask config flag set")
	ErrConfigDecryptDBPassword      = New(codeConfigDecryptDBPassword, ClassConfig, ScopeInternal, LevelMedium, "decrypt DB password %s failed")
	ErrConfigMetaNoBinlogName       = New(codeConfigMetaNoBinlogName, ClassConfig, ScopeInternal, LevelMedium, "binlog-name must specify")
	ErrConfigMySQLInstNotFound      = New(codeConfigMySQLInstNotFound, ClassConfig, ScopeInternal, LevelMedium, "mysql instance config must specify")
	ErrConfigMySQLInstsAtLeastOne   = New(codeConfigMySQLInstsAtLeastOne, ClassConfig, ScopeInternal, LevelMedium, "must specify at least one mysql-instances")
	ErrConfigMySQLInstSameSourceID  = New(codeConfigMySQLInstSameSourceID, ClassConfig, ScopeInternal, LevelMedium, "mysql-instance (%d) and (%d) have same source-id (%s)")
	ErrConfigMydumperCfgConflict    = New(codeConfigMydumperCfgConflict, ClassConfig, ScopeInternal, LevelMedium, "mydumper-config-name and mydumper should only specify one")
	ErrConfigLoaderCfgConflict      = New(codeConfigLoaderCfgConflict, ClassConfig, ScopeInternal, LevelMedium, "loader-config-name and loader should only specify one")
	ErrConfigSyncerCfgConflict      = New(codeConfigSyncerCfgConflict, ClassConfig, ScopeInternal, LevelMedium, "syncer-config-name and syncer should only specify one")
	ErrConfigReadTaskCfgFromFile    = New(codeConfigReadTaskCfgFromFile, ClassConfig, ScopeInternal, LevelMedium, "read config file %v")
	ErrConfigNeedUniqueTaskName     = New(codeConfigNeedUniqueTaskName, ClassConfig, ScopeInternal, LevelMedium, "must specify a unique task name")
	ErrConfigInvalidTaskMode        = New(codeConfigInvalidTaskMode, ClassConfig, ScopeInternal, LevelMedium, "please specify right task-mode, support `full`, `incremental`, `all`")
	ErrConfigNeedTargetDB           = New(codeConfigNeedTargetDB, ClassConfig, ScopeInternal, LevelMedium, "must specify target-database")
	ErrConfigMetadataNotSet         = New(codeConfigMetadataNotSet, ClassConfig, ScopeInternal, LevelMedium, "mysql-instance(%d) must set meta for task-mode %s")
	ErrConfigRouteRuleNotFound      = New(codeConfigRouteRuleNotFound, ClassConfig, ScopeInternal, LevelMedium, "mysql-instance(%d)'s route-rules %s not exist in routes")
	ErrConfigFilterRuleNotFound     = New(codeConfigFilterRuleNotFound, ClassConfig, ScopeInternal, LevelMedium, "mysql-instance(%d)'s filter-rules %s not exist in filters")
	ErrConfigColumnMappingNotFound  = New(codeConfigColumnMappingNotFound, ClassConfig, ScopeInternal, LevelMedium, "mysql-instance(%d)'s column-mapping-rules %s not exist in column-mapping")
	ErrConfigBWListNotFound         = New(codeConfigBWListNotFound, ClassConfig, ScopeInternal, LevelMedium, "mysql-instance(%d)'s list %s not exist in black white list")
	ErrConfigMydumperCfgNotFound    = New(codeConfigMydumperCfgNotFound, ClassConfig, ScopeInternal, LevelMedium, "mysql-instance(%d)'s mydumper config %s not exist in mydumpers")
	ErrConfigMydumperPathNotValid   = New(codeConfigMydumperPathNotValid, ClassConfig, ScopeInternal, LevelMedium, "mysql-instance(%d)'s mydumper-path must specify a valid path to mydumper binary when task-mode is all or full")
	ErrConfigLoaderCfgNotFound      = New(codeConfigLoaderCfgNotFound, ClassConfig, ScopeInternal, LevelMedium, "mysql-instance(%d)'s loader config %s not exist in loaders")
	ErrConfigSyncerCfgNotFound      = New(codeConfigSyncerCfgNotFound, ClassConfig, ScopeInternal, LevelMedium, "mysql-instance(%d)'s syncer config %s not exist in syncer")
	ErrConfigSourceIDNotFound       = New(codeConfigSourceIDNotFound, ClassConfig, ScopeInternal, LevelMedium, "source %s in deployment configuration")

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

	// Relay unit error
	ErrRelayUUIDSuffixNotValid           = New(codeRelayUUIDSuffixNotValid, ClassRelayUnit, ScopeInternal, LevelHigh, "UUID %s suffix %d should be 1 larger than previous suffix %d")
	ErrRelayUUIDSuffixLessThanPrev       = New(codeRelayUUIDSuffixLessThanPrev, ClassRelayUnit, ScopeInternal, LevelHigh, "previous UUID %s has suffix larger than %s")
	ErrRelayLoadMetaData                 = New(codeRelayLoadMetaData, ClassRelayUnit, ScopeInternal, LevelHigh, "load meta data")
	ErrRelayBinlogNameNotValid           = New(codeRelayBinlogNameNotValid, ClassRelayUnit, ScopeInternal, LevelHigh, "relay-binlog-name %s not valid")
	ErrRelayNoCurrentUUID                = New(codeRelayNoCurrentUUID, ClassRelayUnit, ScopeInternal, LevelHigh, "no current UUID set")
	ErrRelayFlushLocalMeta               = New(codeRelayFlushLocalMeta, ClassRelayUnit, ScopeInternal, LevelHigh, "flush local meta")
	ErrRelayUpdateIndexFile              = New(codeRelayUpdateIndexFile, ClassRelayUnit, ScopeInternal, LevelHigh, "update UUID index file %s")
	ErrRelayLogDirpathEmpty              = New(codeRelayLogDirpathEmpty, ClassRelayUnit, ScopeInternal, LevelHigh, "dirpath is empty")
	ErrRelayReaderNotStateNew            = New(codeRelayReaderNotStateNew, ClassRelayUnit, ScopeInternal, LevelHigh, "stage %s, expect %s, already started")
	ErrRelayReaderStateCannotClose       = New(codeRelayReaderStateCannotClose, ClassRelayUnit, ScopeInternal, LevelHigh, "stage %s, expect %s, can not close")
	ErrRelayReaderNeedStart              = New(codeRelayReaderNeedStart, ClassRelayUnit, ScopeInternal, LevelHigh, "stage %s, expect %s, please start the reader first")
	ErrRelayTCPReaderStartSync           = New(codeRelayTCPReaderStartSync, ClassRelayUnit, ScopeUpstream, LevelHigh, "start sync from position %s")
	ErrRelayTCPReaderNilGTID             = New(codeRelayTCPReaderNilGTID, ClassRelayUnit, ScopeInternal, LevelHigh, "nil GTID set not valid")
	ErrRelayTCPReaderStartSyncGTID       = New(codeRelayTCPReaderStartSyncGTID, ClassRelayUnit, ScopeUpstream, LevelHigh, "start sync from GTID set %s")
	ErrRelayTCPReaderGetEvent            = New(codeRelayTCPReaderGetEvent, ClassRelayUnit, ScopeUpstream, LevelHigh, "TCPReader get event")
	ErrRelayWriterNotStateNew            = New(codeRelayWriterNotStateNew, ClassRelayUnit, ScopeInternal, LevelHigh, "stage %s, expect %s, already started")
	ErrRelayWriterStateCannotClose       = New(codeRelayWriterStateCannotClose, ClassRelayUnit, ScopeInternal, LevelHigh, "stage %s, expect %s, can not close")
	ErrRelayWriterNeedStart              = New(codeRelayWriterNeedStart, ClassRelayUnit, ScopeInternal, LevelHigh, "stage %s, expect %s, please start the writer first")
	ErrRelayWriterNotOpened              = New(codeRelayWriterNotOpened, ClassRelayUnit, ScopeInternal, LevelHigh, "no underlying writer opened")
	ErrRelayWriterExpectRotateEv         = New(codeRelayWriterExpectRotateEv, ClassRelayUnit, ScopeInternal, LevelHigh, "except RotateEvent, but got %+v")
	ErrRelayWriterRotateEvWithNoWriter   = New(codeRelayWriterRotateEvWithNoWriter, ClassRelayUnit, ScopeInternal, LevelHigh, "non-fake RotateEvent %+v received, but no binlog file opened")
	ErrRelayWriterStatusNotValid         = New(codeRelayWriterStatusNotValid, ClassRelayUnit, ScopeInternal, LevelHigh, "invalid status type %T of the underlying writer")
	ErrRelayWriterGetFileStat            = New(codeRelayWriterGetFileStat, ClassRelayUnit, ScopeInternal, LevelHigh, "get stat for %s")
	ErrRelayWriterLatestPosGTFileSize    = New(codeRelayWriterLatestPosGTFileSize, ClassRelayUnit, ScopeInternal, LevelHigh, "latest pos %d greater than file size %d, should not happen")
	ErrRelayWriterFileOperate            = New(codeRelayWriterFileOperate, ClassRelayUnit, ScopeInternal, LevelHigh, "")
	ErrRelayCheckBinlogFileHeaderExist   = New(codeRelayCheckBinlogFileHeaderExist, ClassRelayUnit, ScopeInternal, LevelHigh, "")
	ErrRelayCheckFormatDescEventExist    = New(codeRelayCheckFormatDescEventExist, ClassRelayUnit, ScopeInternal, LevelHigh, "")
	ErrRelayCheckFormatDescEventParseEv  = New(codeRelayCheckFormatDescEventParseEv, ClassRelayUnit, ScopeInternal, LevelHigh, "parse %s")
	ErrRelayCheckIsDuplicateEvent        = New(codeRelayCheckIsDuplicateEvent, ClassRelayUnit, ScopeInternal, LevelHigh, "")
	ErrRelayUpdateGTID                   = New(codeRelayUpdateGTID, ClassRelayUnit, ScopeInternal, LevelHigh, "update GTID set %v with GTID %s")
	ErrRelayNeedPrevGTIDEvBeforeGTIDEv   = New(codeRelayNeedPrevGTIDEvBeforeGTIDEv, ClassRelayUnit, ScopeInternal, LevelHigh, "should have a PreviousGTIDsEvent before the GTIDEvent %+v")
	ErrRelayNeedMaGTIDListEvBeforeGTIDEv = New(codeRelayNeedMaGTIDListEvBeforeGTIDEv, ClassRelayUnit, ScopeInternal, LevelHigh, "should have a MariadbGTIDListEvent before the MariadbGTIDEvent %+v")
	ErrRelayMkdir                        = New(codeRelayMkdir, ClassRelayUnit, ScopeInternal, LevelHigh, "relay mkdir")
	ErrRelaySwitchMasterNeedGTID         = New(codeRelaySwitchMasterNeedGTID, ClassRelayUnit, ScopeInternal, LevelHigh, "can only switch relay's master server when GTID enabled")
	ErrRelayThisStrategyIsPurging        = New(codeRelayThisStrategyIsPurging, ClassRelayUnit, ScopeInternal, LevelHigh, "this strategy is purging")
	ErrRelayOtherStrategyIsPurging       = New(codeRelayOtherStrategyIsPurging, ClassRelayUnit, ScopeInternal, LevelHigh, "%s is purging")
	ErrRelayPurgeIsForbidden             = New(codeRelayPurgeIsForbidden, ClassRelayUnit, ScopeInternal, LevelHigh, "relay log purge is forbidden temporarily, because %s, please try again later")
	ErrRelayNoActiveRelayLog             = New(codeRelayNoActiveRelayLog, ClassRelayUnit, ScopeInternal, LevelHigh, "no active relay log file found")
	ErrRelayPurgeRequestNotValid         = New(codeRelayPurgeRequestNotValid, ClassRelayUnit, ScopeInternal, LevelHigh, "request %+v not valid")
	ErrRelayTrimUUIDNotFound             = New(codeRelayTrimUUIDNotFound, ClassRelayUnit, ScopeInternal, LevelHigh, "UUID %s in UUIDs %v not found")
	ErrRelayRemoveFileFail               = New(codeRelayRemoveFileFail, ClassRelayUnit, ScopeInternal, LevelHigh, "remove relay log %s %s")
	ErrRelayPurgeArgsNotValid            = New(codeRelayPurgeArgsNotValid, ClassRelayUnit, ScopeInternal, LevelHigh, "args (%T) %+v not valid")

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
	ErrSyncerUnitPanic                   = New(codeSyncerUnitPanic, ClassSyncUnit, ScopeInternal, LevelHigh, "panic error: %v")
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
	ErrSyncUnitDMLStatementFound            = New(codeSyncUnitDMLStatementFound, ClassSyncUnit, ScopeInternal, LevelHigh, "only support ROW format binlog, unexpected DML statement found in query event")
	ErrSyncerUnitBinlogEventFilter          = New(codeSyncerUnitBinlogEventFilter, ClassSyncUnit, ScopeInternal, LevelHigh, "")
	ErrSyncerUnitInvalidReplicaEvent        = New(codeSyncerUnitInvalidReplicaEvent, ClassSyncUnit, ScopeInternal, LevelHigh, "[syncer] invalid replication event type %v")
	ErrSyncerUnitParseStmt                  = New(codeSyncerUnitParseStmt, ClassSyncUnit, ScopeInternal, LevelHigh, "")
	ErrSyncerUnitUUIDNotLatest              = New(codeSyncerUnitUUIDNotLatest, ClassSyncUnit, ScopeInternal, LevelHigh, "UUID %s not the latest one in UUIDs %v")
	ErrSyncerUnitDDLExecChanCloseOrBusy     = New(codeSyncerUnitDDLExecChanCloseOrBusy, ClassSyncUnit, ScopeInternal, LevelHigh, "the chan has closed or already in sending")
	ErrSyncerUnitDDLChanDone                = New(codeSyncerUnitDDLChanDone, ClassSyncUnit, ScopeInternal, LevelHigh, "canceled from external")
	ErrSyncerUnitDDLChanCanceled            = New(codeSyncerUnitDDLChanCanceled, ClassSyncUnit, ScopeInternal, LevelHigh, "canceled by Close or Renew")
	ErrSyncerUnitDDLOnMultipleTable         = New(codeSyncerUnitDDLOnMultipleTable, ClassSyncUnit, ScopeInternal, LevelHigh, "ddl on multiple table: %s")
	ErrSyncerUnitInjectDDLOnly              = New(codeSyncerUnitInjectDDLOnly, ClassSyncUnit, ScopeInternal, LevelLow, "only support inject DDL for sharding group to be synced currently, but got %s")
	ErrSyncerUnitInjectDDLWithoutSchema     = New(codeSyncerUnitInjectDDLWithoutSchema, ClassSyncUnit, ScopeInternal, LevelLow, "injected DDL %s without schema name not valid")
	ErrSyncerUnitNotSupportedOperate        = New(codeSyncerUnitNotSupportedOperate, ClassSyncUnit, ScopeInternal, LevelMedium, "op %s not supported")
	ErrSyncerUnitNilOperatorReq             = New(codeSyncerUnitNilOperatorReq, ClassSyncUnit, ScopeInternal, LevelMedium, "nil request not valid")
	ErrSyncerUnitDMLColumnNotMatch          = New(codeSyncerUnitDMLColumnNotMatch, ClassSyncUnit, ScopeInternal, LevelHigh, "Column count doesn't match value count: %d (columns) vs %d (values)")
	ErrSyncerUnitDMLOldNewValueMismatch     = New(codeSyncerUnitDMLOldNewValueMismatch, ClassSyncUnit, ScopeInternal, LevelHigh, "Old value count doesn't match new value count: %d (old) vs %d (new)")
	ErrSyncerUnitDMLPruneColumnMismatch     = New(codeSyncerUnitDMLPruneColumnMismatch, ClassSyncUnit, ScopeInternal, LevelHigh, "prune DML columns and data mismatch in length: %d (columns) %d (data)")
	ErrSyncerUnitNewBinlogEventFilter       = New(codeSyncerUnitNewBinlogEventFilter, ClassSyncUnit, ScopeInternal, LevelHigh, "new binlog event filter")
	ErrSyncerUnitGenTableRouter             = New(codeSyncerUnitGenTableRouter, ClassSyncUnit, ScopeInternal, LevelHigh, "generate table router")
	ErrSyncerUnitNewColumnMapping           = New(codeSyncerUnitNewColumnMapping, ClassSyncUnit, ScopeInternal, LevelHigh, "new column mapping")
	ErrSyncerUnitDoColumnMapping            = New(codeSyncerUnitDoColumnMapping, ClassSyncUnit, ScopeInternal, LevelHigh, "mapping row data %v for table `%s`.`%s`")
	ErrSyncerUnitCacheKeyNotFound           = New(codeSyncerUnitCacheKeyNotFound, ClassSyncUnit, ScopeInternal, LevelHigh, "cache key %s in %s not found")
	ErrSyncerUnitHeartbeatCheckConfig       = New(codeSyncerUnitHeartbeatCheckConfig, ClassSyncUnit, ScopeInternal, LevelMedium, "")
	ErrSyncerUnitHeartbeatRecordExists      = New(codeSyncerUnitHeartbeatRecordExists, ClassSyncUnit, ScopeInternal, LevelMedium, "heartbeat slave record for task %s already exists")
	ErrSyncerUnitHeartbeatRecordNotFound    = New(codeSyncerUnitHeartbeatRecordNotFound, ClassSyncUnit, ScopeInternal, LevelMedium, "heartbeat slave record for task %s not found")
	ErrSyncerUnitHeartbeatRecordNotValid    = New(codeSyncerUnitHeartbeatRecordNotValid, ClassSyncUnit, ScopeInternal, LevelMedium, "heartbeat record %s not valid")
	ErrSyncerUnitOnlineDDLInvalidMeta       = New(codeSyncerUnitOnlineDDLInvalidMeta, ClassSyncUnit, ScopeInternal, LevelHigh, "online ddl meta invalid")
	ErrSyncerUnitOnlineDDLSchemeNotSupport  = New(codeSyncerUnitOnlineDDLSchemeNotSupport, ClassSyncUnit, ScopeInternal, LevelHigh, "online ddl scheme (%s) not supported")
	ErrSyncerUnitOnlineDDLOnMultipleTable   = New(codeSyncerUnitOnlineDDLOnMultipleTable, ClassSyncUnit, ScopeInternal, LevelHigh, "online ddl changes on multiple table: %s not supported")
	ErrSyncerUnitGhostApplyEmptyTable       = New(codeSyncerUnitGhostApplyEmptyTable, ClassSyncUnit, ScopeInternal, LevelHigh, "empty tables not valid")
	ErrSyncerUnitGhostRenameTableNotValid   = New(codeSyncerUnitGhostRenameTableNotValid, ClassSyncUnit, ScopeInternal, LevelHigh, "tables should contain old and new table name")
	ErrSyncerUnitGhostRenameToGhostTable    = New(codeSyncerUnitGhostRenameToGhostTable, ClassSyncUnit, ScopeInternal, LevelHigh, "rename table to ghost table %s not supported")
	ErrSyncerUnitGhostRenameGhostTblToOther = New(codeSyncerUnitGhostRenameGhostTblToOther, ClassSyncUnit, ScopeInternal, LevelHigh, "rename ghost table to other ghost table %s not supported")
	ErrSyncerUnitGhostOnlineDDLOnGhostTbl   = New(codeSyncerUnitGhostOnlineDDLOnGhostTbl, ClassSyncUnit, ScopeInternal, LevelHigh, "online ddls on ghost table `%s`.`%s`")
	ErrSyncerUnitPTApplyEmptyTable          = New(codeSyncerUnitPTApplyEmptyTable, ClassSyncUnit, ScopeInternal, LevelHigh, "empty tables not valid")
	ErrSyncerUnitPTRenameTableNotValid      = New(codeSyncerUnitPTRenameTableNotValid, ClassSyncUnit, ScopeInternal, LevelHigh, "tables should contain old and new table name")
	ErrSyncerUnitPTRenameToGhostTable       = New(codeSyncerUnitPTRenameToGhostTable, ClassSyncUnit, ScopeInternal, LevelHigh, "rename table to ghost table %s not supported")
	ErrSyncerUnitPTRenameGhostTblToOther    = New(codeSyncerUnitPTRenameGhostTblToOther, ClassSyncUnit, ScopeInternal, LevelHigh, "rename ghost table to other ghost table %s not supported")
	ErrSyncerUnitPTOnlineDDLOnGhostTbl      = New(codeSyncerUnitPTOnlineDDLOnGhostTbl, ClassSyncUnit, ScopeInternal, LevelHigh, "online ddls on ghost table `%s`.`%s`")
	ErrSyncerUnitRemoteSteamerWithGTID      = New(codeSyncerUnitRemoteSteamerWithGTID, ClassSyncUnit, ScopeInternal, LevelHigh, "open remote streamer with GTID mode not supported")
	ErrSyncerUnitRemoteSteamerStartSync     = New(codeSyncerUnitRemoteSteamerStartSync, ClassSyncUnit, ScopeInternal, LevelHigh, "start syncing binlog from remote streamer")
	ErrSyncerUnitGetTableFromDB             = New(codeSyncerUnitGetTableFromDB, ClassSyncUnit, ScopeInternal, LevelHigh, "invalid table `%s`.`%s`")
	ErrSyncerUnitFirstEndPosNotFound        = New(codeSyncerUnitFirstEndPosNotFound, ClassSyncUnit, ScopeInternal, LevelHigh, "no valid End_log_pos of the first DDL exists for sharding group with source %s")
	ErrSyncerUnitResolveCasualityFail       = New(codeSyncerUnitResolveCasualityFail, ClassSyncUnit, ScopeInternal, LevelHigh, "resolve karam error %v")
	ErrSyncerUnitReopenStreamNotSupport     = New(codeSyncerUnitReopenStreamNotSupport, ClassSyncUnit, ScopeInternal, LevelHigh, "reopen %T not supported")
	ErrSyncerUnitUpdateConfigInSharding     = New(codeSyncerUnitUpdateConfigInSharding, ClassSyncUnit, ScopeInternal, LevelHigh, "try update config when some tables' (%v) sharding DDL not synced not supported")
	ErrSyncerUnitExecWithNoBlockingDDL      = New(codeSyncerUnitExecWithNoBlockingDDL, ClassSyncUnit, ScopeInternal, LevelHigh, "process unit not waiting for sharding DDL to sync")
)
