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

// Database operation error code list
const (
	codeDBDriverError ErrCode = iota + 10001
	codeDBBadConn
	codeDBInvalidConn
	codeDBUnExpect
	codeDBQueryFailed
	codeDBExecuteFailed
)

// Functional error code list
const (
	codeParseMydumperMeta ErrCode = iota + 11001
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
	codeReaderAlreadyRunning
	codeReaderAlreadyStarted
	codeReaderStateCannotClose
	codeReaderShouldStartSync
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
	codeEncryptGenCipher
	codeEncryptGenIV
	codeCiphertextLenNotValid
	codeCiphertextContextNotValid
	codeInvalidBinlogPosStr
	codeEncCipherTextBase64Decode
	// pkg/binlog
	codeBinlogWriteBinaryData
	codeBinlogWriteDataToBuffer
	codeBinlogHeaderLengthNotValid
	codeBinlogEventDecode
	codeBinlogEmptyNextBinName
	codeBinlogParseSID
	codeBinlogEmptyGTID
	codeBinlogGTIDSetNotValid
	codeBinlogGTIDMySQLNotValid
	codeBinlogGTIDMariaDBNotValid
	codeBinlogMariaDBServerIDMismatch
	codeBinlogOnlyOneGTIDSupport
	codeBinlogOnlyOneIntervalInUUID
	codeBinlogIntervalValueNotValid
	codeBinlogEmptyQuery
	codeBinlogTableMapEvNotValid
	codeBinlogExpectFormatDescEv
	codeBinlogExpectTableMapEv
	codeBinlogExpectRowsEv
	codeBinlogUnexpectedEv
	codeBinlogParseSingleEv
	codeBinlogEventTypeNotValid
	codeBinlogEventNoRows
	codeBinlogEventNoColumns
	codeBinlogEventRowLengthNotEq
	codeBinlogColumnTypeNotSupport
	codeBinlogGoMySQLTypeNotSupport
	codeBinlogColumnTypeMisMatch
	codeBinlogDummyEvSizeTooSmall
	codeBinlogFlavorNotSupport
	codeBinlogDMLEmptyData
	codeBinlogLatestGTIDNotInPrev
	codeBinlogReadFileByGTID
	codeBinlogWriterNotStateNew
	codeBinlogWriterStateCannotClose
	codeBinlogWriterNeedStart
	codeBinlogWriterOpenFile
	codeBinlogWriterGetFileStat
	codeBinlogWriterWriteDataLen
	codeBinlogWriterFileNotOpened
	codeBinlogWriterFileSync
	codeBinlogPrevGTIDEvNotValid
	codeBinlogDecodeMySQLGTIDSet
	codeBinlogNeedMariaDBGTIDSet
	codeBinlogParseMariaDBGTIDSet
	codeBinlogMariaDBAddGTIDSet
	// pkg/tracing
	codeTracingEventDataNotValid
	codeTracingUploadData
	codeTracingEventTypeNotValid
	codeTracingGetTraceCode
	codeTracingDataChecksum
	codeTracingGetTSO
	// pkg/backoff
	codeBackoffArgsNotValid

	codeInitLoggerFail

	// pkg/gtid
	codeGTIDTruncateInvalid
	// pkg/streamer
	codeRelayLogGivenPosTooBig
	// pkg/election
	codeElectionCampaignFail
	codeElectionGetLeaderIDFail
)

// Config related error code list
const (
	codeConfigCheckItemNotSupport ErrCode = iota + 20001
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
	codeConfigDuplicateCfgItem
)

// Binlog operation error code list
const (
	codeBinlogExtractPosition ErrCode = iota + 22001
	codeBinlogInvalidFilename
	codeBinlogParsePosFromStr
)

// Checkpoint error code
const (
	codeCheckpointInvalidTaskMode ErrCode = iota + 24001
	codeCheckpointSaveInvalidPos
	codeCheckpointInvalidTableFile
	codeCheckpointDBNotExistInFile
	codeCheckpointTableNotExistInFile
	codeCheckpointRestoreCountGreater
)

// Task check error code
const (
	codeTaskCheckSameTableName ErrCode = iota + 26001
	codeTaskCheckFailedOpenDB
	codeTaskCheckGenTableRouter
	codeTaskCheckGenColumnMapping
	codeTaskCheckSyncConfigError
	codeTaskCheckGenBWList
)

// Relay log utils error code
const (
	codeRelayParseUUIDIndex ErrCode = iota + 28001
	codeRelayParseUUIDSuffix
	codeRelayUUIDWithSuffixNotFound
	codeRelayGenFakeRotateEvent
	codeRelayNoValidRelaySubDir
)

// Relay unit error code
const (
	codeRelayUUIDSuffixNotValid ErrCode = iota + 30001
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
	codePreviousGTIDsNotValid
)

// Dump unit error code
const (
	codeDumpUnitRuntime ErrCode = iota + 32001
	codeDumpUnitGenTableRouter
	codeDumpUnitGenBWList
)

// Load unit error code
const (
	codeLoadUnitCreateSchemaFile ErrCode = iota + 34001
	codeLoadUnitInvalidFileEnding
	codeLoadUnitParseQuoteValues
	codeLoadUnitDoColumnMapping
	codeLoadUnitReadSchemaFile
	codeLoadUnitParseStatement
	codeLoadUnitNotCreateTable
	codeLoadUnitDispatchSQLFromFile
	codeLoadUnitInvalidInsertSQL
	codeLoadUnitGenTableRouter
	codeLoadUnitGenColumnMapping
	codeLoadUnitNoDBFile
	codeLoadUnitNoTableFile
	codeLoadUnitDumpDirNotFound
	codeLoadUnitDuplicateTableFile
	codeLoadUnitGenBWList
)

// Sync unit error code
const (
	codeSyncerUnitPanic ErrCode = iota + 36001
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
	codeSyncerUnitGenBinlogEventFilter
	codeSyncerUnitGenTableRouter
	codeSyncerUnitGenColumnMapping
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
	codeSyncerUnitGenBWList
)

// DM-master error code
const (
	codeMasterSQLOpNilRequest ErrCode = iota + 38001
	codeMasterSQLOpNotSupport
	codeMasterSQLOpWithoutSharding
	codeMasterGRPCCreateConn
	codeMasterGRPCSendOnCloseConn
	codeMasterGRPCClientClose
	codeMasterGRPCInvalidReqType
	codeMasterGRPCRequestError
	codeMasterDeployMapperVerify
	codeMasterConfigParseFlagSet
	codeMasterConfigUnknownItem
	codeMasterConfigInvalidFlag
	codeMasterConfigTomlTransform
	codeMasterConfigTimeoutParse
	codeMasterConfigUpdateCfgFile
	codeMasterShardingDDLDiff
	codeMasterStartService
	codeMasterNoEmitToken
	codeMasterLockNotFound
	codeMasterLockIsResolving
	codeMasterWorkerCliNotFound
	codeMasterWorkerNotWaitLock
	codeMasterHandleSQLReqFail
	codeMasterOwnerExecDDL
	codeMasterPartWorkerExecDDLFail
	codeMasterWorkerExistDDLLock
	codeMasterGetWorkerCfgExtractor
	codeMasterTaskConfigExtractor
	codeMasterWorkerArgsExtractor
	codeMasterQueryWorkerConfig
	codeMasterOperNotFound
	codeMasterOperRespNotSuccess
	codeMasterOperRequestTimeout
	codeMasterHandleHTTPApis
	codeMasterHostPortNotValid
	codeMasterGetHostnameFail
	codeMasterGenEmbedEtcdConfigFail
	codeMasterStartEmbedEtcdFail
	codeMasterParseURLFail
	codeMasterJoinEmbedEtcdFail
)

// DM-worker error code
const (
	codeWorkerParseFlagSet ErrCode = iota + 40001
	codeWorkerInvalidFlag
	codeWorkerDecodeConfigFromFile
	codeWorkerUndecodedItemFromFile
	codeWorkerNeedSourceID
	codeWorkerTooLongSourceID
	codeWorkerRelayBinlogName
	codeWorkerWriteConfigFile
	codeWorkerLogInvalidHandler
	codeWorkerLogPointerInvalid
	codeWorkerLogFetchPointer
	codeWorkerLogUnmarshalPointer
	codeWorkerLogClearPointer
	codeWorkerLogTaskKeyNotValid
	codeWorkerLogUnmarshalTaskKey
	codeWorkerLogFetchLogIter
	codeWorkerLogGetTaskLog
	codeWorkerLogUnmarshalBinary
	codeWorkerLogForwardPointer
	codeWorkerLogMarshalTask
	codeWorkerLogSaveTask
	codeWorkerLogDeleteKV
	codeWorkerLogDeleteKVIter
	codeWorkerLogUnmarshalTaskMeta
	codeWorkerLogFetchTaskFromMeta
	codeWorkerLogVerifyTaskMeta
	codeWorkerLogSaveTaskMeta
	codeWorkerLogGetTaskMeta
	codeWorkerLogDeleteTaskMeta
	codeWorkerMetaTomlTransform
	codeWorkerMetaOldFileStat
	codeWorkerMetaOldReadFile
	codeWorkerMetaEncodeTask
	codeWorkerMetaRemoveOldDir
	codeWorkerMetaTaskLogNotFound
	codeWorkerMetaHandleTaskOrder
	codeWorkerMetaOpenTxn
	codeWorkerMetaCommitTxn
	codeWorkerRelayStageNotValid
	codeWorkerRelayOperNotSupport
	codeWorkerOpenKVDBFile
	codeWorkerUpgradeCheckKVDir
	codeWorkerMarshalVerBinary
	codeWorkerUnmarshalVerBinary
	codeWorkerGetVersionFromKV
	codeWorkerSaveVersionToKV
	codeWorkerVerAutoDowngrade
	codeWorkerStartService
	codeWorkerAlreadyClosed
	codeWorkerNotRunningStage
	codeWorkerNotPausedStage
	codeWorkerUpdateTaskStage
	codeWorkerMigrateStopRelay
	codeWorkerSubTaskNotFound
	codeWorkerSubTaskExists
	codeWorkerOperSyncUnitOnly
	codeWorkerRelayUnitStage
	codeWorkerNoSyncerRunning
	codeWorkerCannotUpdateSourceID
	codeWorkerNoAvailUnits
	codeWorkerDDLLockInfoNotFound
	codeWorkerDDLLockInfoExists
	codeWorkerCacheDDLInfoExists
	codeWorkerExecSkipDDLConflict
	codeWorkerExecDDLSyncerOnly
	codeWorkerExecDDLTimeout
	codeWorkerWaitRelayCatchupTimeout
	codeWorkerRelayIsPurging
	codeWorkerHostPortNotValid
	codeWorkerNoStart
	codeWorkerAlreadyStarted
	codeWorkerSourceNotMatch
)

// DM-tracer error code
const (
	codeTracerParseFlagSet ErrCode = iota + 42001
	codeTracerConfigTomlTransform
	codeTracerConfigInvalidFlag
	codeTracerTraceEventNotFound
	codeTracerTraceIDNotProvided
	codeTracerParamNotValid
	codeTracerPostMethodOnly
	codeTracerEventAssertionFail
	codeTracerEventTypeNotValid
	codeTracerStartService
)

// Error instances
var (
	// Database operation related error
	ErrDBDriverError = New(codeDBDriverError, ClassDatabase, ScopeNotSet, LevelHigh, "database driver error")
	ErrDBBadConn     = New(codeDBBadConn, ClassDatabase, ScopeNotSet, LevelHigh, "database driver")
	ErrDBInvalidConn = New(codeDBInvalidConn, ClassDatabase, ScopeNotSet, LevelHigh, "database driver")

	ErrDBUnExpect      = New(codeDBUnExpect, ClassDatabase, ScopeNotSet, LevelHigh, "unexpect database error: %s")
	ErrDBQueryFailed   = New(codeDBQueryFailed, ClassDatabase, ScopeNotSet, LevelHigh, "query statement failed: %s")
	ErrDBExecuteFailed = New(codeDBExecuteFailed, ClassDatabase, ScopeNotSet, LevelHigh, "execute statement failed: %s")

	// Functional error
	ErrParseMydumperMeta      = New(codeParseMydumperMeta, ClassFunctional, ScopeInternal, LevelHigh, "parse mydumper metadata error: %s")
	ErrGetFileSize            = New(codeGetFileSize, ClassFunctional, ScopeInternal, LevelHigh, "get file %s size")
	ErrDropMultipleTables     = New(codeDropMultipleTables, ClassFunctional, ScopeInternal, LevelHigh, "not allowed operation: drop multiple tables in one statement")
	ErrRenameMultipleTables   = New(codeRenameMultipleTables, ClassFunctional, ScopeInternal, LevelHigh, "not allowed operation: rename multiple tables in one statement")
	ErrAlterMultipleTables    = New(codeAlterMultipleTables, ClassFunctional, ScopeInternal, LevelHigh, "not allowed operation: alter multiple tables in one statement")
	ErrParseSQL               = New(codeParseSQL, ClassFunctional, ScopeInternal, LevelHigh, "parse statement")
	ErrUnknownTypeDDL         = New(codeUnknownTypeDDL, ClassFunctional, ScopeInternal, LevelHigh, "unknown type ddl %+v")
	ErrRestoreASTNode         = New(codeRestoreASTNode, ClassFunctional, ScopeInternal, LevelHigh, "restore ast node")
	ErrParseGTID              = New(codeParseGTID, ClassFunctional, ScopeInternal, LevelHigh, "parse GTID %s")
	ErrNotSupportedFlavor     = New(codeNotSupportedFlavor, ClassFunctional, ScopeInternal, LevelHigh, "flavor %s not supported")
	ErrNotMySQLGTID           = New(codeNotMySQLGTID, ClassFunctional, ScopeInternal, LevelHigh, "%s is not mysql GTID set")
	ErrNotMariaDBGTID         = New(codeNotMariaDBGTID, ClassFunctional, ScopeInternal, LevelHigh, "%s is not mariadb GTID set")
	ErrNotUUIDString          = New(codeNotUUIDString, ClassFunctional, ScopeInternal, LevelHigh, "%v is not UUID string")
	ErrMariaDBDomainID        = New(codeMariaDBDomainID, ClassFunctional, ScopeInternal, LevelHigh, "%v is not uint32")
	ErrInvalidServerID        = New(codeInvalidServerID, ClassFunctional, ScopeInternal, LevelHigh, "invalid server id %s")
	ErrGetSQLModeFromStr      = New(codeGetSQLModeFromStr, ClassFunctional, ScopeInternal, LevelHigh, "get sql mode from string literal %s")
	ErrVerifySQLOperateArgs   = New(codeVerifySQLOperateArgs, ClassFunctional, ScopeInternal, LevelLow, "")
	ErrStatFileSize           = New(codeStatFileSize, ClassFunctional, ScopeInternal, LevelHigh, "get file statfs")
	ErrReaderAlreadyRunning   = New(codeReaderAlreadyRunning, ClassFunctional, ScopeInternal, LevelHigh, "binlog reader is already running")
	ErrReaderAlreadyStarted   = New(codeReaderAlreadyStarted, ClassFunctional, ScopeInternal, LevelHigh, "stage %s, expect %s, already started")
	ErrReaderStateCannotClose = New(codeReaderStateCannotClose, ClassFunctional, ScopeInternal, LevelHigh, "stage %s, expect %s, can not close")
	ErrReaderShouldStartSync  = New(codeReaderShouldStartSync, ClassFunctional, ScopeInternal, LevelHigh, "stage %s, expect %s, please start sync first")
	// pkg/streamer
	ErrEmptyRelayDir            = New(codeEmptyRelayDir, ClassFunctional, ScopeInternal, LevelHigh, "empty relay dir")
	ErrReadDir                  = New(codeReadDir, ClassFunctional, ScopeInternal, LevelHigh, "read dir: %s")
	ErrBaseFileNotFound         = New(codeBaseFileNotFound, ClassFunctional, ScopeInternal, LevelHigh, "base file %s in directory %s not found")
	ErrBinFileCmpCondNotSupport = New(codeBinFileCmpCondNotSupport, ClassFunctional, ScopeInternal, LevelHigh, "cmp condition %v not supported")
	ErrBinlogFileNotValid       = New(codeBinlogFileNotValid, ClassFunctional, ScopeInternal, LevelHigh, "binlog file %s not valid")
	ErrBinlogFilesNotFound      = New(codeBinlogFilesNotFound, ClassFunctional, ScopeInternal, LevelHigh, "binlog files in dir %s not found")
	ErrGetRelayLogStat          = New(codeGetRelayLogStat, ClassFunctional, ScopeInternal, LevelHigh, "get stat for relay log %s")
	ErrAddWatchForRelayLogDir   = New(codeAddWatchForRelayLogDir, ClassFunctional, ScopeInternal, LevelHigh, "add watch for relay log dir %s")
	ErrWatcherStart             = New(codeWatcherStart, ClassFunctional, ScopeInternal, LevelHigh, "watcher starts for relay log dir %s")
	ErrWatcherChanClosed        = New(codeWatcherChanClosed, ClassFunctional, ScopeInternal, LevelHigh, "watcher's %s chan for relay log dir %s closed")
	ErrWatcherChanRecvError     = New(codeWatcherChanRecvError, ClassFunctional, ScopeInternal, LevelHigh, "watcher receives error, relay log dir %s")
	ErrRelayLogFileSizeSmaller  = New(codeRelayLogFileSizeSmaller, ClassFunctional, ScopeInternal, LevelHigh, "file size of relay log %s become smaller, please check the status of relay log and re-pull it. If you want to re-pull it, you should open relay.meta, set the binlog-name to the error pos name, set binlog-pos to 4, delete the stashed relay log and run `resume-relay` in dmctl")
	ErrBinlogFileNotSpecified   = New(codeBinlogFileNotSpecified, ClassFunctional, ScopeInternal, LevelHigh, "binlog file must be specified")
	ErrNoRelayLogMatchPos       = New(codeNoRelayLogMatchPos, ClassFunctional, ScopeInternal, LevelHigh, "no relay log files in dir %s match pos %s")
	ErrFirstRelayLogNotMatchPos = New(codeFirstRelayLogNotMatchPos, ClassFunctional, ScopeInternal, LevelHigh, "the first relay log %s not match the start pos %v")
	ErrParserParseRelayLog      = New(codeParserParseRelayLog, ClassFunctional, ScopeInternal, LevelHigh, "parse relay log file %s")
	ErrNoSubdirToSwitch         = New(codeNoSubdirToSwitch, ClassFunctional, ScopeInternal, LevelHigh, "parse for previous sub relay directory finished, but no next sub directory need to switch not supported")
	ErrNeedSyncAgain            = New(codeNeedSyncAgain, ClassFunctional, ScopeInternal, LevelHigh, "Last sync error or closed, try sync and get event again")
	ErrSyncClosed               = New(codeSyncClosed, ClassFunctional, ScopeInternal, LevelHigh, "Sync was closed")
	// pkg/utils
	ErrSchemaTableNameNotValid   = New(codeSchemaTableNameNotValid, ClassFunctional, ScopeInternal, LevelHigh, "table name %s not valid")
	ErrGenTableRouter            = New(codeGenTableRouter, ClassFunctional, ScopeInternal, LevelHigh, "generate table router")
	ErrEncryptSecretKeyNotValid  = New(codeEncryptSecretKeyNotValid, ClassFunctional, ScopeInternal, LevelHigh, "key size should be 16, 24 or 32, but input key's size is %d")
	ErrEncryptGenCipher          = New(codeEncryptGenCipher, ClassFunctional, ScopeInternal, LevelHigh, "generate cipher")
	ErrEncryptGenIV              = New(codeEncryptGenIV, ClassFunctional, ScopeInternal, LevelHigh, "generate iv")
	ErrCiphertextLenNotValid     = New(codeCiphertextLenNotValid, ClassFunctional, ScopeInternal, LevelHigh, "ciphertext's length should be greater than %d, but got %d not valid")
	ErrCiphertextContextNotValid = New(codeCiphertextContextNotValid, ClassFunctional, ScopeInternal, LevelHigh, "ciphertext's content not valid")
	ErrInvalidBinlogPosStr       = New(codeInvalidBinlogPosStr, ClassFunctional, ScopeInternal, LevelHigh, "invalid mysql position string: %s")
	ErrEncCipherTextBase64Decode = New(codeEncCipherTextBase64Decode, ClassFunctional, ScopeInternal, LevelHigh, "decode base64 encoded password %s")
	// pkg/binlog
	ErrBinlogWriteBinaryData         = New(codeBinlogWriteBinaryData, ClassFunctional, ScopeInternal, LevelHigh, "")
	ErrBinlogWriteDataToBuffer       = New(codeBinlogWriteDataToBuffer, ClassFunctional, ScopeInternal, LevelHigh, "")
	ErrBinlogHeaderLengthNotValid    = New(codeBinlogHeaderLengthNotValid, ClassFunctional, ScopeInternal, LevelHigh, "header length should be %d, but got %d not valid")
	ErrBinlogEventDecode             = New(codeBinlogEventDecode, ClassFunctional, ScopeInternal, LevelHigh, "decode % X")
	ErrBinlogEmptyNextBinName        = New(codeBinlogEmptyNextBinName, ClassFunctional, ScopeInternal, LevelHigh, "empty next binlog name not valid")
	ErrBinlogParseSID                = New(codeBinlogParseSID, ClassFunctional, ScopeInternal, LevelHigh, "")
	ErrBinlogEmptyGTID               = New(codeBinlogEmptyGTID, ClassFunctional, ScopeInternal, LevelHigh, "empty GTID set not valid")
	ErrBinlogGTIDSetNotValid         = New(codeBinlogGTIDSetNotValid, ClassFunctional, ScopeInternal, LevelHigh, "GTID set %s with flavor %s not valid")
	ErrBinlogGTIDMySQLNotValid       = New(codeBinlogGTIDMySQLNotValid, ClassFunctional, ScopeInternal, LevelHigh, "GTID set string %s for MySQL not valid")
	ErrBinlogGTIDMariaDBNotValid     = New(codeBinlogGTIDMariaDBNotValid, ClassFunctional, ScopeInternal, LevelHigh, "GTID set string %s for MariaDB not valid")
	ErrBinlogMariaDBServerIDMismatch = New(codeBinlogMariaDBServerIDMismatch, ClassFunctional, ScopeInternal, LevelHigh, "server_id mismatch, in GTID (%d), in event header/server_id (%d)")
	ErrBinlogOnlyOneGTIDSupport      = New(codeBinlogOnlyOneGTIDSupport, ClassFunctional, ScopeInternal, LevelHigh, "only one GTID in set is supported, but got %d (%s)")
	ErrBinlogOnlyOneIntervalInUUID   = New(codeBinlogOnlyOneIntervalInUUID, ClassFunctional, ScopeInternal, LevelHigh, "only one Interval in UUIDSet is supported, but got %d (%s)")
	ErrBinlogIntervalValueNotValid   = New(codeBinlogIntervalValueNotValid, ClassFunctional, ScopeInternal, LevelHigh, "Interval's Stop should equal to Start+1, but got %+v (%s)")
	ErrBinlogEmptyQuery              = New(codeBinlogEmptyQuery, ClassFunctional, ScopeInternal, LevelHigh, "empty query not valid")
	ErrBinlogTableMapEvNotValid      = New(codeBinlogTableMapEvNotValid, ClassFunctional, ScopeInternal, LevelHigh, "empty schema (% X) or table (% X) or column type (% X)")
	ErrBinlogExpectFormatDescEv      = New(codeBinlogExpectFormatDescEv, ClassFunctional, ScopeInternal, LevelHigh, "expect FormatDescriptionEvent, but got %+v")
	ErrBinlogExpectTableMapEv        = New(codeBinlogExpectTableMapEv, ClassFunctional, ScopeInternal, LevelHigh, "expect TableMapEvent, but got %+v")
	ErrBinlogExpectRowsEv            = New(codeBinlogExpectRowsEv, ClassFunctional, ScopeInternal, LevelHigh, "expect event with type (%d), but got %+v")
	ErrBinlogUnexpectedEv            = New(codeBinlogUnexpectedEv, ClassFunctional, ScopeInternal, LevelHigh, "unexpected event %+v")
	ErrBinlogParseSingleEv           = New(codeBinlogParseSingleEv, ClassFunctional, ScopeInternal, LevelHigh, "")
	ErrBinlogEventTypeNotValid       = New(codeBinlogEventTypeNotValid, ClassFunctional, ScopeInternal, LevelHigh, "event type %d not valid")
	ErrBinlogEventNoRows             = New(codeBinlogEventNoRows, ClassFunctional, ScopeInternal, LevelHigh, "no rows not valid")
	ErrBinlogEventNoColumns          = New(codeBinlogEventNoColumns, ClassFunctional, ScopeInternal, LevelHigh, "no columns not valid")
	ErrBinlogEventRowLengthNotEq     = New(codeBinlogEventRowLengthNotEq, ClassFunctional, ScopeInternal, LevelHigh, "length of row (%d) not equal to length of column-type (%d)")
	ErrBinlogColumnTypeNotSupport    = New(codeBinlogColumnTypeNotSupport, ClassFunctional, ScopeInternal, LevelHigh, "column type %d in binlog not supported")
	ErrBinlogGoMySQLTypeNotSupport   = New(codeBinlogGoMySQLTypeNotSupport, ClassFunctional, ScopeInternal, LevelHigh, "go-mysql type %d in event generator not supported")
	ErrBinlogColumnTypeMisMatch      = New(codeBinlogColumnTypeMisMatch, ClassFunctional, ScopeInternal, LevelHigh, "value %+v (type %v) with column type %v not valid")
	ErrBinlogDummyEvSizeTooSmall     = New(codeBinlogDummyEvSizeTooSmall, ClassFunctional, ScopeInternal, LevelHigh, "required dummy event size (%d) is too small, the minimum supported size is %d")
	ErrBinlogFlavorNotSupport        = New(codeBinlogFlavorNotSupport, ClassFunctional, ScopeInternal, LevelHigh, "flavor %s not supported")
	ErrBinlogDMLEmptyData            = New(codeBinlogDMLEmptyData, ClassFunctional, ScopeInternal, LevelHigh, "empty data not valid")
	ErrBinlogLatestGTIDNotInPrev     = New(codeBinlogLatestGTIDNotInPrev, ClassFunctional, ScopeInternal, LevelHigh, "latest GTID %s is not one of the latest previousGTIDs %s not valid")
	ErrBinlogReadFileByGTID          = New(codeBinlogReadFileByGTID, ClassFunctional, ScopeInternal, LevelHigh, "read from file by GTID not supported")
	ErrBinlogWriterNotStateNew       = New(codeBinlogWriterNotStateNew, ClassFunctional, ScopeInternal, LevelHigh, "stage %s, expect %s, already started")
	ErrBinlogWriterStateCannotClose  = New(codeBinlogWriterStateCannotClose, ClassFunctional, ScopeInternal, LevelHigh, "stage %s, expect %s, can not close")
	ErrBinlogWriterNeedStart         = New(codeBinlogWriterNeedStart, ClassFunctional, ScopeInternal, LevelHigh, "stage %s, expect %s, please start the writer first")
	ErrBinlogWriterOpenFile          = New(codeBinlogWriterOpenFile, ClassFunctional, ScopeInternal, LevelHigh, "open file")
	ErrBinlogWriterGetFileStat       = New(codeBinlogWriterGetFileStat, ClassFunctional, ScopeInternal, LevelHigh, "get stat for %s")
	ErrBinlogWriterWriteDataLen      = New(codeBinlogWriterWriteDataLen, ClassFunctional, ScopeInternal, LevelHigh, "data length %d")
	ErrBinlogWriterFileNotOpened     = New(codeBinlogWriterFileNotOpened, ClassFunctional, ScopeInternal, LevelHigh, "file %s not opened")
	ErrBinlogWriterFileSync          = New(codeBinlogWriterFileSync, ClassFunctional, ScopeInternal, LevelHigh, "sync file")
	ErrBinlogPrevGTIDEvNotValid      = New(codeBinlogPrevGTIDEvNotValid, ClassFunctional, ScopeInternal, LevelHigh, "PreviousGTIDsEvent should be a GenericEvent in go-mysql, but got %T")
	ErrBinlogDecodeMySQLGTIDSet      = New(codeBinlogDecodeMySQLGTIDSet, ClassFunctional, ScopeInternal, LevelHigh, "decode from % X")
	ErrBinlogNeedMariaDBGTIDSet      = New(codeBinlogNeedMariaDBGTIDSet, ClassFunctional, ScopeInternal, LevelHigh, "the event should be a MariadbGTIDListEvent, but got %T")
	ErrBinlogParseMariaDBGTIDSet     = New(codeBinlogParseMariaDBGTIDSet, ClassFunctional, ScopeInternal, LevelHigh, "parse MariaDB GTID set")
	ErrBinlogMariaDBAddGTIDSet       = New(codeBinlogMariaDBAddGTIDSet, ClassFunctional, ScopeInternal, LevelHigh, "add set %v to GTID set")
	// pkg/tracing
	ErrTracingEventDataNotValid = New(codeTracingEventDataNotValid, ClassFunctional, ScopeInternal, LevelHigh, "invalid event data for type: %s")
	ErrTracingUploadData        = New(codeTracingUploadData, ClassFunctional, ScopeInternal, LevelHigh, "upload event")
	ErrTracingEventTypeNotValid = New(codeTracingEventTypeNotValid, ClassFunctional, ScopeInternal, LevelHigh, "invalid event type %s, will not process")
	ErrTracingGetTraceCode      = New(codeTracingGetTraceCode, ClassFunctional, ScopeInternal, LevelHigh, "failed to get code information from runtime.Caller")
	ErrTracingDataChecksum      = New(codeTracingDataChecksum, ClassFunctional, ScopeInternal, LevelHigh, "calc data checksum")
	ErrTracingGetTSO            = New(codeTracingGetTSO, ClassFunctional, ScopeInternal, LevelHigh, "get tso")
	// pkg/backoff
	ErrBackoffArgsNotValid = New(codeBackoffArgsNotValid, ClassFunctional, ScopeInternal, LevelMedium, "backoff argument %s value %v not valid")
	// pkg
	ErrInitLoggerFail = New(codeInitLoggerFail, ClassFunctional, ScopeInternal, LevelMedium, "init logger failed")
	// pkg/gtid
	ErrGTIDTruncateInvalid = New(codeGTIDTruncateInvalid, ClassFunctional, ScopeInternal, LevelHigh, "truncate GTID sets %v to %v not valid")
	// pkg/streamer
	ErrRelayLogGivenPosTooBig = New(codeRelayLogGivenPosTooBig, ClassFunctional, ScopeInternal, LevelHigh, "the given relay log pos %s of meta config is too big, please check it again")
	// pkg/election
	ErrElectionCampaignFail    = New(codeElectionCampaignFail, ClassFunctional, ScopeInternal, LevelHigh, "fail to campaign leader: %s")
	ErrElectionGetLeaderIDFail = New(codeElectionGetLeaderIDFail, ClassFunctional, ScopeInternal, LevelMedium, "fail to get leader ID")

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
	ErrConfigSourceIDNotFound       = New(codeConfigSourceIDNotFound, ClassConfig, ScopeInternal, LevelMedium, "source %s in deployment configuration not found")
	ErrConfigDuplicateCfgItem       = New(codeConfigDuplicateCfgItem, ClassConfig, ScopeInternal, LevelMedium, "the following mysql configs have duplicate items, please remove the duplicates:\n%s")

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
	ErrTaskCheckGenTableRouter   = New(codeTaskCheckGenTableRouter, ClassTaskCheck, ScopeInternal, LevelMedium, "generate table router error")
	ErrTaskCheckGenColumnMapping = New(codeTaskCheckGenColumnMapping, ClassTaskCheck, ScopeInternal, LevelMedium, "generate column mapping error")
	ErrTaskCheckSyncConfigError  = New(codeTaskCheckSyncConfigError, ClassTaskCheck, ScopeInternal, LevelMedium, "%s %v: %v\n detail: %v")
	ErrTaskCheckGenBWList        = New(codeTaskCheckGenBWList, ClassTaskCheck, ScopeInternal, LevelMedium, "generate black white list error")

	// Relay log basic API error
	ErrRelayParseUUIDIndex         = New(codeRelayParseUUIDIndex, ClassRelayEventLib, ScopeInternal, LevelHigh, "parse server-uuid.index")
	ErrRelayParseUUIDSuffix        = New(codeRelayParseUUIDSuffix, ClassRelayEventLib, ScopeInternal, LevelHigh, "UUID (with suffix) %s not valid")
	ErrRelayUUIDWithSuffixNotFound = New(codeRelayUUIDWithSuffixNotFound, ClassRelayEventLib, ScopeInternal, LevelHigh, "no UUID (with suffix) matched %s found in %s, all UUIDs are %v")
	ErrRelayGenFakeRotateEvent     = New(codeRelayGenFakeRotateEvent, ClassRelayEventLib, ScopeInternal, LevelHigh, "generate fake rotate event")
	ErrRelayNoValidRelaySubDir     = New(codeRelayNoValidRelaySubDir, ClassRelayEventLib, ScopeInternal, LevelHigh, "no valid relay sub directory exists")

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
	ErrRelayTCPReaderGetEvent            = New(codeRelayTCPReaderGetEvent, ClassRelayUnit, ScopeUpstream, LevelHigh, "TCPReader get relay event with error")
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
	ErrPreviousGTIDsNotValid             = New(codePreviousGTIDsNotValid, ClassRelayUnit, ScopeInternal, LevelHigh, "previousGTIDs %s not valid")

	// Dump unit error
	ErrDumpUnitRuntime        = New(codeDumpUnitRuntime, ClassDumpUnit, ScopeInternal, LevelHigh, "mydumper runs with error")
	ErrDumpUnitGenTableRouter = New(codeDumpUnitGenTableRouter, ClassDumpUnit, ScopeInternal, LevelHigh, "generate table router")
	ErrDumpUnitGenBWList      = New(codeDumpUnitGenBWList, ClassDumpUnit, ScopeInternal, LevelHigh, "generate black white list")

	// Load unit error
	ErrLoadUnitCreateSchemaFile    = New(codeLoadUnitCreateSchemaFile, ClassLoadUnit, ScopeInternal, LevelMedium, "generate schema file")
	ErrLoadUnitInvalidFileEnding   = New(codeLoadUnitInvalidFileEnding, ClassLoadUnit, ScopeInternal, LevelHigh, "corresponding ending of sql: ')' not found")
	ErrLoadUnitParseQuoteValues    = New(codeLoadUnitParseQuoteValues, ClassLoadUnit, ScopeInternal, LevelHigh, "parse quote values error")
	ErrLoadUnitDoColumnMapping     = New(codeLoadUnitDoColumnMapping, ClassLoadUnit, ScopeInternal, LevelHigh, "mapping row data %v for table %+v")
	ErrLoadUnitReadSchemaFile      = New(codeLoadUnitReadSchemaFile, ClassLoadUnit, ScopeInternal, LevelHigh, "read schema from sql file %s")
	ErrLoadUnitParseStatement      = New(codeLoadUnitParseStatement, ClassLoadUnit, ScopeInternal, LevelHigh, "parse statement %s")
	ErrLoadUnitNotCreateTable      = New(codeLoadUnitNotCreateTable, ClassLoadUnit, ScopeInternal, LevelHigh, "statement %s for %s/%s is not create table statement")
	ErrLoadUnitDispatchSQLFromFile = New(codeLoadUnitDispatchSQLFromFile, ClassLoadUnit, ScopeInternal, LevelHigh, "dispatch sql")
	ErrLoadUnitInvalidInsertSQL    = New(codeLoadUnitInvalidInsertSQL, ClassLoadUnit, ScopeInternal, LevelHigh, "invalid insert sql %s")
	ErrLoadUnitGenTableRouter      = New(codeLoadUnitGenTableRouter, ClassLoadUnit, ScopeInternal, LevelHigh, "generate table router")
	ErrLoadUnitGenColumnMapping    = New(codeLoadUnitGenColumnMapping, ClassLoadUnit, ScopeInternal, LevelHigh, "generate column mapping")
	ErrLoadUnitNoDBFile            = New(codeLoadUnitNoDBFile, ClassLoadUnit, ScopeInternal, LevelHigh, "invalid data sql file, cannot find db - %s")
	ErrLoadUnitNoTableFile         = New(codeLoadUnitNoTableFile, ClassLoadUnit, ScopeInternal, LevelHigh, "invalid data sql file, cannot find table - %s")
	ErrLoadUnitDumpDirNotFound     = New(codeLoadUnitDumpDirNotFound, ClassLoadUnit, ScopeInternal, LevelHigh, "%s does not exist or it's not a dir")
	ErrLoadUnitDuplicateTableFile  = New(codeLoadUnitDuplicateTableFile, ClassLoadUnit, ScopeInternal, LevelHigh, "invalid table schema file, duplicated item - %s")
	ErrLoadUnitGenBWList           = New(codeLoadUnitGenBWList, ClassLoadUnit, ScopeInternal, LevelHigh, "generate black white list")

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
	ErrSyncerUnitInvalidReplicaEvent        = New(codeSyncerUnitInvalidReplicaEvent, ClassSyncUnit, ScopeInternal, LevelHigh, "invalid replication event type %v")
	ErrSyncerUnitParseStmt                  = New(codeSyncerUnitParseStmt, ClassSyncUnit, ScopeInternal, LevelHigh, "")
	ErrSyncerUnitUUIDNotLatest              = New(codeSyncerUnitUUIDNotLatest, ClassSyncUnit, ScopeInternal, LevelHigh, "UUID %s not the latest one in UUIDs %v")
	ErrSyncerUnitDDLExecChanCloseOrBusy     = New(codeSyncerUnitDDLExecChanCloseOrBusy, ClassSyncUnit, ScopeInternal, LevelHigh, "the chan has closed or already in sending")
	ErrSyncerUnitDDLChanDone                = New(codeSyncerUnitDDLChanDone, ClassSyncUnit, ScopeInternal, LevelHigh, "canceled from external")
	ErrSyncerUnitDDLChanCanceled            = New(codeSyncerUnitDDLChanCanceled, ClassSyncUnit, ScopeInternal, LevelHigh, "canceled by Close or Renew")
	ErrSyncerUnitDDLOnMultipleTable         = New(codeSyncerUnitDDLOnMultipleTable, ClassSyncUnit, ScopeInternal, LevelHigh, "ddl on multiple table: %s not supported")
	ErrSyncerUnitInjectDDLOnly              = New(codeSyncerUnitInjectDDLOnly, ClassSyncUnit, ScopeInternal, LevelLow, "only support inject DDL for sharding group to be synced currently, but got %s")
	ErrSyncerUnitInjectDDLWithoutSchema     = New(codeSyncerUnitInjectDDLWithoutSchema, ClassSyncUnit, ScopeInternal, LevelLow, "injected DDL %s without schema name not valid")
	ErrSyncerUnitNotSupportedOperate        = New(codeSyncerUnitNotSupportedOperate, ClassSyncUnit, ScopeInternal, LevelMedium, "op %s not supported")
	ErrSyncerUnitNilOperatorReq             = New(codeSyncerUnitNilOperatorReq, ClassSyncUnit, ScopeInternal, LevelMedium, "nil request not valid")
	ErrSyncerUnitDMLColumnNotMatch          = New(codeSyncerUnitDMLColumnNotMatch, ClassSyncUnit, ScopeInternal, LevelHigh, "Column count doesn't match value count: %d (columns) vs %d (values)")
	ErrSyncerUnitDMLOldNewValueMismatch     = New(codeSyncerUnitDMLOldNewValueMismatch, ClassSyncUnit, ScopeInternal, LevelHigh, "Old value count doesn't match new value count: %d (old) vs %d (new)")
	ErrSyncerUnitDMLPruneColumnMismatch     = New(codeSyncerUnitDMLPruneColumnMismatch, ClassSyncUnit, ScopeInternal, LevelHigh, "prune DML columns and data mismatch in length: %d (columns) %d (data)")
	ErrSyncerUnitGenBinlogEventFilter       = New(codeSyncerUnitGenBinlogEventFilter, ClassSyncUnit, ScopeInternal, LevelHigh, "generate binlog event filter")
	ErrSyncerUnitGenTableRouter             = New(codeSyncerUnitGenTableRouter, ClassSyncUnit, ScopeInternal, LevelHigh, "generate table router")
	ErrSyncerUnitGenColumnMapping           = New(codeSyncerUnitGenColumnMapping, ClassSyncUnit, ScopeInternal, LevelHigh, "generate column mapping")
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
	ErrSyncerUnitGenBWList                  = New(codeSyncerUnitGenBWList, ClassSyncUnit, ScopeInternal, LevelHigh, "generate black white list")

	// DM-master error
	ErrMasterSQLOpNilRequest        = New(codeMasterSQLOpNilRequest, ClassDMMaster, ScopeInternal, LevelMedium, "nil request not valid")
	ErrMasterSQLOpNotSupport        = New(codeMasterSQLOpNotSupport, ClassDMMaster, ScopeInternal, LevelMedium, "op %s not supported")
	ErrMasterSQLOpWithoutSharding   = New(codeMasterSQLOpWithoutSharding, ClassDMMaster, ScopeInternal, LevelMedium, "operate request without --sharding specified not valid")
	ErrMasterGRPCCreateConn         = New(codeMasterGRPCCreateConn, ClassDMMaster, ScopeInternal, LevelHigh, "create grpc connection")
	ErrMasterGRPCSendOnCloseConn    = New(codeMasterGRPCSendOnCloseConn, ClassDMMaster, ScopeInternal, LevelHigh, "send request on a closed client")
	ErrMasterGRPCClientClose        = New(codeMasterGRPCClientClose, ClassDMMaster, ScopeInternal, LevelHigh, "close rpc client")
	ErrMasterGRPCInvalidReqType     = New(codeMasterGRPCInvalidReqType, ClassDMMaster, ScopeInternal, LevelHigh, "invalid request type: %v")
	ErrMasterGRPCRequestError       = New(codeMasterGRPCRequestError, ClassDMMaster, ScopeInternal, LevelHigh, "grpc request error")
	ErrMasterDeployMapperVerify     = New(codeMasterDeployMapperVerify, ClassDMMaster, ScopeInternal, LevelHigh, "user should specify valid relation between source(mysql/mariadb) and dm-worker, config %+v not valid")
	ErrMasterConfigParseFlagSet     = New(codeMasterConfigParseFlagSet, ClassDMMaster, ScopeInternal, LevelMedium, "parse config flag set")
	ErrMasterConfigUnknownItem      = New(codeMasterConfigUnknownItem, ClassDMMaster, ScopeInternal, LevelMedium, "master config contained unknown configuration options: %s")
	ErrMasterConfigInvalidFlag      = New(codeMasterConfigInvalidFlag, ClassDMMaster, ScopeInternal, LevelMedium, "'%s' is an invalid flag")
	ErrMasterConfigTomlTransform    = New(codeMasterConfigTomlTransform, ClassDMMaster, ScopeInternal, LevelMedium, "config toml transform")
	ErrMasterConfigTimeoutParse     = New(codeMasterConfigTimeoutParse, ClassDMMaster, ScopeInternal, LevelMedium, "parse rpc timeout str")
	ErrMasterConfigUpdateCfgFile    = New(codeMasterConfigUpdateCfgFile, ClassDMMaster, ScopeInternal, LevelHigh, "update config file")
	ErrMasterShardingDDLDiff        = New(codeMasterShardingDDLDiff, ClassDMMaster, ScopeInternal, LevelHigh, "sharding ddls in ddl lock %s is different with %s")
	ErrMasterStartService           = New(codeMasterStartService, ClassDMMaster, ScopeInternal, LevelHigh, "start server")
	ErrMasterNoEmitToken            = New(codeMasterNoEmitToken, ClassDMMaster, ScopeInternal, LevelHigh, "fail to get emit opportunity for worker %s")
	ErrMasterLockNotFound           = New(codeMasterLockNotFound, ClassDMMaster, ScopeInternal, LevelHigh, "lock with ID %s not found")
	ErrMasterLockIsResolving        = New(codeMasterLockIsResolving, ClassDMMaster, ScopeInternal, LevelHigh, "lock %s is resolving")
	ErrMasterWorkerCliNotFound      = New(codeMasterWorkerCliNotFound, ClassDMMaster, ScopeInternal, LevelHigh, "worker %s relevant worker-client not found")
	ErrMasterWorkerNotWaitLock      = New(codeMasterWorkerNotWaitLock, ClassDMMaster, ScopeInternal, LevelHigh, "worker %s not waiting for DDL lock %s")
	ErrMasterHandleSQLReqFail       = New(codeMasterHandleSQLReqFail, ClassDMMaster, ScopeInternal, LevelHigh, "request DDL lock %s owner %s handle SQLs request %s fail %s")
	ErrMasterOwnerExecDDL           = New(codeMasterOwnerExecDDL, ClassDMMaster, ScopeInternal, LevelHigh, "owner %s ExecuteDDL fail")
	ErrMasterPartWorkerExecDDLFail  = New(codeMasterPartWorkerExecDDLFail, ClassDMMaster, ScopeInternal, LevelHigh, "DDL lock %s owner ExecuteDDL successfully, so DDL lock removed. but some dm-workers ExecuteDDL fail, you should to handle dm-worker directly")
	ErrMasterWorkerExistDDLLock     = New(codeMasterWorkerExistDDLLock, ClassDMMaster, ScopeInternal, LevelHigh, "worker %s exist ddl lock, please unlock ddl lock first")
	ErrMasterGetWorkerCfgExtractor  = New(codeMasterGetWorkerCfgExtractor, ClassDMMaster, ScopeInternal, LevelHigh, "")
	ErrMasterTaskConfigExtractor    = New(codeMasterTaskConfigExtractor, ClassDMMaster, ScopeInternal, LevelHigh, "")
	ErrMasterWorkerArgsExtractor    = New(codeMasterWorkerArgsExtractor, ClassDMMaster, ScopeInternal, LevelHigh, "")
	ErrMasterQueryWorkerConfig      = New(codeMasterQueryWorkerConfig, ClassDMMaster, ScopeInternal, LevelHigh, "")
	ErrMasterOperNotFound           = New(codeMasterOperNotFound, ClassDMMaster, ScopeInternal, LevelHigh, "operation %d of task %s on worker %s not found, please execute `query-status` to check status")
	ErrMasterOperRespNotSuccess     = New(codeMasterOperRespNotSuccess, ClassDMMaster, ScopeInternal, LevelHigh, "operation %d of task %s on worker %s not success: %s")
	ErrMasterOperRequestTimeout     = New(codeMasterOperRequestTimeout, ClassDMMaster, ScopeInternal, LevelHigh, "request to dm-worker %s is timeout, but request may be successful, please execute `query-status` to check status")
	ErrMasterHandleHTTPApis         = New(codeMasterHandleHTTPApis, ClassDMMaster, ScopeInternal, LevelHigh, "serve http apis to grpc")
	ErrMasterHostPortNotValid       = New(codeMasterHostPortNotValid, ClassDMMaster, ScopeInternal, LevelHigh, "host:port '%s' not valid")
	ErrMasterGetHostnameFail        = New(codeMasterGetHostnameFail, ClassDMMaster, ScopeInternal, LevelHigh, "get hostname fail")
	ErrMasterGenEmbedEtcdConfigFail = New(codeMasterGenEmbedEtcdConfigFail, ClassDMMaster, ScopeInternal, LevelHigh, "fail to generate config item %s for embed etcd")
	ErrMasterStartEmbedEtcdFail     = New(codeMasterStartEmbedEtcdFail, ClassDMMaster, ScopeInternal, LevelHigh, "fail to start embed etcd")
	ErrMasterParseURLFail           = New(codeMasterParseURLFail, ClassDMMaster, ScopeInternal, LevelHigh, "fail to parse URL %s")
	ErrMasterJoinEmbedEtcdFail      = New(codeMasterJoinEmbedEtcdFail, ClassDMMaster, ScopeInternal, LevelHigh, "fail to join embed etcd: %s")

	// DM-worker error
	ErrWorkerParseFlagSet            = New(codeWorkerParseFlagSet, ClassDMWorker, ScopeInternal, LevelMedium, "parse dm-worker config flag set")
	ErrWorkerInvalidFlag             = New(codeWorkerInvalidFlag, ClassDMWorker, ScopeInternal, LevelMedium, "'%s' is an invalid flag")
	ErrWorkerDecodeConfigFromFile    = New(codeWorkerDecodeConfigFromFile, ClassDMWorker, ScopeInternal, LevelMedium, "toml decode file")
	ErrWorkerUndecodedItemFromFile   = New(codeWorkerUndecodedItemFromFile, ClassDMWorker, ScopeInternal, LevelMedium, "worker config contains unknown configuration options: %s")
	ErrWorkerNeedSourceID            = New(codeWorkerNeedSourceID, ClassDMWorker, ScopeInternal, LevelMedium, "dm-worker should bind a non-empty source ID which represents a MySQL/MariaDB instance or a replica group. \n notice: if you use old version dm-ansible, please update to newest version.")
	ErrWorkerTooLongSourceID         = New(codeWorkerTooLongSourceID, ClassDMWorker, ScopeInternal, LevelMedium, "the length of source ID %s is more than max allowed value %d")
	ErrWorkerRelayBinlogName         = New(codeWorkerRelayBinlogName, ClassDMWorker, ScopeInternal, LevelMedium, "relay-binlog-name %s not valid")
	ErrWorkerWriteConfigFile         = New(codeWorkerWriteConfigFile, ClassDMWorker, ScopeInternal, LevelMedium, "write config to local file")
	ErrWorkerLogInvalidHandler       = New(codeWorkerLogInvalidHandler, ClassDMWorker, ScopeInternal, LevelHigh, "handler is nil, please pass a leveldb.DB or leveldb.Transaction")
	ErrWorkerLogPointerInvalid       = New(codeWorkerLogPointerInvalid, ClassDMWorker, ScopeInternal, LevelHigh, "not valid length data as pointer % X")
	ErrWorkerLogFetchPointer         = New(codeWorkerLogFetchPointer, ClassDMWorker, ScopeInternal, LevelHigh, "fetch handled pointer")
	ErrWorkerLogUnmarshalPointer     = New(codeWorkerLogUnmarshalPointer, ClassDMWorker, ScopeInternal, LevelHigh, "unmarshal handle pointer % X")
	ErrWorkerLogClearPointer         = New(codeWorkerLogClearPointer, ClassDMWorker, ScopeInternal, LevelHigh, "clear handled pointer")
	ErrWorkerLogTaskKeyNotValid      = New(codeWorkerLogTaskKeyNotValid, ClassDMWorker, ScopeInternal, LevelHigh, "not valid length data as task log key % X")
	ErrWorkerLogUnmarshalTaskKey     = New(codeWorkerLogUnmarshalTaskKey, ClassDMWorker, ScopeInternal, LevelHigh, "unmarshal task log % X")
	ErrWorkerLogFetchLogIter         = New(codeWorkerLogFetchLogIter, ClassDMWorker, ScopeInternal, LevelHigh, "fetch logs from meta with handle pointer %+v")
	ErrWorkerLogGetTaskLog           = New(codeWorkerLogGetTaskLog, ClassDMWorker, ScopeInternal, LevelHigh, "get task log %d from leveldb")
	ErrWorkerLogUnmarshalBinary      = New(codeWorkerLogUnmarshalBinary, ClassDMWorker, ScopeInternal, LevelHigh, "unmarshal task log binary % X")
	ErrWorkerLogForwardPointer       = New(codeWorkerLogForwardPointer, ClassDMWorker, ScopeInternal, LevelHigh, "forward handled pointer to %d")
	ErrWorkerLogMarshalTask          = New(codeWorkerLogMarshalTask, ClassDMWorker, ScopeInternal, LevelHigh, "marshal task log %+v")
	ErrWorkerLogSaveTask             = New(codeWorkerLogSaveTask, ClassDMWorker, ScopeInternal, LevelHigh, "save task log %+v")
	ErrWorkerLogDeleteKV             = New(codeWorkerLogDeleteKV, ClassDMWorker, ScopeInternal, LevelHigh, "delete kv with prefix % X until % X")
	ErrWorkerLogDeleteKVIter         = New(codeWorkerLogDeleteKVIter, ClassDMWorker, ScopeInternal, LevelHigh, "iterate kv with prefix % X")
	ErrWorkerLogUnmarshalTaskMeta    = New(codeWorkerLogUnmarshalTaskMeta, ClassDMWorker, ScopeInternal, LevelHigh, "unmarshal task meta % X")
	ErrWorkerLogFetchTaskFromMeta    = New(codeWorkerLogFetchTaskFromMeta, ClassDMWorker, ScopeInternal, LevelHigh, "fetch tasks from meta with prefix % X")
	ErrWorkerLogVerifyTaskMeta       = New(codeWorkerLogVerifyTaskMeta, ClassDMWorker, ScopeInternal, LevelHigh, "")
	ErrWorkerLogSaveTaskMeta         = New(codeWorkerLogSaveTaskMeta, ClassDMWorker, ScopeInternal, LevelHigh, "save task meta %s into kv db")
	ErrWorkerLogGetTaskMeta          = New(codeWorkerLogGetTaskMeta, ClassDMWorker, ScopeInternal, LevelHigh, "get task meta %s from kv db")
	ErrWorkerLogDeleteTaskMeta       = New(codeWorkerLogDeleteTaskMeta, ClassDMWorker, ScopeInternal, LevelHigh, "delete task meta %s from kv db")
	ErrWorkerMetaTomlTransform       = New(codeWorkerMetaTomlTransform, ClassDMWorker, ScopeInternal, LevelHigh, "meta toml transform")
	ErrWorkerMetaOldFileStat         = New(codeWorkerMetaOldFileStat, ClassDMWorker, ScopeInternal, LevelHigh, "get old file stat")
	ErrWorkerMetaOldReadFile         = New(codeWorkerMetaOldReadFile, ClassDMWorker, ScopeInternal, LevelHigh, "read old metadata file %s")
	ErrWorkerMetaEncodeTask          = New(codeWorkerMetaEncodeTask, ClassDMWorker, ScopeInternal, LevelHigh, "encode task %v")
	ErrWorkerMetaRemoveOldDir        = New(codeWorkerMetaRemoveOldDir, ClassDMWorker, ScopeInternal, LevelHigh, "remove old meta dir")
	ErrWorkerMetaTaskLogNotFound     = New(codeWorkerMetaTaskLogNotFound, ClassDMWorker, ScopeInternal, LevelHigh, "any task operation log not found")
	ErrWorkerMetaHandleTaskOrder     = New(codeWorkerMetaHandleTaskOrder, ClassDMWorker, ScopeInternal, LevelHigh, "please handle task operation order by log ID, the log need to be handled is %+v, not %+v")
	ErrWorkerMetaOpenTxn             = New(codeWorkerMetaOpenTxn, ClassDMWorker, ScopeInternal, LevelHigh, "open kv db txn")
	ErrWorkerMetaCommitTxn           = New(codeWorkerMetaCommitTxn, ClassDMWorker, ScopeInternal, LevelHigh, "commit kv db txn")
	ErrWorkerRelayStageNotValid      = New(codeWorkerRelayStageNotValid, ClassDMWorker, ScopeInternal, LevelHigh, "current stage is %s, %s required, relay op %s")
	ErrWorkerRelayOperNotSupport     = New(codeWorkerRelayOperNotSupport, ClassDMWorker, ScopeInternal, LevelHigh, "operation %s not supported")
	ErrWorkerOpenKVDBFile            = New(codeWorkerOpenKVDBFile, ClassDMWorker, ScopeInternal, LevelHigh, "open kv db file")
	ErrWorkerUpgradeCheckKVDir       = New(codeWorkerUpgradeCheckKVDir, ClassDMWorker, ScopeInternal, LevelHigh, "")
	ErrWorkerMarshalVerBinary        = New(codeWorkerMarshalVerBinary, ClassDMWorker, ScopeInternal, LevelHigh, "marshal version %s to binary data")
	ErrWorkerUnmarshalVerBinary      = New(codeWorkerUnmarshalVerBinary, ClassDMWorker, ScopeInternal, LevelHigh, "unmarshal version from data % X")
	ErrWorkerGetVersionFromKV        = New(codeWorkerGetVersionFromKV, ClassDMWorker, ScopeInternal, LevelHigh, "load version with key %v from levelDB")
	ErrWorkerSaveVersionToKV         = New(codeWorkerSaveVersionToKV, ClassDMWorker, ScopeInternal, LevelHigh, "save version %v into levelDB with key %v")
	ErrWorkerVerAutoDowngrade        = New(codeWorkerVerAutoDowngrade, ClassDMWorker, ScopeInternal, LevelHigh, "the previous version %s is newer than current %s, automatic downgrade is not supported now, please handle it manually")
	ErrWorkerStartService            = New(codeWorkerStartService, ClassDMWorker, ScopeInternal, LevelHigh, "start server")
	ErrWorkerNoStart                 = New(codeWorkerNoStart, ClassDMWorker, ScopeInternal, LevelHigh, "worker has not started")
	ErrWorkerAlreadyClosed           = New(codeWorkerAlreadyClosed, ClassDMWorker, ScopeInternal, LevelHigh, "worker already closed")
	ErrWorkerAlreadyStart            = New(codeWorkerAlreadyStarted, ClassDMWorker, ScopeInternal, LevelHigh, "worker already started")
	ErrWorkerNotRunningStage         = New(codeWorkerNotRunningStage, ClassDMWorker, ScopeInternal, LevelHigh, "current stage is not running not valid")
	ErrWorkerNotPausedStage          = New(codeWorkerNotPausedStage, ClassDMWorker, ScopeInternal, LevelHigh, "current stage is not paused not valid")
	ErrWorkerUpdateTaskStage         = New(codeWorkerUpdateTaskStage, ClassDMWorker, ScopeInternal, LevelHigh, "can only update task on Paused stage, but current stage is %s")
	ErrWorkerMigrateStopRelay        = New(codeWorkerMigrateStopRelay, ClassDMWorker, ScopeInternal, LevelHigh, "relay unit has stopped, can not be migrated")
	ErrWorkerSubTaskNotFound         = New(codeWorkerSubTaskNotFound, ClassDMWorker, ScopeInternal, LevelHigh, "sub task with name %s not found")
	ErrWorkerSubTaskExists           = New(codeWorkerSubTaskExists, ClassDMWorker, ScopeInternal, LevelHigh, "sub task %s already exists")
	ErrWorkerOperSyncUnitOnly        = New(codeWorkerOperSyncUnitOnly, ClassDMWorker, ScopeInternal, LevelHigh, "such operation is only available for syncer, but now syncer is not running. current unit is %s")
	ErrWorkerRelayUnitStage          = New(codeWorkerRelayUnitStage, ClassDMWorker, ScopeInternal, LevelHigh, "Worker's relay log unit in invalid stage: %s")
	ErrWorkerNoSyncerRunning         = New(codeWorkerNoSyncerRunning, ClassDMWorker, ScopeInternal, LevelHigh, "there is a subtask does not run syncer")
	ErrWorkerCannotUpdateSourceID    = New(codeWorkerCannotUpdateSourceID, ClassDMWorker, ScopeInternal, LevelHigh, "update source ID is not allowed")
	ErrWorkerNoAvailUnits            = New(codeWorkerNoAvailUnits, ClassDMWorker, ScopeInternal, LevelHigh, "subtask %s has no dm units for mode %s")
	ErrWorkerDDLLockInfoNotFound     = New(codeWorkerDDLLockInfoNotFound, ClassDMWorker, ScopeInternal, LevelHigh, "DDLLockInfo with ID %s not found")
	ErrWorkerDDLLockInfoExists       = New(codeWorkerDDLLockInfoExists, ClassDMWorker, ScopeInternal, LevelHigh, "DDLLockInfo for task %s already exists")
	ErrWorkerCacheDDLInfoExists      = New(codeWorkerCacheDDLInfoExists, ClassDMWorker, ScopeInternal, LevelHigh, "CacheDDLInfo for task %s already exists")
	ErrWorkerExecSkipDDLConflict     = New(codeWorkerExecSkipDDLConflict, ClassDMWorker, ScopeInternal, LevelHigh, "execDDL and skipDDL can not specify both at the same time")
	ErrWorkerExecDDLSyncerOnly       = New(codeWorkerExecDDLSyncerOnly, ClassDMWorker, ScopeInternal, LevelHigh, "only syncer support ExecuteDDL, but current unit is %s")
	ErrWorkerExecDDLTimeout          = New(codeWorkerExecDDLTimeout, ClassDMWorker, ScopeInternal, LevelHigh, "ExecuteDDL timeout (exceeding %s), try use `query-status` to query whether the DDL is still blocking")
	ErrWorkerWaitRelayCatchupTimeout = New(codeWorkerWaitRelayCatchupTimeout, ClassDMWorker, ScopeInternal, LevelHigh, "waiting for relay binlog pos to catch up with loader end binlog pos is timeout (exceeding %s), loader end binlog pos: %s, relay binlog pos: %s")
	ErrWorkerRelayIsPurging          = New(codeWorkerRelayIsPurging, ClassDMWorker, ScopeInternal, LevelHigh, "relay log purger is purging, cannot start sub task %s, please try again later")
	ErrWorkerHostPortNotValid        = New(codeWorkerHostPortNotValid, ClassDMWorker, ScopeInternal, LevelHigh, "host:port '%s' not valid")
	ErrWorkerSourceNotMatch          = New(codeWorkerSourceNotMatch, ClassDMWorker, ScopeInternal, LevelHigh, "source of request does not match with source in worker")

	// DM-tracer error
	ErrTracerParseFlagSet        = New(codeTracerParseFlagSet, ClassDMTracer, ScopeInternal, LevelMedium, "parse dm-tracer config flag set")
	ErrTracerConfigTomlTransform = New(codeTracerConfigTomlTransform, ClassDMTracer, ScopeInternal, LevelMedium, "config toml transform")
	ErrTracerConfigInvalidFlag   = New(codeTracerConfigInvalidFlag, ClassDMTracer, ScopeInternal, LevelMedium, "'%s' is an invalid flag")
	ErrTracerTraceEventNotFound  = New(codeTracerTraceEventNotFound, ClassDMTracer, ScopeInternal, LevelMedium, "trace event %s not found")
	ErrTracerTraceIDNotProvided  = New(codeTracerTraceIDNotProvided, ClassDMTracer, ScopeInternal, LevelMedium, "trace id not provided")
	ErrTracerParamNotValid       = New(codeTracerParamNotValid, ClassDMTracer, ScopeInternal, LevelMedium, "param %s value %s not valid")
	ErrTracerPostMethodOnly      = New(codeTracerPostMethodOnly, ClassDMTracer, ScopeInternal, LevelMedium, "post method only")
	ErrTracerEventAssertionFail  = New(codeTracerEventAssertionFail, ClassDMTracer, ScopeInternal, LevelHigh, "type %s event: %v not valid")
	ErrTracerEventTypeNotValid   = New(codeTracerEventTypeNotValid, ClassDMTracer, ScopeInternal, LevelHigh, "trace event type %d not valid")
	ErrTracerStartService        = New(codeTracerStartService, ClassDMTracer, ScopeInternal, LevelHigh, "start server")
)
