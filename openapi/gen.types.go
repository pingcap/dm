// Package openapi provides primitives to interact with the openapi HTTP API.
//
// Code generated by github.com/deepmap/oapi-codegen version v1.8.2 DO NOT EDIT.
package openapi

// Defines values for TaskOnDuplication.
const (
	TaskOnDuplicationError TaskOnDuplication = "error"

	TaskOnDuplicationOverwrite TaskOnDuplication = "overwrite"
)

// Defines values for TaskShardMode.
const (
	TaskShardModeOptimistic TaskShardMode = "optimistic"

	TaskShardModePessimistic TaskShardMode = "pessimistic"
)

// Defines values for TaskTaskMode.
const (
	TaskTaskModeAll TaskTaskMode = "all"

	TaskTaskModeFull TaskTaskMode = "full"

	TaskTaskModeIncremental TaskTaskMode = "incremental"
)

// CreateTaskRequest defines model for CreateTaskRequest.
type CreateTaskRequest struct {
	// whether to remove meta database in downstream database
	RemoveMeta bool `json:"remove_meta"`

	// task
	Task Task `json:"task"`
}

// operation error
type ErrorWithMessage struct {
	// error code
	ErrorCode int `json:"error_code"`

	// error message
	ErrorMsg string `json:"error_msg"`
}

// GetSourceListResponse defines model for GetSourceListResponse.
type GetSourceListResponse struct {
	Data  []Source `json:"data"`
	Total int      `json:"total"`
}

// GetSourceStatusResponse defines model for GetSourceStatusResponse.
type GetSourceStatusResponse struct {
	Data  []SourceStatus `json:"data"`
	Total int            `json:"total"`
}

// GetTaskListResponse defines model for GetTaskListResponse.
type GetTaskListResponse struct {
	Data  []Task `json:"data"`
	Total int    `json:"total"`
}

// GetTaskStatusResponse defines model for GetTaskStatusResponse.
type GetTaskStatusResponse struct {
	Data  []SubTaskStatus `json:"data"`
	Total int             `json:"total"`
}

// status of load unit
type LoadStatus struct {
	FinishedBytes  int64  `json:"finished_bytes"`
	MetaBinlog     string `json:"meta_binlog"`
	MetaBinlogGtid string `json:"meta_binlog_gtid"`
	Progress       string `json:"progress"`
	TotalBytes     int64  `json:"total_bytes"`
}

// relay log cleanup policy configuration
type Purge struct {
	// expiration time of relay log
	Expires *int64 `json:"expires"`

	// The interval to periodically check if the relay log is expired, default value: 3600, in seconds
	Interval *int64 `json:"interval"`

	// Minimum free disk space, in GB
	RemainSpace *int64 `json:"remain_space"`
}

// status of relay log
type RelayStatus struct {
	// upstream binlog file information
	MasterBinlog string `json:"master_binlog"`

	// GTID of the upstream
	MasterBinlogGtid string `json:"master_binlog_gtid"`

	// relay current GTID
	RelayBinlogGtid string `json:"relay_binlog_gtid"`

	// whether to catch up with upstream progress
	RelayCatchUpMaster bool `json:"relay_catch_up_master"`

	// the directory where the relay log is stored
	RelayDir string `json:"relay_dir"`

	// current status
	Stage string `json:"stage"`
}

// data source ssl configuration, the field will be hidden when getting the data source configuration from the interface
type Security struct {
	// Common Name of SSL certificates
	CertAllowedCn *[]string `json:"cert_allowed_cn,omitempty"`

	// certificate file content
	SslCaContent string `json:"ssl_ca_content"`

	// File content of PEM format/X509 format certificates
	SslCertContent string `json:"ssl_cert_content"`

	// Content of the private key file in X509 format
	SslKeyContent string `json:"ssl_key_content"`
}

// ShardingGroup defines model for ShardingGroup.
type ShardingGroup struct {
	DdlList       []string `json:"ddl_list"`
	FirstLocation string   `json:"first_location"`
	Synced        []string `json:"synced"`
	Target        string   `json:"target"`
	Unsynced      []string `json:"unsynced"`
}

// source
type Source struct {
	// whether to use GTID to pull binlogs from upstream
	EnableGtid bool `json:"enable_gtid"`

	// source address
	Host string `json:"host"`

	// source password
	Password string `json:"password"`

	// source port
	Port int `json:"port"`

	// relay log cleanup policy configuration
	Purge *Purge `json:"purge,omitempty"`

	// data source ssl configuration, the field will be hidden when getting the data source configuration from the interface
	Security *Security `json:"security"`

	// source name
	SourceName string `json:"source_name"`

	// source username
	User string `json:"user"`
}

// source status
type SourceStatus struct {
	// status of relay log
	RelayStatus *RelayStatus `json:"relay_status,omitempty"`

	// source name
	SourceName string `json:"source_name"`

	// The worker currently bound to the source
	WorkerName string `json:"worker_name"`
}

// action to open a relay request
type StartRelayRequest struct {
	// starting GTID of the upstream binlog
	RelayBinlogGtid *string `json:"relay_binlog_gtid"`

	// starting filename of the upstream binlog
	RelayBinlogName *string `json:"relay_binlog_name"`

	// the directory where the relay log is stored
	RelayDir *string `json:"relay_dir"`

	// worker name list
	WorkerNameList WorkerNameList `json:"worker_name_list"`
}

// action to open a relay request
type StopRelayRequest struct {
	// worker name list
	WorkerNameList WorkerNameList `json:"worker_name_list"`
}

// SubTaskStatus defines model for SubTaskStatus.
type SubTaskStatus struct {
	// status of load unit
	LoadStatus *LoadStatus `json:"load_status,omitempty"`

	// task name
	Name string `json:"name"`

	// source name
	SourceName string `json:"source_name"`

	// current stage of the task
	Stage string `json:"stage"`

	// status of sync uuit
	SyncStatus *SyncStatus `json:"sync_status,omitempty"`

	// task unit type
	Unit                string  `json:"unit"`
	UnresolvedDdlLockId *string `json:"unresolved_ddl_lock_id,omitempty"`

	// worker name
	WorkerName string `json:"worker_name"`
}

// status of sync uuit
type SyncStatus struct {
	BinlogType string `json:"binlog_type"`

	// sharding DDL which current is blocking
	BlockingDdls        []string `json:"blocking_ddls"`
	MasterBinlog        string   `json:"master_binlog"`
	MasterBinlogGtid    string   `json:"master_binlog_gtid"`
	RecentTps           int64    `json:"recent_tps"`
	SecondsBehindMaster int64    `json:"seconds_behind_master"`
	Synced              bool     `json:"synced"`
	SyncerBinlog        string   `json:"syncer_binlog"`
	SyncerBinlogGtid    string   `json:"syncer_binlog_gtid"`
	TotalEvents         int64    `json:"total_events"`
	TotalTps            int64    `json:"total_tps"`

	// sharding groups which current are un-resolved
	UnresolvedGroups []ShardingGroup `json:"unresolved_groups"`
}

// task
type Task struct {
	// whether to enable support for the online ddl plugin
	EnhanceOnlineSchemaChange bool `json:"enhance_online_schema_change"`

	// binlog event filter rule set
	EventFilterRule *[]TaskEventFilterRule `json:"event_filter_rule,omitempty"`

	// downstream database for storing meta information
	MetaSchema *string `json:"meta_schema,omitempty"`

	// task name
	Name string `json:"name"`

	// how to handle conflicted data
	OnDuplication TaskOnDuplication `json:"on_duplication"`

	// the way to coordinate DDL
	ShardMode *TaskShardMode `json:"shard_mode,omitempty"`

	// source-related configuration
	SourceConfig TaskSourceConfig `json:"source_config"`

	// table migrate rule
	TableMigrateRule []TaskTableMigrateRule `json:"table_migrate_rule"`

	// downstream database configuration
	TargetConfig TaskTargetDataBase `json:"target_config"`

	// migrate mode
	TaskMode TaskTaskMode `json:"task_mode"`
}

// how to handle conflicted data
type TaskOnDuplication string

// the way to coordinate DDL
type TaskShardMode string

// migrate mode
type TaskTaskMode string

// Filtering rules at binlog level
type TaskEventFilterRule struct {
	// event type
	IgnoreEvent *[]string `json:"ignore_event,omitempty"`

	// sql pattern to filter
	IgnoreSql *[]string `json:"ignore_sql,omitempty"`

	// rule name
	RuleName string `json:"rule_name"`
}

// configuration of full migrate tasks
type TaskFullMigrateConf struct {
	// storage dir name
	DataDir *string `json:"data_dir,omitempty"`

	// full export of concurrent
	ExportThreads *int `json:"export_threads,omitempty"`

	// full import of concurrent
	ImportThreads *int `json:"import_threads,omitempty"`
}

// configuration of incremental tasks
type TaskIncrMigrateConf struct {
	// incremental synchronization of batch execution sql quantities
	ReplBatch *int `json:"repl_batch,omitempty"`

	// incremental task of concurrent
	ReplThreads *int `json:"repl_threads,omitempty"`
}

// TaskSourceConf defines model for TaskSourceConf.
type TaskSourceConf struct {
	BinlogGtid *string `json:"binlog_gtid,omitempty"`
	BinlogName *string `json:"binlog_name,omitempty"`
	BinlogPos  *int    `json:"binlog_pos,omitempty"`

	// source name
	SourceName string `json:"source_name"`
}

// source-related configuration
type TaskSourceConfig struct {
	// configuration of full migrate tasks
	FullMigrateConf *TaskFullMigrateConf `json:"full_migrate_conf,omitempty"`

	// configuration of incremental tasks
	IncrMigrateConf *TaskIncrMigrateConf `json:"incr_migrate_conf,omitempty"`

	// source configuration
	SourceConf []TaskSourceConf `json:"source_conf"`
}

// upstream table to downstream migrate rules
type TaskTableMigrateRule struct {
	// filter rule name
	EventFilterName *[]string `json:"event_filter_name,omitempty"`

	// source-related configuration
	Source struct {
		// schema name, wildcard support
		Schema string `json:"schema"`

		// source name
		SourceName string `json:"source_name"`

		// table name, wildcard support
		Table string `json:"table"`
	} `json:"source"`

	// downstream-related configuration
	Target struct {
		// schema name, does not support wildcards
		Schema string `json:"schema"`

		// table name, does not support wildcards
		Table string `json:"table"`
	} `json:"target"`
}

// downstream database configuration
type TaskTargetDataBase struct {
	// source address
	Host string `json:"host"`

	// source password
	Password string `json:"password"`

	// ource port
	Port int `json:"port"`

	// data source ssl configuration, the field will be hidden when getting the data source configuration from the interface
	Security *Security `json:"security"`

	// source username
	User string `json:"user"`
}

// worker name list
type WorkerNameList []string

// DMAPICreateSourceJSONBody defines parameters for DMAPICreateSource.
type DMAPICreateSourceJSONBody Source

// DMAPIStartRelayJSONBody defines parameters for DMAPIStartRelay.
type DMAPIStartRelayJSONBody StartRelayRequest

// DMAPIStopRelayJSONBody defines parameters for DMAPIStopRelay.
type DMAPIStopRelayJSONBody StopRelayRequest

// DMAPIStartTaskJSONBody defines parameters for DMAPIStartTask.
type DMAPIStartTaskJSONBody CreateTaskRequest

// DMAPICreateSourceJSONRequestBody defines body for DMAPICreateSource for application/json ContentType.
type DMAPICreateSourceJSONRequestBody DMAPICreateSourceJSONBody

// DMAPIStartRelayJSONRequestBody defines body for DMAPIStartRelay for application/json ContentType.
type DMAPIStartRelayJSONRequestBody DMAPIStartRelayJSONBody

// DMAPIStopRelayJSONRequestBody defines body for DMAPIStopRelay for application/json ContentType.
type DMAPIStopRelayJSONRequestBody DMAPIStopRelayJSONBody

// DMAPIStartTaskJSONRequestBody defines body for DMAPIStartTask for application/json ContentType.
type DMAPIStartTaskJSONRequestBody DMAPIStartTaskJSONBody
