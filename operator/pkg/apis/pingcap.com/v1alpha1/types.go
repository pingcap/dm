/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// DMNode is a specification for a DMNode resource
type DMNode struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              DMNodeSpec   `json:"spec"`
	Status            DMNodeStatus `json:"status"`
}

// DMNodeSpec is the spec for a DMNode resource
type DMNodeSpec struct {
	ContainerSpec
	Service          DMNodeService                        `json:"service,omitempty"`
	Timezone         string                               `json:"timezone,omitempty"`
	Replicas         int32                                `json:"replicas,omitempty"`
	Affinity         *corev1.Affinity                     `json:"affinity,omitempty"`
	NodeSelector     map[string]string                    `json:"nodeSelector,omitempty"`
	StorageClassName string                               `json:"storageClassName,omitempty"`
	Tolerations      []corev1.Toleration                  `json:"tolerations,omitempty"`
	Annotations      map[string]string                    `json:"annotations,omitempty"`
	PVReclaimPolicy  corev1.PersistentVolumeReclaimPolicy `json:"pvReclaimPolicy,omitempty"`
	Source           DMSource                             `json:"source,omitempty"`
	NodeConfig       DMWorkerConfig                       `json:"nodeConfig,omitempty"`
	MasterConfig     DMMasterConfig                       `json:"masterConfig,omitempty"`
}

//DMWorkerConfig is the config for DM worker
type DMWorkerConfig struct {
	LogLevel    string `toml:"log-level" json:"log-level"`
	LogFile     string `toml:"log-file" json:"log-file"`
	LogRotate   string `toml:"log-rotate" json:"log-rotate"`
	WorkerAddr  string `toml:"worker-addr" json:"worker-addr"`
	EnableGTID  bool   `toml:"enable-gtid" json:"enable-gtid"`
	AutoFixGTID bool   `toml:"auto-fix-gtid" json:"auto-fix-gtid"`
	RelayDir    string `toml:"relay-dir" json:"relay-dir"`
	MetaDir     string `toml:"meta-dir" json:"meta-dir"`
	ServerID    uint32 `toml:"server-id" json:"server-id"`
	Flavor      string `toml:"flavor" json:"flavor"`
	Charset     string `toml:"charset" json:"charset"`
	// relay synchronous starting point (if specified)
	RelayBinLogName string   `toml:"relay-binlog-name" json:"relay-binlog-name"`
	RelayBinlogGTID string   `toml:"relay-binlog-gtid" json:"relay-binlog-gtid"`
	SourceID        string   `toml:"source-id" json:"source-id"`
	From            DBConfig `toml:"from" json:"from"`

	// config items for purger
	Purge DMPurger `toml:"purge" json:"purge"`

	// config items for tracer
	Tracer DMTracer `toml:"tracer" json:"tracer"`

	ConfigFile string `json:"config-file"`
}

//DBConfig is the DB config
type DBConfig struct {
	DMDB
	MaxAllowedPacket *int `toml:"max-allowed-packet" json:"max-allowed-packet" yaml:"max-allowed-packet"`
}

//DMPurger is the purger config
type DMPurger struct {
	Interval    int64 `toml:"interval" json:"interval"`         // check whether need to purge at this @Interval (seconds)
	Expires     int64 `toml:"expires" json:"expires"`           // if file's modified time is older than @Expires (hours), then it can be purged
	RemainSpace int64 `toml:"remain-space" json:"remain-space"` // if remain space in @RelayBaseDir less than @RemainSpace (GB), then it can be purged
}

// DMTracer is the tracer config
type DMTracer struct {
	Enable     bool   `toml:"enable" json:"enable"`           // whether to enable tracing
	Source     string `toml:"source" json:"source"`           // trace event source id
	TracerAddr string `toml:"tracer-addr" json:"tracer-addr"` // tracing service rpc address
	BatchSize  int    `toml:"batch-size" json:"batch-size"`   // upload trace event batch size
	Checksum   bool   `toml:"checksum" json:"checksum"`       // whether to caclculate checksum of data
}

//DMMasterConfig is the DM master config
type DMMasterConfig struct {
	LogLevel  string `toml:"log-level" json:"log-level"`
	LogFile   string `toml:"log-file" json:"log-file"`
	LogRotate string `toml:"log-rotate" json:"log-rotate"`

	RPCTimeoutStr string          `toml:"rpc-timeout" json:"rpc-timeout"`
	RPCRateLimit  float64         `toml:"rpc-rate-limit" json:"rpc-rate-limit"`
	RPCRateBurst  int             `toml:"rpc-rate-burst" json:"rpc-rate-burst"`
	MasterAddr    string          `toml:"master-addr" json:"master-addr"`
	Deploy        []*DeployMapper `toml:"deploy" json:"deploy"`
	ConfigFile    string          `json:"config-file"`
}

//DeployMapper is the deploy config for DM master
type DeployMapper struct {
	MySQL  string `toml:"mysql-instance" json:"mysql-instance"` //  deprecated, use source-id instead
	Source string `toml:"source-id" json:"source-id"`           // represents a MySQL/MariaDB instance or a replica group
	Worker string `toml:"dm-worker" json:"dm-worker"`
}

// DMNodeService is the service spec for a DMNode resource
type DMNodeService struct {
	Annotations map[string]string  `json:"annotations,omitempty"`
	Spec        corev1.ServiceSpec `json:"spec,omitempty"`
}

// DMSource is the upstream source spec
type DMSource struct {
	ID string `json:"id"`
	DBConfig
}

// DMDB is the DB info spec
type DMDB struct {
	Host     string `toml:"host" json:"host" yaml:"host"`
	Port     int    `toml:"port" json:"port" yaml:"port"`
	User     string `toml:"user" json:"user" yaml:"user"`
	Password string `toml:"password" json:"password" yaml:"password"`
}

// ContainerSpec is the container spec of a pod
type ContainerSpec struct {
	Image           string               `json:"image"`
	ImagePullPolicy corev1.PullPolicy    `json:"imagePullPolicy,omitempty"`
	Requests        *ResourceRequirement `json:"requests,omitempty"`
	Limits          *ResourceRequirement `json:"limits,omitempty"`
}

// ResourceRequirement is resource requirements for a pod
type ResourceRequirement struct {
	// CPU is how many cores a pod requires
	CPU string `json:"cpu,omitempty"`
	// Memory is how much memory a pod requires
	Memory string `json:"memory,omitempty"`
	// Storage is storage size a pod requires
	Storage string `json:"storage,omitempty"`
}

// DMNodeStatus is the status for a DMNode resource
type DMNodeStatus struct {
	StatefulSet apps.StatefulSetStatus `json:"statefulSet,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// DMNodeList is a list of DMNode resources
type DMNodeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []DMNode `json:"items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// DMTask is a specification for a DMTask resource
type DMTask struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              DMTaskSpec   `json:"spec"`
	Status            DMTaskStatus `json:"status"`
}

// DMTaskSpec is a specification for a DMTask resource
type DMTaskSpec struct {
	Status string       `json:"status"`
	Task   DMTaskConfig `json:"task"`
}

// DMTaskStatus is the status for a DMTask resource
type DMTaskStatus struct {
	CurrentStatus string `json:"currentStatus"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// DMTaskList is a list of DMTask resources
type DMTaskList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []DMTask `json:"items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// DMMeta is a specification for a DMMeta resource
type DMMeta struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              DMMetaSpec `json:"spec"`
}

// DMMetaSpec is a specification for a DMMeta resource
type DMMetaSpec struct {
	Mysql DMDB `json:"mysql"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// DMMetaList is a list of DMMeta resources
type DMMetaList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []DMMeta `json:"items"`
}

// DMTaskConfig is the configuration for Task
type DMTaskConfig struct {
	Name       string `yaml:"name" toml:"name" json:"name"`
	TaskMode   string `yaml:"task-mode" toml:"task-mode" json:"task-mode"`
	IsSharding bool   `yaml:"is-sharding" toml:"is-sharding" json:"is-sharding"`
	//  treat it as hidden configuration
	IgnoreCheckingItems []string `yaml:"ignore-checking-items" toml:"ignore-checking-items" json:"ignore-checking-items"`
	// we store detail status in meta
	// don't save configuration into it
	MetaSchema string `yaml:"meta-schema" toml:"meta-schema" json:"meta-schema"`
	// remove meta from downstreaming database
	// now we delete checkpoint and online ddl information
	RemoveMeta              bool   `yaml:"remove-meta" toml:"remove-meta" json:"remove-meta"`
	DisableHeartbeat        bool   `yaml:"disable-heartbeat" toml:"disable-heartbeat" json:"disable-heartbeat"` //  deprecated, use !enable-heartbeat instead
	EnableHeartbeat         bool   `yaml:"enable-heartbeat" toml:"enable-heartbeat" json:"enable-heartbeat"`
	HeartbeatUpdateInterval int    `yaml:"heartbeat-update-interval" toml:"heartbeat-update-interval" json:"heartbeat-update-interval"`
	HeartbeatReportInterval int    `yaml:"heartbeat-report-interval" toml:"heartbeat-report-interval" json:"heartbeat-report-interval"`
	Timezone                string `yaml:"timezone" toml:"timezone" json:"timezone"`

	// handle schema/table name mode, and only for schema/table name
	// if case insensitive, we would convert schema/table name to lower case
	CaseSensitive bool `yaml:"case-sensitive" toml:"case-sensitive" json:"case-sensitive"`

	TargetDB *DBConfig `yaml:"target-database" toml:"target-database" json:"target-database"`

	MySQLInstances []*MySQLInstance `yaml:"mysql-instances" toml:"mysql-instances" json:"mysql-instances"`

	OnlineDDLScheme string `yaml:"online-ddl-scheme" toml:"online-ddl-scheme" json:"online-ddl-scheme"`

	Routes         map[string]*TableRule       `yaml:"routes" toml:"routes" json:"routes"`
	Filters        map[string]*BinlogEventRule `yaml:"filters" toml:"filters" json:"filters"`
	ColumnMappings map[string]*Rule            `yaml:"column-mappings" toml:"column-mappings" json:"column-mappings"`
	BWList         map[string]*Rules           `yaml:"black-white-list" toml:"black-white-list" json:"black-white-list"`

	Mydumpers map[string]*MydumperConfig `yaml:"mydumpers" toml:"mydumpers" json:"mydumpers"`
	Loaders   map[string]*LoaderConfig   `yaml:"loaders" toml:"loaders" json:"loaders"`
	Syncers   map[string]*SyncerConfig   `yaml:"syncers" toml:"syncers" json:"syncers"`
}

// MySQLInstance represents a sync config of a MySQL instance
type MySQLInstance struct {
	// it represents a MySQL/MariaDB instance or a replica group
	SourceID           string   `yaml:"source-id" toml:"source-id" json:"source-id"`
	Meta               *Meta    `yaml:"meta" toml:"meta" json:"meta"`
	FilterRules        []string `yaml:"filter-rules" toml:"filter-rules" json:"filter-rules"`
	ColumnMappingRules []string `yaml:"column-mapping-rules" toml:"column-mapping-rules" json:"column-mapping-rules"`
	RouteRules         []string `yaml:"route-rules" toml:"route-rules" json:"route-rules"`
	BWListName         string   `yaml:"black-white-list" toml:"black-white-list" json:"black-white-list"`

	MydumperConfigName string          `yaml:"mydumper-config-name" toml:"mydumper-config-name" json:"mydumper-config-name"`
	Mydumper           *MydumperConfig `yaml:"mydumper" toml:"mydumper" json:"mydumper"`
	LoaderConfigName   string          `yaml:"loader-config-name" toml:"loader-config-name" json:"loader-config-name"`
	Loader             *LoaderConfig   `yaml:"loader" toml:"loader" json:"loader"`
	SyncerConfigName   string          `yaml:"syncer-config-name" toml:"syncer-config-name" json:"syncer-config-name"`
	Syncer             *SyncerConfig   `yaml:"syncer" toml:"syncer" json:"syncer"`
}

//Meta is the meta config for binlog
type Meta struct {
	BinLogName string `yaml:"binlog-name" toml:"binlog-name" json:"binlog-name"`
	BinLogPos  uint32 `yaml:"binlog-pos" toml:"binlog-pos" json:"binlog-pos"`
}

// MydumperConfig represents mydumper process unit's specific config
type MydumperConfig struct {
	MydumperPath  string `yaml:"mydumper-path" toml:"mydumper-path" json:"mydumper-path"`    // mydumper binary path
	Threads       int    `yaml:"threads" toml:"threads" json:"threads"`                      // -t, --threads
	ChunkFilesize int64  `yaml:"chunk-filesize" toml:"chunk-filesize" json:"chunk-filesize"` // -F, --chunk-filesize
	SkipTzUTC     bool   `yaml:"skip-tz-utc" toml:"skip-tz-utc" json:"skip-tz-utc"`          // --skip-tz-utc
	ExtraArgs     string `yaml:"extra-args" toml:"extra-args" json:"extra-args"`             // other extra args
	// NOTE: use LoaderConfig.Dir as --outputdir
	// TODO zxc: combine -B -T --regex with filter rules?
}

// LoaderConfig represents loader process unit's specific config
type LoaderConfig struct {
	PoolSize int    `yaml:"pool-size" toml:"pool-size" json:"pool-size"`
	Dir      string `yaml:"dir" toml:"dir" json:"dir"`
}

// SyncerConfig represents syncer process unit's specific config
type SyncerConfig struct {
	MetaFile    string `yaml:"meta-file" toml:"meta-file" json:"meta-file"` // meta filename, used only when load SubConfig directly
	WorkerCount int    `yaml:"worker-count" toml:"worker-count" json:"worker-count"`
	Batch       int    `yaml:"batch" toml:"batch" json:"batch"`
	MaxRetry    int    `yaml:"max-retry" toml:"max-retry" json:"max-retry"`

	// refine following configs to top level configs?
	AutoFixGTID      bool `yaml:"auto-fix-gtid" toml:"auto-fix-gtid" json:"auto-fix-gtid"`
	EnableGTID       bool `yaml:"enable-gtid" toml:"enable-gtid" json:"enable-gtid"`
	DisableCausality bool `yaml:"disable-detect" toml:"disable-detect" json:"disable-detect"`
	SafeMode         bool `yaml:"safe-mode" toml:"safe-mode" json:"safe-mode"`
	EnableANSIQuotes bool `yaml:"enable-ansi-quotes" toml:"enable-ansi-quotes" json:"enable-ansi-quotes"`
}

//TableRule is the config for table rules
type TableRule struct {
	SchemaPattern string `json:"schema-pattern" toml:"schema-pattern" yaml:"schema-pattern"`
	TablePattern  string `json:"table-pattern" toml:"table-pattern" yaml:"table-pattern"`
	TargetSchema  string `json:"target-schema" toml:"target-schema" yaml:"target-schema"`
	TargetTable   string `json:"target-table" toml:"target-table" yaml:"target-table"`
}

//BinlogEventRule is the config for binlog event rules
type BinlogEventRule struct {
	SchemaPattern string      `json:"schema-pattern" toml:"schema-pattern" yaml:"schema-pattern"`
	TablePattern  string      `json:"table-pattern" toml:"table-pattern" yaml:"table-pattern"`
	Events        []EventType `json:"events" toml:"events" yaml:"events"`
	SQLPattern    []string    `json:"sql-pattern" toml:"sql-pattern" yaml:"sql-pattern"` // regular expression
	Action        ActionType  `json:"action" toml:"action" yaml:"action"`
}

// ActionType is the action type
type ActionType string

//EventType is the event type
type EventType string

// Expr indicates how to handle column mapping
type Expr string

//Rule is the config for column mapping rules
type Rule struct {
	PatternSchema    string   `yaml:"schema-pattern" json:"schema-pattern" toml:"schema-pattern"`
	PatternTable     string   `yaml:"table-pattern" json:"table-pattern" toml:"table-pattern"`
	SourceColumn     string   `yaml:"source-column" json:"source-column" toml:"source-column"` // modify, add refer column, ignore
	TargetColumn     string   `yaml:"target-column" json:"target-column" toml:"target-column"` // add column, modify
	Expression       Expr     `yaml:"expression" json:"expression" toml:"expression"`
	Arguments        []string `yaml:"arguments" json:"arguments" toml:"arguments"`
	CreateTableQuery string   `yaml:"create-table-query" json:"create-table-query" toml:"create-table-query"`
}

//Rules is the config for filter rules
type Rules struct {
	DoTables []*Table `json:"do-tables" toml:"do-tables" yaml:"do-tables"`
	DoDBs    []string `json:"do-dbs" toml:"do-dbs" yaml:"do-dbs"`

	IgnoreTables []*Table `json:"ignore-tables" toml:"ignore-tables" yaml:"ignore-tables"`
	IgnoreDBs    []string `json:"ignore-dbs" toml:"ignore-dbs" yaml:"ignore-dbs"`
}

//Table define the tables
type Table struct {
	Schema string `toml:"db-name" json:"db-name" yaml:"db-name"`
	Name   string `toml:"tbl-name" json:"tbl-name" yaml:"tbl-name"`
}
