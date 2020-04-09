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

package common

import (
	"time"

	"github.com/siddontang/go-mysql/replication"
)

var (
	// max reconnection times for binlog syncer in go-mysql
	maxBinlogSyncerReconnect = 60
	// SlaveReadTimeout slave read binlog data timeout, ref: https://dev.mysql.com/doc/refman/8.0/en/replication-options-slave.html#sysvar_slave_net_timeout
	SlaveReadTimeout      = 1 * time.Minute
	masterHeartbeatPeriod = 30 * time.Second // master server send heartbeat period: ref: `MASTER_HEARTBEAT_PERIOD` in https://dev.mysql.com/doc/refman/8.0/en/change-master-to.html
)

// SetDefaultReplicationCfg sets some default value for BinlogSyncerConfig
func SetDefaultReplicationCfg(cfg *replication.BinlogSyncerConfig) {
	cfg.UseDecimal = true // must set true. ref: https://github.com/pingcap/tidb-enterprise-tools/pull/272
	cfg.VerifyChecksum = true
	cfg.MaxReconnectAttempts = maxBinlogSyncerReconnect
	cfg.ReadTimeout = SlaveReadTimeout
	cfg.HeartbeatPeriod = masterHeartbeatPeriod
}
