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

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/pingcap/tidb-tools/pkg/filter"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
)

// privileges: SELECT, UPDATE,  optionaly INSERT, optionaly CREATE.
// GRANT SELECT,UPDATE,INSERT,CREATE ON `your_database`.`heartbeat` to 'your_replicate_user'@'your_replicate_host';

const (
	// when we not need to support MySQL <=5.5, we can replace with `2006-01-02 15:04:05.000000`
	timeFormat = "2006-01-02 15:04:05"
)

var (
	heartbeat *Heartbeat // singleton instance
	once      sync.Once

	reportLagFunc = reportLag
)

// HeartbeatConfig represents Heartbeat configurations.
type HeartbeatConfig struct {
	updateInterval int64 // in second
	reportInterval int64 // in second
	// serverID from dm-worker (relay)
	// now, heartbeat not be synced to downstream
	// so it will not be used by user directly and also enough to differ from other dm-worker's
	serverID  uint32
	masterCfg config.DBConfig // master server's DBConfig
}

// Equal tests whether config equals to other
func (cfg *HeartbeatConfig) Equal(other *HeartbeatConfig) error {
	if other.updateInterval != 0 && other.updateInterval != cfg.updateInterval {
		return terror.ErrSyncerUnitHeartbeatCheckConfig.Generatef("updateInterval not equal, self: %d, other: %d", cfg.updateInterval, other.updateInterval)
	}
	if other.reportInterval != 0 && other.reportInterval != cfg.reportInterval {
		return terror.ErrSyncerUnitHeartbeatCheckConfig.Generatef("reportInterval not equal, self: %d, other: %d", cfg.reportInterval, other.reportInterval)
	}
	if cfg.serverID != other.serverID {
		return terror.ErrSyncerUnitHeartbeatCheckConfig.Generatef("serverID not equal, self: %d, other: %d", cfg.serverID, other.serverID)
	}
	if !reflect.DeepEqual(cfg.masterCfg, other.masterCfg) {
		return terror.ErrSyncerUnitHeartbeatCheckConfig.Generatef("masterCfg not equal, self: %+v, other: %+v", cfg.masterCfg, other.masterCfg)
	}
	return nil
}

// Heartbeat represents a heartbeat mechanism to measures replication lag on mysql and tidb/mysql.
// Learn from: https://www.percona.com/doc/percona-toolkit/LATEST/pt-heartbeat.html
type Heartbeat struct {
	lock chan struct{} // use a chan to simulate the lock (mutex), because mutex do not support something like TryLock

	cfg    *HeartbeatConfig
	schema string // for which schema the heartbeat table belongs to
	table  string // for which table the heartbeat table belongs to

	master   *sql.DB
	slavesTs map[string]float64 // task-name => slave (syncer) ts

	cancel context.CancelFunc
	wg     sync.WaitGroup

	logger log.Logger
}

// GetHeartbeat gets singleton instance of Heartbeat
func GetHeartbeat(cfg *HeartbeatConfig) (*Heartbeat, error) {
	once.Do(func() {
		heartbeat = &Heartbeat{
			lock:     make(chan struct{}, 1), // with buffer 1, no recursion supported
			cfg:      cfg,
			schema:   filter.DMHeartbeatSchema,
			table:    filter.DMHeartbeatTable,
			slavesTs: make(map[string]float64),
			logger:   log.With(zap.String("component", "heartbeat")),
		}
	})
	if err := heartbeat.cfg.Equal(cfg); err != nil {
		return nil, terror.Annotate(err, "heartbeat config is different from previous used")
	}
	return heartbeat, nil
}

// AddTask adds a new task
func (h *Heartbeat) AddTask(name string) error {
	h.lock <- struct{}{} // send to chan, acquire the lock
	defer func() {
		<-h.lock // read from the chan, release the lock
	}()
	if _, ok := h.slavesTs[name]; ok {
		return terror.ErrSyncerUnitHeartbeatRecordExists.Generate(name)
	}
	if h.master == nil {
		// open DB
		dbCfg := h.cfg.masterCfg
		dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8&interpolateParams=true&readTimeout=1m", dbCfg.User, dbCfg.Password, dbCfg.Host, dbCfg.Port)
		master, err := sql.Open("mysql", dbDSN)
		if err != nil {
			return terror.WithScope(terror.DBErrorAdapt(err, terror.ErrDBDriverError), terror.ScopeUpstream)
		}
		h.master = master

		// init table
		err = h.init()
		if err != nil {
			h.master.Close()
			h.master = nil
			return terror.WithScope(terror.DBErrorAdapt(err, terror.ErrDBDriverError), terror.ScopeUpstream)
		}

		// run work
		if h.cancel != nil {
			h.cancel()
			h.cancel = nil
			h.wg.Wait()
		}
		ctx, cancel := context.WithCancel(context.Background())
		h.cancel = cancel

		h.wg.Add(1)
		go func() {
			defer h.wg.Done()
			h.run(ctx)
		}()
	}
	h.slavesTs[name] = 0 // init to 0
	return nil
}

// RemoveTask removes a previous added task
func (h *Heartbeat) RemoveTask(name string) error {
	h.lock <- struct{}{}
	defer func() {
		<-h.lock
	}()
	if _, ok := h.slavesTs[name]; !ok {
		return terror.ErrSyncerUnitHeartbeatRecordNotFound.Generate(name)
	}
	delete(h.slavesTs, name)

	if len(h.slavesTs) == 0 {
		// cancel work
		h.cancel()
		h.cancel = nil
		h.wg.Wait()

		// close DB
		h.master.Close()
		h.master = nil
	}

	return nil
}

// TryUpdateTaskTs tries to update task's ts
func (h *Heartbeat) TryUpdateTaskTs(taskName, schema, table string, data [][]interface{}) {
	if schema != h.schema || table != h.table {
		h.logger.Debug("don't need to handle non-heartbeat table", zap.String("schema", schema), zap.String("table", table))
		return // not heartbeat table
	}
	if len(data) == 0 || len(data[0]) != 2 {
		h.logger.Warn("rows / columns mismatch for heartbeat", zap.Reflect("data", data))
		return // rows / columns mismatch
	}

	latest := data[len(data)-1]
	serverID, ok := latest[1].(int32)
	if !ok {
		h.logger.Warn("invalid data server_id for heartbeat", zap.Reflect("server ID", latest[1]))
		return
	}
	if uint32(serverID) != h.cfg.serverID {
		h.logger.Debug("ignore mismatched server_id for heartbeat", zap.Int32("obtained server ID", serverID), zap.Uint32("excepted server ID", h.cfg.serverID))
		return // only ignore
	}

	ts, ok := latest[0].(string)
	if !ok {
		h.logger.Warn("invalid ts for heartbeat", zap.Reflect("ts", latest[0]))
		return
	}

	t, err := time.Parse(timeFormat, ts)
	if err != nil {
		h.logger.Error("parse heartbeat ts", zap.String("ts", ts), log.ShortError(err))
		return
	}

	select {
	case h.lock <- struct{}{}:
		if _, ok := h.slavesTs[taskName]; ok {
			h.slavesTs[taskName] = h.timeToSeconds(t)
		}
		<-h.lock
	default:
		// do nothing, because we can accept no update perform
	}
}

func (h *Heartbeat) init() error {
	err := h.createDatabase()
	if err != nil {
		return err
	}

	err = h.createTable()
	if err != nil {
		return err
	}

	return nil
}

// run create `heartbeat` table if not exists, and initialize heartbeat record,
// and then update `ts` every `updateInterval` second.
func (h *Heartbeat) run(ctx context.Context) {

	updateTicker := time.NewTicker(time.Second * time.Duration(h.cfg.updateInterval))
	defer updateTicker.Stop()

	reportTicker := time.NewTicker(time.Second * time.Duration(h.cfg.reportInterval))
	defer reportTicker.Stop()

	for {
		select {
		case <-updateTicker.C:
			err := h.updateTS()
			if err != nil {
				h.logger.Error("update heartbeat ts", zap.Error(err))
			}

		case <-reportTicker.C:
			err := h.calculateLag(ctx)
			if err != nil {
				h.logger.Error("calculate replication lag", zap.Error(err))
			}

		case <-ctx.Done():
			return
		}
	}
}

// createTable creates heartbeat database if not exists in master
func (h *Heartbeat) createDatabase() error {
	createDatabase := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", h.schema)
	_, err := h.master.Exec(createDatabase)
	h.logger.Info("create heartbeat schema", zap.String("sql", createDatabase))
	return terror.WithScope(terror.DBErrorAdapt(err, terror.ErrDBDriverError), terror.ScopeUpstream)
}

// createTable creates heartbeat table if not exists in master
func (h *Heartbeat) createTable() error {
	tableName := fmt.Sprintf("`%s`.`%s`", h.schema, h.table)
	createTableStmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
  ts varchar(26) NOT NULL,
  server_id int(10) unsigned NOT NULL,
  PRIMARY KEY (server_id)
)`, tableName)

	_, err := h.master.Exec(createTableStmt)
	h.logger.Info("create heartbeat table", zap.String("sql", createTableStmt))
	return terror.WithScope(terror.DBErrorAdapt(err, terror.ErrDBDriverError), terror.ScopeUpstream)
}

// updateTS use `REPLACE` statement to insert or update ts
func (h *Heartbeat) updateTS() error {
	// when we not need to support MySQL <=5.5, we can replace with `UTC_TIMESTAMP(6)`
	query := fmt.Sprintf("REPLACE INTO `%s`.`%s` (`ts`, `server_id`) VALUES(UTC_TIMESTAMP(), ?)", h.schema, h.table)
	_, err := h.master.Exec(query, h.cfg.serverID)
	h.logger.Debug("update ts", zap.String("sql", query), zap.Uint32("server ID", h.cfg.serverID))
	return terror.WithScope(terror.DBErrorAdapt(err, terror.ErrDBDriverError), terror.ScopeUpstream)
}

func (h *Heartbeat) calculateLag(ctx context.Context) error {
	masterTS, err := h.getMasterTS()
	if err != nil {
		return err
	}

	select {
	case h.lock <- struct{}{}:
		for taskName, ts := range h.slavesTs {
			lag := masterTS - ts
			reportLagFunc(taskName, lag)
		}
		<-h.lock
	case <-ctx.Done():
		// can be canceled by outer
	}

	return nil
}

func reportLag(taskName string, lag float64) {
	replicationLagGauge.WithLabelValues(taskName).Set(float64(lag))
}

func (h *Heartbeat) getMasterTS() (float64, error) {
	return h.getTS(h.master)
}

func (h *Heartbeat) getTS(db *sql.DB) (float64, error) {
	query := fmt.Sprintf("SELECT `ts` FROM `%s`.`%s` WHERE `server_id`=?", h.schema, h.table)
	var ts string
	err := db.QueryRow(query, h.cfg.serverID).Scan(&ts)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, terror.WithScope(terror.DBErrorAdapt(err, terror.ErrDBDriverError), terror.ScopeUpstream)
	}

	return h.tsToSeconds(ts)
}

func (h *Heartbeat) tsToSeconds(ts string) (float64, error) {
	t, err := time.Parse(timeFormat, ts)
	if err != nil {
		return 0, terror.ErrSyncerUnitHeartbeatRecordNotValid.Delegate(err, ts)
	}

	return h.timeToSeconds(t), nil
}

func (h *Heartbeat) timeToSeconds(t time.Time) float64 {
	nsec := t.UnixNano()
	sec := nsec / 1e9
	nsec = nsec % 1e9
	return float64(sec) + float64(nsec)/1e9
}
