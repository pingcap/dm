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

package checker

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql" // for mysql
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/unit"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/utils"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/check"
	column "github.com/pingcap/tidb-tools/pkg/column-mapping"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/siddontang/go/sync2"
	"golang.org/x/net/context"
)

type mysqlInstance struct {
	cfg *config.SubTaskConfig

	sourceDB     *sql.DB
	sourceDBinfo *dbutil.DBConfig

	targetDB     *sql.DB
	targetDBInfo *dbutil.DBConfig
}

// Checker performs pre-check of data synchronization
type Checker struct {
	closed sync2.AtomicBool

	instances []*mysqlInstance

	checkList     []check.Checker
	checkingItems map[string]string
	result        struct {
		sync.RWMutex
		detail *check.Results
	}
}

// NewChecker returns a checker
func NewChecker(cfgs []*config.SubTaskConfig, checkingItems map[string]string) *Checker {
	c := &Checker{
		instances:     make([]*mysqlInstance, 0, len(cfgs)),
		checkingItems: checkingItems,
	}

	for _, cfg := range cfgs {
		c.instances = append(c.instances, &mysqlInstance{
			cfg: cfg,
		})
	}

	return c
}

// Init implements Unit interface
func (c *Checker) Init() error {
	// target name => source => schema => [tables]
	sharding := make(map[string]map[string]map[string][]string)
	shardingCounter := make(map[string]int)
	dbs := make(map[string]*sql.DB)
	columnMapping := make(map[string]*column.Mapping)
	_, checkingShardID := c.checkingItems[config.ShardAutoIncrementIDChecking]
	_, checkingShard := c.checkingItems[config.ShardTableSchemaChecking]
	_, checkSchema := c.checkingItems[config.TableSchemaChecking]

	for _, instance := range c.instances {
		bw := filter.New(instance.cfg.CaseSensitive, instance.cfg.BWList)
		r, err := router.NewTableRouter(instance.cfg.CaseSensitive, instance.cfg.RouteRules)
		if err != nil {
			return errors.Trace(err)
		}

		columnMapping[instance.cfg.SourceID], err = column.NewMapping(instance.cfg.CaseSensitive, instance.cfg.ColumnMappingRules)
		if err != nil {
			return errors.Trace(err)
		}

		instance.sourceDBinfo = &dbutil.DBConfig{
			Host:     instance.cfg.From.Host,
			Port:     instance.cfg.From.Port,
			User:     instance.cfg.From.User,
			Password: instance.cfg.From.Password,
		}

		instance.sourceDB, err = dbutil.OpenDB(*instance.sourceDBinfo)
		if err != nil {
			return errors.Trace(err)
		}

		instance.targetDBInfo = &dbutil.DBConfig{
			Host:     instance.cfg.To.Host,
			Port:     instance.cfg.To.Port,
			User:     instance.cfg.To.User,
			Password: instance.cfg.To.Password,
		}
		if len(instance.targetDBInfo.Password) > 0 {
			pswd, err2 := utils.Decrypt(instance.targetDBInfo.Password)
			if err2 != nil {
				return errors.Annotatef(err2, "can not decrypt password %s", instance.targetDBInfo.Password)
			}
			instance.targetDBInfo.Password = pswd
		}
		instance.targetDB, err = dbutil.OpenDB(*instance.targetDBInfo)
		if err != nil {
			return errors.Trace(err)
		}

		if _, ok := c.checkingItems[config.VersionChecking]; ok {
			c.checkList = append(c.checkList, check.NewMySQLVersionChecker(instance.sourceDB, instance.sourceDBinfo))
		}
		if _, ok := c.checkingItems[config.BinlogEnableChecking]; ok {
			c.checkList = append(c.checkList, check.NewMySQLBinlogEnableChecker(instance.sourceDB, instance.sourceDBinfo))
		}
		if _, ok := c.checkingItems[config.BinlogFormatChecking]; ok {
			c.checkList = append(c.checkList, check.NewMySQLBinlogFormatChecker(instance.sourceDB, instance.sourceDBinfo))
		}
		if _, ok := c.checkingItems[config.BinlogRowImageChecking]; ok {
			c.checkList = append(c.checkList, check.NewMySQLBinlogRowImageChecker(instance.sourceDB, instance.sourceDBinfo))
		}
		if _, ok := c.checkingItems[config.DumpPrivilegeChecking]; ok {
			c.checkList = append(c.checkList, check.NewSourceDumpPrivilegeChecker(instance.sourceDB, instance.sourceDBinfo))
		}
		if _, ok := c.checkingItems[config.ReplicationPrivilegeChecking]; ok {
			c.checkList = append(c.checkList, check.NewSourceReplicationPrivilegeChecker(instance.sourceDB, instance.sourceDBinfo))
		}

		if !checkingShard && !checkSchema {
			continue
		}

		mapping, err := utils.FetchTargetDoTables(instance.sourceDB, bw, r)
		if err != nil {
			return errors.Trace(err)
		}

		err = sameTableNameDetection(mapping)
		if err != nil {
			return errors.Trace(err)
		}

		checkTables := make(map[string][]string)
		for name, tables := range mapping {
			for _, table := range tables {
				checkTables[table.Schema] = append(checkTables[table.Schema], table.Name)
				if _, ok := sharding[name]; !ok {
					sharding[name] = make(map[string]map[string][]string)
				}
				if _, ok := sharding[name][instance.cfg.SourceID]; !ok {
					sharding[name][instance.cfg.SourceID] = make(map[string][]string)
				}
				if _, ok := sharding[name][instance.cfg.SourceID][table.Schema]; !ok {
					sharding[name][instance.cfg.SourceID][table.Schema] = make([]string, 0, 1)
				}

				sharding[name][instance.cfg.SourceID][table.Schema] = append(sharding[name][instance.cfg.SourceID][table.Schema], table.Name)
				shardingCounter[name]++
			}
		}
		dbs[instance.cfg.SourceID] = instance.sourceDB

		if checkSchema {
			c.checkList = append(c.checkList, check.NewTablesChecker(instance.sourceDB, instance.sourceDBinfo, checkTables))
		}
	}

	if checkingShard {
		for name, shardingSet := range sharding {
			if shardingCounter[name] <= 1 {
				continue
			}

			c.checkList = append(c.checkList, check.NewShardingTablesCheck(name, dbs, shardingSet, columnMapping, checkingShardID))
		}
	}

	log.Infof(c.displayCheckingItems())
	return nil
}

func (c *Checker) displayCheckingItems() string {
	if len(c.checkList) == 0 {
		return "not found any checking items\n"
	}

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "\n************ task %s checking items ************\n", c.instances[0].cfg.Name)
	for _, checkFunc := range c.checkList {
		fmt.Fprintf(&buf, "%s\n", checkFunc.Name())
	}
	fmt.Fprintf(&buf, "************ task %s checking items ************", c.instances[0].cfg.Name)
	return buf.String()
}

// Process implements Unit interface
func (c *Checker) Process(ctx context.Context, pr chan pb.ProcessResult) {
	cctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	isCanceled := false
	errs := make([]*pb.ProcessError, 0, 1)
	result, _ := check.Do(cctx, c.checkList)
	if !result.Summary.Passed {
		errs = append(errs, unit.NewProcessError(pb.ErrorType_CheckFailed, "check was failed, please see detail"))

	}

	select {
	case <-cctx.Done():
		isCanceled = true
	default:
	}

	rawResult, err := json.MarshalIndent(result, "\t", "\t")
	if err != nil {
		rawResult = []byte(fmt.Sprintf("marshal error %v", err))
	}

	c.result.Lock()
	c.result.detail = result
	c.result.Unlock()

	pr <- pb.ProcessResult{
		IsCanceled: isCanceled,
		Errors:     errs,
		Detail:     rawResult,
	}
}

// Close implements Unit interface
func (c *Checker) Close() {
	if c.closed.Get() {
		return
	}

	for _, instance := range c.instances {
		if instance.sourceDB != nil {
			if err := dbutil.CloseDB(instance.sourceDB); err != nil {
				log.Errorf("close source db %+v error %v", instance.sourceDBinfo, err)
			}
		}

		if instance.targetDB != nil {
			if err := dbutil.CloseDB(instance.targetDB); err != nil {
				log.Errorf("close target db %+v error %v", instance.targetDBInfo, err)
			}
		}
	}

	c.closed.Set(true)
}

// Pause implements Unit interface
func (c *Checker) Pause() {
	if c.closed.Get() {
		log.Warn("[checker] try to pause, but already closed")
		return
	}
}

// Resume resumes the paused process
func (c *Checker) Resume(ctx context.Context, pr chan pb.ProcessResult) {
	if c.closed.Get() {
		log.Warn("[checker] try to resume, but already closed")
		return
	}

	c.Process(ctx, pr)
}

// Update implements Unit.Update
func (c *Checker) Update(cfg *config.SubTaskConfig) error {
	// not support update configuration now
	return nil
}

// Type implements Unit interface
func (c *Checker) Type() pb.UnitType {
	return pb.UnitType_Check
}

// IsFreshTask implements Unit.IsFreshTask
func (c *Checker) IsFreshTask() (bool, error) {
	return true, nil
}

// Status implements Unit interface
func (c *Checker) Status() interface{} {
	c.result.RLock()
	res := c.result.detail
	c.result.RUnlock()

	rawResult, err := json.Marshal(res)
	if err != nil {
		rawResult = []byte(fmt.Sprintf("marshal %+v error %v", res, err))
	}

	return &pb.CheckStatus{
		Passed:     res.Summary.Passed,
		Total:      int32(res.Summary.Total),
		Failed:     int32(res.Summary.Failed),
		Successful: int32(res.Summary.Successful),
		Warning:    int32(res.Summary.Warning),
		Detail:     rawResult,
	}
}

// Error implements Unit interface
func (c *Checker) Error() interface{} {
	return &pb.CheckError{}
}
func sameTableNameDetection(tables map[string][]*filter.Table) error {
	tableNameSets := make(map[string]string)
	var messages []string

	for name := range tables {
		nameL := strings.ToLower(name)
		if nameO, ok := tableNameSets[nameL]; !ok {
			tableNameSets[nameL] = name
		} else {
			messages = append(messages, fmt.Sprintf("same target table %v vs %s", nameO, name))
		}
	}

	if len(messages) > 0 {
		return errors.Errorf("same table name in case-sensitive %v", messages)
	}

	return nil
}
