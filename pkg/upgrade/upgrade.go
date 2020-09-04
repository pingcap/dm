// Copyright 2020 PingCAP, Inc.
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

package upgrade

import (
	"context"
	"fmt"

	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/conn"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/cputil"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/utils"
)

var (
	// upgrades records all functions used to upgrade from one version to the later version.
	upgrades = []func(cli *clientv3.Client, uctx UpgradeContext) error{
		upgradeToVer1,
		upgradeToVer2,
	}
)

// NOTE that zero value of Context is nil, be aware of nil-dereference
type UpgradeContext struct {
	Context        context.Context
	SubTaskConfigs map[string]map[string]config.SubTaskConfig
}

func NewUpgradeContext() UpgradeContext {
	return UpgradeContext{
		Context: context.Background(),
	}
}

// TryUpgrade tries to upgrade the cluster from an older version to a new version.
// This methods should have no side effects even calling multiple times.
func TryUpgrade(cli *clientv3.Client, uctx UpgradeContext) error {
	// 1. get previous version from etcd.
	preVer, _, err := GetVersion(cli)
	if err != nil {
		return err
	}

	// 2. check if any previous version exists.
	if preVer.NotSet() {
		// no initialization operations exist for Ver1 now,
		// add any operations (may includes `upgrades`) if needed later.
		// put the current version into etcd.
		_, err = PutVersion(cli, CurrentVersion)
		return err
	}

	// 3. compare the previous version with the current version.
	if cmp := preVer.Compare(CurrentVersion); cmp == 0 {
		// previous == current version, no need to upgrade.
		return nil
	} else if cmp > 0 {
		// previous >= current version, this often means a older version of DM-master become the leader after started,
		// do nothing for this now.
		return nil
	}

	// 4. do upgrade operations.
	for _, upgrade := range upgrades {
		err = upgrade(cli, uctx)
		if err != nil {
			return err
		}
	}

	// 5. put the current version into etcd.
	_, err = PutVersion(cli, CurrentVersion)
	return err
}

// upgradeToVer1 does upgrade operations from Ver0 to Ver1.
// in fact, this do nothing now, and just for demonstration.
func upgradeToVer1(cli *clientv3.Client, uctx UpgradeContext) error {
	return nil
}

// upgradeToVer2 does upgrade operations from Ver1 to Ver2 (v2.0.0-rc3) to upgrade syncer checkpoint schema
func upgradeToVer2(cli *clientv3.Client, uctx UpgradeContext) error {
	upgradeTaskName := "upgradeToVer2"
	logger := log.L().WithFields(zap.String("task", upgradeTaskName))

	if uctx.SubTaskConfigs == nil {
		logger.Info("no downstream DB, skipping")
		return nil
	}

	// tableName -> DBConfig
	dbConfigs := map[string]config.DBConfig{}
	for task, m := range uctx.SubTaskConfigs {
		for sourceId, subCfg := range m {
			tableName := dbutil.TableName(subCfg.MetaSchema, cputil.SyncerCheckpoint(subCfg.Name))
			subCfg2, err := subCfg.DecryptPassword()
			if err != nil {
				log.L().Error("skip subconfig when upgrading", zap.String("task", task),
					zap.String("source id", sourceId), zap.Error(err))
			}
			dbConfigs[tableName] = subCfg2.To
		}
	}

	toClose := make([]*conn.BaseDB, 0, len(dbConfigs))
	defer func() {
		for _, db := range toClose {
			db.Close()
		}
	}()
	for tableName, cfg := range dbConfigs {
		targetDB, err := conn.DefaultDBProvider.Apply(cfg)
		if err != nil {
			logger.Error("skip target DB when upgrading", zap.String("table name", tableName))
			return err
		}
		toClose = append(toClose, targetDB)
		// try to add columns.
		// NOTE: ignore already exists error to continue the process.
		queries := []string{
			fmt.Sprintf(`ALTER TABLE %s ADD COLUMN binlog_gtid TEXT AFTER binlog_pos`, tableName),
			fmt.Sprintf(`ALTER TABLE %s ADD COLUMN table_info JSON NOT NULL AFTER binlog_gtid`, tableName),
		}
		tctx := tcontext.NewContext(uctx.Context, logger)
		dbConn, err := targetDB.GetBaseConn(tctx.Ctx)
		if err != nil {
			logger.Error("skip target DB when upgrading", zap.String("table name", tableName))
			return err
		}
		_, err = dbConn.ExecuteSQLWithIgnoreError(tctx, nil, upgradeTaskName, utils.IgnoreErrorCheckpoint, queries)
		if err != nil {
			logger.Error("error while adding column for checkpoint table", zap.String("table name", tableName))
			return err
		}
	}

	return nil
}
