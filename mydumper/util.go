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

package mydumper

import (
	"fmt"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/baseconn"
	"github.com/pingcap/dm/pkg/retry"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/pkg/filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"go.uber.org/zap"
)

var maxDMLConnectionTimeout = "5m"

// ParseArgLikeBash parses list arguments like bash, which helps us to run
// executable command via os/exec more likely running from bash
func ParseArgLikeBash(args []string) []string {
	result := make([]string, 0, len(args))
	for _, arg := range args {
		parsedArg := trimOutQuotes(arg)
		result = append(result, parsedArg)
	}
	return result
}

// trimOutQuotes trims a pair of single quotes or a pair of double quotes from arg
func trimOutQuotes(arg string) string {
	argLen := len(arg)
	if argLen >= 2 {
		if arg[0] == '"' && arg[argLen-1] == '"' {
			return arg[1 : argLen-1]
		}
		if arg[0] == '\'' && arg[argLen-1] == '\'' {
			return arg[1 : argLen-1]
		}
	}
	return arg
}

func createBaseConn(dbCfg config.DBConfig, timeout string, rawDBCfg *baseconn.RawDBConfig) (*baseconn.BaseConn, error) {
	failpoint.Inject("mockMydumperEmptyExtraArgs", func(_ failpoint.Value) {
		log.S().Info("create mock baseConn which is nil", zap.String("failpoint", "mockMydumperEmptyExtraArgs"))
		failpoint.Return(nil, nil)
	})

	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&interpolateParams=true&readTimeout=%s&maxAllowedPacket=%d",
		dbCfg.User, dbCfg.Password, dbCfg.Host, dbCfg.Port, timeout, *dbCfg.MaxAllowedPacket)
	baseConn, err := baseconn.NewBaseConn(dbDSN, &retry.FiniteRetryStrategy{}, rawDBCfg)
	if err != nil {
		return nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	return baseConn, nil
}

func fetchMyDumperDoTables(cfg *config.SubTaskConfig) (string, error) {
	fromDB, err := createBaseConn(cfg.From, maxDMLConnectionTimeout, baseconn.DefaultRawDBConfig())
	if err != nil {
		return "", err
	}
	bw := filter.New(cfg.CaseSensitive, cfg.BWList)
	r, err := router.NewTableRouter(cfg.CaseSensitive, cfg.RouteRules)
	if err != nil {
		return "", err
	}
	sourceTables, err := utils.FetchTargetDoTables(fromDB.DB, bw, r)
	if err != nil {
		return "", err
	}
	var stringTables string
	var notFirstTable bool
	for _, tables := range sourceTables {
		for _, table := range tables {
			if notFirstTable {
				stringTables += ","
			} else {
				notFirstTable = true
			}
			stringTables += table.String()
		}
	}
	return stringTables, nil
}
