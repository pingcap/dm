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
	"strings"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/conn"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"

	"github.com/pingcap/tidb-tools/pkg/filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
)

var applyNewBaseDB = conn.DefaultDBProvider.Apply
var fetchTargetDoTables = utils.FetchTargetDoTables

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

// fetchMyDumperDoTables fetches and filters the tables that needed to be dumped through black-white list and route rules
func fetchMyDumperDoTables(cfg *config.SubTaskConfig) (string, error) {
	fromDB, err := applyNewBaseDB(cfg.From)
	if err != nil {
		return "", terror.WithClass(err, terror.ClassDumpUnit)
	}
	defer fromDB.Close()
	bw, err := filter.New(cfg.CaseSensitive, cfg.BWList)
	if err != nil {
		return "", terror.ErrDumpUnitGenBWList.Delegate(err)
	}
	r, err := router.NewTableRouter(cfg.CaseSensitive, cfg.RouteRules)
	if err != nil {
		return "", terror.ErrDumpUnitGenTableRouter.Delegate(err)
	}
	sourceTables, err := fetchTargetDoTables(fromDB.DB, bw, r)
	if err != nil {
		return "", terror.WithClass(err, terror.ClassDumpUnit)
	}
	var filteredTables []string
	// TODO: For tables which contains special chars like ' , ` mydumper will fail while dumping. Once this bug is fixed on mydumper we should add quotes to table.Schema and table.Name
	for _, tables := range sourceTables {
		for _, table := range tables {
			filteredTables = append(filteredTables, table.Schema+"."+table.Name)
		}
	}
	return strings.Join(filteredTables, ","), nil
}

// needToGenerateDoTables will check whether customers specify the databases/tables that needed to be dumped
// If not, this function will return true to notify mydumper to generate args
func needToGenerateDoTables(args []string) bool {
	for _, arg := range args {
		if strings.HasPrefix(arg, "-B") || strings.HasPrefix(arg, "--database") {
			return false
		}
		if strings.HasPrefix(arg, "-T") || strings.HasPrefix(arg, "--tables-list") {
			return false
		}
		if strings.HasPrefix(arg, "-x") || strings.HasPrefix(arg, "--regex") {
			return false
		}
	}
	return true
}
