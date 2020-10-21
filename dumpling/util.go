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

package dumpling

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/docker/go-units"
	"github.com/pingcap/dumpling/v4/export"
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
	"github.com/spf13/pflag"

	"github.com/pingcap/dm/pkg/log"
)

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

func parseExtraArgs(logger *log.Logger, dumpCfg *export.Config, args []string) error {
	var (
		dumplingFlagSet = pflag.NewFlagSet("dumpling", pflag.ContinueOnError)
		fileSizeStr     string
		tablesList      []string
		filters         []string
		noLocks         bool
	)

	dumplingFlagSet.StringSliceVarP(&dumpCfg.Databases, "database", "B", dumpCfg.Databases, "Database to dump")
	dumplingFlagSet.StringSliceVarP(&tablesList, "tables-list", "T", nil, "Comma delimited table list to dump; must be qualified table names")
	dumplingFlagSet.IntVarP(&dumpCfg.Threads, "threads", "t", dumpCfg.Threads, "Number of goroutines to use, default 4")
	dumplingFlagSet.StringVarP(&fileSizeStr, "filesize", "F", "", "The approximate size of output file")
	dumplingFlagSet.Uint64VarP(&dumpCfg.StatementSize, "statement-size", "S", dumpCfg.StatementSize, "Attempted size of INSERT statement in bytes")
	dumplingFlagSet.StringVar(&dumpCfg.Consistency, "consistency", dumpCfg.Consistency, "Consistency level during dumping: {auto|none|flush|lock|snapshot}")
	dumplingFlagSet.StringVar(&dumpCfg.Snapshot, "snapshot", dumpCfg.Snapshot, "Snapshot position. Valid only when consistency=snapshot")
	dumplingFlagSet.BoolVarP(&dumpCfg.NoViews, "no-views", "W", dumpCfg.NoViews, "Do not dump views")
	dumplingFlagSet.Uint64VarP(&dumpCfg.Rows, "rows", "r", dumpCfg.Rows, "Split table into chunks of this many rows, default unlimited")
	dumplingFlagSet.StringVar(&dumpCfg.Where, "where", dumpCfg.Where, "Dump only selected records")
	dumplingFlagSet.BoolVar(&dumpCfg.EscapeBackslash, "escape-backslash", dumpCfg.EscapeBackslash, "Use backslash to escape quotation marks")
	dumplingFlagSet.StringArrayVarP(&filters, "filter", "f", []string{"*.*"}, "Filter to select which tables to dump")
	dumplingFlagSet.StringVar(&dumpCfg.Security.CAPath, "ca", dumpCfg.Security.CAPath, "The path name to the certificate authority file for TLS connection")
	dumplingFlagSet.StringVar(&dumpCfg.Security.CertPath, "cert", dumpCfg.Security.CertPath, "The path name to the client certificate file for TLS connection")
	dumplingFlagSet.StringVar(&dumpCfg.Security.KeyPath, "key", dumpCfg.Security.KeyPath, "The path name to the client private key file for TLS connection")
	dumplingFlagSet.BoolVar(&noLocks, "no-locks", false, "")

	err := dumplingFlagSet.Parse(args)
	if err != nil {
		return err
	}

	// compatibility for `--no-locks`
	if noLocks {
		logger.Warn("`--no-locks` is replaced by `--consistency none` since v2.0.0")
		// it's default consistency or by meaning of "auto", we could overwrite it by `none`
		if dumpCfg.Consistency == "auto" {
			dumpCfg.Consistency = "none"
		} else if dumpCfg.Consistency != "none" {
			return errors.New("cannot both specify `--no-locks` and `--consistency` other than `none`")
		}
	}

	if fileSizeStr != "" {
		dumpCfg.FileSize, err = parseFileSize(fileSizeStr)
		if err != nil {
			return err
		}
	}

	if len(tablesList) > 0 || (len(filters) != 1 || filters[0] != "*.*") {
		ff, err2 := parseTableFilter(tablesList, filters)
		if err2 != nil {
			return err2
		}
		dumpCfg.TableFilter = ff // overwrite `block-allow-list`.
		logger.Warn("overwrite `block-allow-list` by `tables-list` or `filter`")
	}

	return nil
}

func parseFileSize(fileSizeStr string) (uint64, error) {
	var fileSize uint64
	if len(fileSizeStr) == 0 {
		fileSize = export.UnspecifiedSize
	} else if fileSizeMB, err := strconv.ParseUint(fileSizeStr, 10, 64); err == nil {
		fileSize = fileSizeMB * units.MiB
	} else if size, err := units.RAMInBytes(fileSizeStr); err == nil {
		fileSize = uint64(size)
	} else {
		return 0, err
	}
	return fileSize, nil
}

// parseTableFilter parses `--tables-list` and `--filter`.
// copy (and update) from https://github.com/pingcap/dumpling/blob/6f74c686e54183db7b869775af1c32df46462a6a/cmd/dumpling/main.go#L222.
func parseTableFilter(tablesList, filters []string) (filter.Filter, error) {
	if len(tablesList) == 0 {
		return filter.Parse(filters)
	}

	// only parse -T when -f is default value. otherwise bail out.
	if len(filters) != 1 || filters[0] != "*.*" {
		return nil, errors.New("cannot pass --tables-list and --filter together")
	}

	tableNames := make([]filter.Table, 0, len(tablesList))
	for _, table := range tablesList {
		parts := strings.SplitN(table, ".", 2)
		if len(parts) < 2 {
			return nil, fmt.Errorf("--tables-list only accepts qualified table names, but `%s` lacks a dot", table)
		}
		tableNames = append(tableNames, filter.Table{Schema: parts[0], Name: parts[1]})
	}

	return filter.NewTablesFilter(tableNames...), nil
}
