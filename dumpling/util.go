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
	"github.com/pingcap/dumpling/v4/export"
	"github.com/spf13/pflag"
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

func parseExtraArgs(dumpCfg *export.Config, args []string) error {
	var dumplingFlagSet = pflag.NewFlagSet("dumpling", pflag.ContinueOnError)

	dumplingFlagSet.StringVarP(&dumpCfg.Database, "database", "B", "", "Database to dump")
	dumplingFlagSet.IntVarP(&dumpCfg.Threads, "threads", "t", 4, "Number of goroutines to use, default 4")
	dumplingFlagSet.Uint64VarP(&dumpCfg.FileSize, "filesize", "F", export.UnspecifiedSize, "The approximate size of output file")
	dumplingFlagSet.Uint64VarP(&dumpCfg.StatementSize, "statement-size", "S", export.UnspecifiedSize, "Attempted size of INSERT statement in bytes")
	dumplingFlagSet.StringVar(&dumpCfg.Consistency, "consistency", "auto", "Consistency level during dumping: {auto|none|flush|lock|snapshot}")
	dumplingFlagSet.StringVar(&dumpCfg.Snapshot, "snapshot", "", "Snapshot position. Valid only when consistency=snapshot")
	dumplingFlagSet.BoolVarP(&dumpCfg.NoViews, "no-views", "W", true, "Do not dump views")
	dumplingFlagSet.Uint64VarP(&dumpCfg.FileSize, "rows", "r", export.UnspecifiedSize, "Split table into chunks of this many rows, default unlimited")
	dumplingFlagSet.StringVar(&dumpCfg.Where, "where", "", "Dump only selected records")
	dumplingFlagSet.BoolVar(&dumpCfg.EscapeBackslash, "escape-backslash", true, "use backslash to escape quotation marks")

	return dumplingFlagSet.Parse(args)
}
