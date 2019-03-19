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

package master

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/spf13/cobra"

	"github.com/pingcap/dm/dm/command"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
)

var (
	timeFormat = "2006-01-02_15:04:05" // _ join date and time, no space
	timeLayout = "2006-01-02 15:04:05"
)

// NewPurgeRelayCmd creates a PurgeRelay command
// three purge methods supported by dmctl
// 1. purge inactive relay log files
// 2. purge before time, like `PURGE BINARY LOGS BEFORE` in MySQL
// 3. purge before filename, like `PURGE BINARY LOGS TO`
func NewPurgeRelayCmd() *cobra.Command {
	cmd := &cobra.Command{
		//Use:   "purge-relay <-w worker> [--inactive] [--time] [--filename] [--sub-dir]",
		//Short: "purge dm-worker's relay log files, choose 1 of 2 methods",
		Use:   "purge-relay <-w worker> [--filename] [--sub-dir]",
		Short: "purge dm-worker's relay log files according to specified filename",
		Run:   purgeRelayFunc,
	}
	//cmd.Flags().BoolP("inactive", "i", false, "whether try to purge all inactive relay log files")
	//cmd.Flags().StringP("time", "t", "", fmt.Sprintf("whether try to purge relay log files before this time, the format is \"%s\"(_ between date and time)", timeFormat))
	cmd.Flags().StringP("filename", "f", "", "whether try to purge relay log files before this filename, the format is \"mysql-bin.000006\"")
	cmd.Flags().StringP("sub-dir", "s", "", "specify relay sub directory for --filename, if not specified, the latest one will be used, the format is \"2ae76434-f79f-11e8-bde2-0242ac130008.000001\"")

	return cmd
}

// purgeRelayFunc does purge relay log files
func purgeRelayFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) > 0 {
		fmt.Println(cmd.Usage())
		return
	}

	workers, err := common.GetWorkerArgs(cmd)
	if err != nil {
		fmt.Println(errors.ErrorStack(err))
		return
	}
	if len(workers) == 0 {
		fmt.Println("must specify at least one dm-worker (`-w` / `--worker`)")
		return
	}

	//inactive, err := cmd.Flags().GetBool("inactive")
	//if err != nil {
	//	fmt.Println(errors.Trace(err))
	//	return
	//}
	//
	//timeStr, err := cmd.Flags().GetString("time")
	//if err != nil {
	//	fmt.Println(errors.Trace(err))
	//	return
	//}

	filename, err := cmd.Flags().GetString("filename")
	if err != nil {
		fmt.Println(errors.Trace(err))
		return
	}

	subDir, err := cmd.Flags().GetString("sub-dir")
	if err != nil {
		fmt.Println(errors.Trace(err))
		return
	}

	//var count = 0
	//if inactive {
	//	count++
	//}
	//if len(timeStr) > 0 {
	//	count++
	//	timeStr = strings.Trim(timeStr, "\"")
	//}
	if len(filename) > 0 {
		//count++
		filename = strings.Trim(filename, "\"")
	}
	//if count != 1 {
	//	fmt.Println("must specify one (and only one) of --inactive, --time, --filename")
	//	return
	//}

	//var time2 = time.Unix(0, 0) // zero time
	//if len(timeStr) > 0 {
	//	timeStr2 := strings.Replace(timeStr, "_", " ", 1)
	//	time2, err = time.ParseInLocation(timeLayout, timeStr2, time.Local)
	//	if err != nil {
	//		fmt.Printf("invalid time format %s for --time, it should be %s\n", timeStr, timeFormat)
	//		return
	//	}
	//}

	if len(filename) > 0 && len(workers) > 1 {
		fmt.Println("for --filename, can only specify one dm-worker per time")
		return
	}
	if len(subDir) > 0 {
		subDir = command.TrimQuoteMark(subDir)
	}
	if len(filename) > 0 && len(subDir) == 0 {
		fmt.Println("[warn] no --sub-dir specify for --filename, the latest one will be used")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli := common.MasterClient()

	resp, err := cli.PurgeWorkerRelay(ctx, &pb.PurgeWorkerRelayRequest{
		Workers: workers,
		//Inactive: inactive,
		//Time:     time2.Unix(),
		Filename: filename,
		SubDir:   subDir,
	})
	if err != nil {
		common.PrintLines("can not purge relay log files: \n%s", errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
