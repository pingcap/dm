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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/pingcap/dm/dm/pb"
	parserpkg "github.com/pingcap/dm/pkg/parser"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

var (
	masterClient pb.MasterClient
	globalConfig = &Config{}
)

// InitUtils inits necessary dmctl utils
func InitUtils(cfg *Config) error {
	globalConfig = cfg
	return errors.Trace(InitClient(cfg.MasterAddr))
}

// InitClient initializes dm-master client
func InitClient(addr string) error {
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBackoffMaxDelay(3*time.Second))
	if err != nil {
		return errors.Trace(err)
	}
	masterClient = pb.NewMasterClient(conn)
	return nil
}

// GlobalConfig returns global dmctl config
func GlobalConfig() *Config {
	return globalConfig
}

// MasterClient returns dm-master client
func MasterClient() pb.MasterClient {
	return masterClient
}

// PrintLines adds a wrap to support `\n` within `chzyer/readline`
func PrintLines(format string, a ...interface{}) {
	fmt.Println(fmt.Sprintf(format, a...))
}

// PrettyPrintResponse prints a PRC response prettily
func PrettyPrintResponse(resp proto.Message) {
	s, err := marshResponseToString(resp)
	if err != nil {
		PrintLines(errors.ErrorStack(err))
	} else {
		fmt.Println(s)
	}
}

// PrettyPrintInterface prints an interface through encoding/json prettily
func PrettyPrintInterface(resp interface{}) {
	s, err := json.MarshalIndent(resp, "", "    ")
	if err != nil {
		PrintLines(errors.ErrorStack(err))
	} else {
		fmt.Println(string(s))
	}
}

func marshResponseToString(resp proto.Message) (string, error) {
	// encoding/json does not support proto Enum well
	mar := jsonpb.Marshaler{EmitDefaults: true, Indent: "    "}
	s, err := mar.MarshalToString(resp)
	return s, errors.Trace(err)
}

// PrettyPrintResponseWithCheckTask prints a RPC response may contain response Msg with check-task's response prettily.
// check-task's response may contain json-string when checking fail in `detail` field.
// ugly code, but it is a little hard to refine this because needing to convert type.
func PrettyPrintResponseWithCheckTask(resp proto.Message, subStr string) bool {
	var (
		err          error
		found        bool
		replacedStr  string
		marshaledStr string
		placeholder  = "PLACEHOLDER"
	)
	switch chr := resp.(type) {
	case *pb.StartTaskResponse:
		if strings.Contains(chr.Msg, subStr) {
			found = true
			rawMsg := chr.Msg
			chr.Msg = placeholder // replace Msg with placeholder
			marshaledStr, err = marshResponseToString(chr)
			if err == nil {
				replacedStr = strings.Replace(marshaledStr, placeholder, rawMsg, 1)
			}
		}
	case *pb.UpdateTaskResponse:
		if strings.Contains(chr.Msg, subStr) {
			found = true
			rawMsg := chr.Msg
			chr.Msg = placeholder // replace Msg with placeholder
			marshaledStr, err = marshResponseToString(chr)
			if err == nil {
				replacedStr = strings.Replace(marshaledStr, placeholder, rawMsg, 1)
			}
		}
	case *pb.CheckTaskResponse:
		if strings.Contains(chr.Msg, subStr) {
			found = true
			rawMsg := chr.Msg
			chr.Msg = placeholder // replace Msg with placeholder
			marshaledStr, err = marshResponseToString(chr)
			if err == nil {
				replacedStr = strings.Replace(marshaledStr, placeholder, rawMsg, 1)
			}
		}

	default:
		return false
	}

	if !found {
		return found
	}

	if err != nil {
		fmt.Println(errors.ErrorStack(err))
	} else {
		// add indent to make it prettily.
		replacedStr = strings.Replace(replacedStr, "detail: {", "   \tdetail: {", 1)
		fmt.Println(replacedStr)
	}
	return found
}

// GetFileContent reads and returns file's content
func GetFileContent(fpath string) ([]byte, error) {
	content, err := ioutil.ReadFile(fpath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return content, nil
}

// GetSourceArgs extracts sources from cmd
func GetSourceArgs(cmd *cobra.Command) ([]string, error) {
	return cmd.Flags().GetStringSlice("source")
}

// ExtractSQLsFromArgs extract multiple sql from args.
func ExtractSQLsFromArgs(args []string) ([]string, error) {
	if len(args) <= 0 {
		return nil, errors.New("args is empty")
	}

	concat := strings.TrimSpace(strings.Join(args, " "))

	parser := parser.New()
	nodes, err := parserpkg.Parse(parser, concat, "", "")
	if err != nil {
		return nil, errors.Annotatef(err, "invalid sql '%s'", concat)
	}
	realSQLs := make([]string, 0, len(nodes))
	for _, node := range nodes {
		realSQLs = append(realSQLs, node.Text())
	}
	if len(realSQLs) == 0 {
		return nil, errors.New("no valid SQLs")
	}

	return realSQLs, nil
}

// IsDDL tests whether the input is a valid DDL statement
func IsDDL(sql string) (bool, error) {
	parser2 := parser.New()
	node, err := parser2.ParseOneStmt(sql, "", "")
	if err != nil {
		return false, errors.Annotatef(err, "invalid sql '%s'", sql)
	}

	switch node.(type) {
	case ast.DDLNode:
		return true, nil
	default:
		return false, nil
	}
}
