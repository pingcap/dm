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
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/etcd"
	parserpkg "github.com/pingcap/dm/pkg/parser"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb-tools/pkg/utils"
	"github.com/spf13/cobra"
)

var (
	globalConfig = &Config{}

	etcdClient *etcd.Client

	defaultEtcdTimeout = time.Duration(10 * time.Second)

	// result path in etcd:
	//	response: /dm-operate/{operate-id}/response
	//  error:    /dm-operate/{operate-id}/error
	defaultOperatePath = "/dm-operate"
)

// InitUtils inits necessary dmctl utils
func InitUtils(cfg *Config) error {
	globalConfig = cfg

	err := InitEtcdClient(cfg.MasterAddr)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// InitEtcdClient initializes etcd client
func InitEtcdClient(addr string) error {
	ectdEndpoints, err := utils.ParseHostPortAddr(addr)
	if err != nil {
		return errors.Trace(err)
	}

	etcdClient, err = etcd.NewClientFromCfg(ectdEndpoints, defaultEtcdTimeout, defaultOperatePath)
	if err != nil {
		// TODO: use terror
		return errors.Trace(err)
	}

	return nil
}

// GlobalConfig returns global dmctl config
func GlobalConfig() *Config {
	return globalConfig
}

// EtcdClient retuens etcd client
func EtcdClient() *etcd.Client {
	return etcdClient
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

// GetWorkerArgs extracts workers from cmd
func GetWorkerArgs(cmd *cobra.Command) ([]string, error) {
	return cmd.Flags().GetStringSlice("worker")
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

// GetOperateID returns a unique operate id
// TODO: Operate ID should be monotone increasing, just like OpLog.ID in dm-worker
func GetOperateID() int64 {
	rand.Seed(time.Now().UnixNano())
	randomValue := uint32(rand.Intn(1000))
	return time.Now().UnixNano()/1000*1000 + int64(randomValue)
}

// SendRequest writes request to etcd, and wait for the response
func SendRequest(tp pb.CommandType, request []byte) (proto.Message, error) {
	operateID := GetOperateID()

	ctx, cancel := context.WithTimeout(context.Background(), defaultEtcdTimeout)
	defer cancel()

	operateIDStr := strconv.FormatInt(operateID, 10)

	command := pb.Command{
		Tp:      tp,
		Request: request,
	}
	cmdBytes, err := command.Marshal()
	if err != nil {
		return nil, err
	}
	revision, err := etcdClient.Create(ctx, operateIDStr, string(cmdBytes), nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	watchCh := etcdClient.Watch(ctx, operateIDStr, revision)

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case wresp := <-watchCh:
			if wresp.Err() != nil {
				return nil, wresp.Err()
			}

			for _, ev := range wresp.Events {
				command := &pb.Command{}
				err := json.Unmarshal(ev.Kv.Value, &command)
				if err != nil {
					return nil, err
				}

				return transformResponse(command.Tp, command.Response)
			}
		}
	}
}

func transformResponse(tp pb.CommandType, response []byte) (proto.Message, error) {
	switch tp {
	case pb.CommandType_MigrateWorkerRelay:
		resp := &pb.CommonWorkerResponse{}
		err := resp.Unmarshal(response)
		return resp, err
	case pb.CommandType_UpdateWorkerRelayConfig:
		resp := &pb.CommonWorkerResponse{}
		err := resp.Unmarshal(response)
		return resp, err
	case pb.CommandType_StartTask:
		resp := &pb.StartTaskResponse{}
		err := resp.Unmarshal(response)
		return resp, err
	case pb.CommandType_UpdateMasterConfig:
		resp := &pb.UpdateMasterConfigResponse{}
		err := resp.Unmarshal(response)
		return resp, err
	case pb.CommandType_OperateTask:
		resp := &pb.OperateTaskResponse{}
		err := resp.Unmarshal(response)
		return resp, err
	case pb.CommandType_UpdateTask:
		resp := &pb.UpdateTaskResponse{}
		err := resp.Unmarshal(response)
		return resp, err
	case pb.CommandType_QueryStatusList:
		resp := &pb.QueryStatusListResponse{}
		err := resp.Unmarshal(response)
		return resp, err
	case pb.CommandType_QueryErrorList:
		resp := &pb.QueryErrorResponse{}
		err := resp.Unmarshal(response)
		return resp, err
	case pb.CommandType_ShowDDLLocks:
		resp := &pb.ShowDDLLocksResponse{}
		err := resp.Unmarshal(response)
		return resp, err
	case pb.CommandType_UnlockDDLLock:
		resp := &pb.UnlockDDLLockResponse{}
		err := resp.Unmarshal(response)
		return resp, err
	case pb.CommandType_BreakWorkerDDLLock:
		resp := &pb.BreakWorkerDDLLockResponse{}
		err := resp.Unmarshal(response)
		return resp, err
	case pb.CommandType_SwitchWorkerRelayMaster:
		resp := &pb.SwitchWorkerRelayMasterResponse{}
		err := resp.Unmarshal(response)
		return resp, err
	case pb.CommandType_OperateWorkerRelay:
		resp := &pb.OperateWorkerRelayResponse{}
		err := resp.Unmarshal(response)
		return resp, err
	case pb.CommandType_RefreshWorkerTasks:
		resp := &pb.RefreshWorkerTasksResponse{}
		err := resp.Unmarshal(response)
		return resp, err
	case pb.CommandType_HandleSQLs:
		resp := &pb.HandleSQLsResponse{}
		err := resp.Unmarshal(response)
		return resp, err
	case pb.CommandType_PurgeWorkerRelay:
		resp := &pb.PurgeWorkerRelayResponse{}
		err := resp.Unmarshal(response)
		return resp, err
	case pb.CommandType_CheckTask:
		resp := &pb.CheckTaskResponse{}
		err := resp.Unmarshal(response)
		return resp, err
	default:
		// return error
		return nil, nil
	}
}
