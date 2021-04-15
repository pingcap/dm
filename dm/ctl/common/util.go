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
	"reflect"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/clientv3"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	parserpkg "github.com/pingcap/dm/pkg/parser"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	toolutils "github.com/pingcap/tidb-tools/pkg/utils"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	globalConfig = &Config{}
	ctlClient    = &CtlClient{}
)

// CtlClient used to get master client for dmctl.
type CtlClient struct {
	mu           sync.RWMutex
	tls          *toolutils.TLS
	etcdClient   *clientv3.Client
	conn         *grpc.ClientConn
	masterClient pb.MasterClient
}

func (c *CtlClient) updateMasterClient() error {
	var (
		err  error
		conn *grpc.ClientConn
	)

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		c.conn.Close()
	}

	endpoints := c.etcdClient.Endpoints()
	for _, endpoint := range endpoints {
		//nolint:staticcheck
		conn, err = grpc.Dial(utils.UnwrapScheme(endpoint), c.tls.ToGRPCDialOption(), grpc.WithBackoffMaxDelay(3*time.Second), grpc.WithBlock(), grpc.WithTimeout(3*time.Second))
		if err == nil {
			c.conn = conn
			c.masterClient = pb.NewMasterClient(conn)
			return nil
		}
	}
	return terror.ErrCtlGRPCCreateConn.AnnotateDelegate(err, "can't connect to %s", strings.Join(endpoints, ","))
}

func (c *CtlClient) sendRequest(ctx context.Context, reqName string, req interface{}, respPointer interface{}) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	params := []reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(req)}
	results := reflect.ValueOf(c.masterClient).MethodByName(reqName).Call(params)

	reflect.ValueOf(respPointer).Elem().Set(results[0])
	errInterface := results[1].Interface()
	// nil can't pass type conversion, so we handle it separately
	if errInterface == nil {
		return nil
	}
	return errInterface.(error)
}

// SendRequest send request to master.
func SendRequest(ctx context.Context, reqName string, req interface{}, respPointer interface{}) error {
	err := ctlClient.sendRequest(ctx, reqName, req, respPointer)
	if err == nil || status.Code(err) != codes.Unavailable {
		return err
	}

	// update master client
	err = ctlClient.updateMasterClient()
	if err != nil {
		return err
	}

	// sendRequest again
	return ctlClient.sendRequest(ctx, reqName, req, respPointer)
}

// InitUtils inits necessary dmctl utils.
func InitUtils(cfg *Config) error {
	globalConfig = cfg
	return errors.Trace(InitClient(cfg.MasterAddr, cfg.Security))
}

// InitClient initializes dm-master client.
func InitClient(addr string, securityCfg config.Security) error {
	tls, err := toolutils.NewTLS(securityCfg.SSLCA, securityCfg.SSLCert, securityCfg.SSLKey, "", securityCfg.CertAllowedCN)
	if err != nil {
		return terror.ErrCtlInvalidTLSCfg.Delegate(err)
	}

	endpoints := strings.Split(addr, ",")
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:            endpoints,
		DialTimeout:          dialTimeout,
		DialKeepAliveTime:    keepaliveTime,
		DialKeepAliveTimeout: keepaliveTimeout,
		TLS:                  tls.TLSConfig(),
	})
	if err != nil {
		return err
	}

	ctlClient = &CtlClient{
		tls:        tls,
		etcdClient: etcdClient,
	}

	return ctlClient.updateMasterClient()
}

// GlobalConfig returns global dmctl config.
func GlobalConfig() *Config {
	return globalConfig
}

// PrintLinesf adds a wrap to support `\n` within `chzyer/readline`.
func PrintLinesf(format string, a ...interface{}) {
	fmt.Println(fmt.Sprintf(format, a...))
}

// PrettyPrintResponse prints a PRC response prettily.
func PrettyPrintResponse(resp proto.Message) {
	s, err := marshResponseToString(resp)
	if err != nil {
		PrintLinesf("%v", err)
	} else {
		fmt.Println(s)
	}
}

// PrettyPrintInterface prints an interface through encoding/json prettily.
func PrettyPrintInterface(resp interface{}) {
	s, err := json.MarshalIndent(resp, "", "    ")
	if err != nil {
		PrintLinesf("%v", err)
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
		PrintLinesf("%v", err)
	} else {
		// add indent to make it prettily.
		replacedStr = strings.Replace(replacedStr, "detail: {", "   \tdetail: {", 1)
		fmt.Println(replacedStr)
	}
	return found
}

// GetFileContent reads and returns file's content.
func GetFileContent(fpath string) ([]byte, error) {
	content, err := ioutil.ReadFile(fpath)
	if err != nil {
		return nil, errors.Annotate(err, "error in get file content")
	}
	return content, nil
}

// GetSourceArgs extracts sources from cmd.
func GetSourceArgs(cmd *cobra.Command) ([]string, error) {
	ret, err := cmd.Flags().GetStringSlice("source")
	if err != nil {
		PrintLinesf("error in parse `-s` / `--source`")
	}
	return ret, err
}

// ExtractSQLsFromArgs extract multiple sql from args.
func ExtractSQLsFromArgs(args []string) ([]string, error) {
	if len(args) == 0 {
		return nil, errors.New("args is empty")
	}

	concat := strings.TrimSpace(strings.Join(args, " "))
	concat = utils.TrimQuoteMark(concat)

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

// GetTaskNameFromArgOrFile tries to retrieve name from the file if arg is yaml-filename-like, otherwise returns arg directly.
func GetTaskNameFromArgOrFile(arg string) string {
	if !(strings.HasSuffix(arg, ".yaml") || strings.HasSuffix(arg, ".yml")) {
		return arg
	}
	var (
		content []byte
		err     error
	)
	if content, err = GetFileContent(arg); err != nil {
		return arg
	}
	cfg := config.NewTaskConfig()
	if err := cfg.Decode(string(content)); err != nil {
		return arg
	}
	return cfg.Name
}

// PrintCmdUsage prints the usage of the command.
func PrintCmdUsage(cmd *cobra.Command) {
	if err := cmd.Usage(); err != nil {
		fmt.Println("can't output command's usage:", err)
	}
}

// SyncMasterEndpoints sync masters' endpoints.
func SyncMasterEndpoints(ctx context.Context) {
	lastClientUrls := []string{}
	clientURLs := []string{}
	updateF := func() {
		clientURLs = clientURLs[:0]
		resp, err := ctlClient.etcdClient.MemberList(ctx)
		if err != nil {
			return
		}

		for _, m := range resp.Members {
			clientURLs = append(clientURLs, m.GetClientURLs()...)
		}
		if utils.NonRepeatStringsEqual(clientURLs, lastClientUrls) {
			return
		}
		ctlClient.etcdClient.SetEndpoints(clientURLs...)
		lastClientUrls = make([]string, len(clientURLs))
		copy(lastClientUrls, clientURLs)
	}

	for {
		updateF()

		select {
		case <-ctx.Done():
			return
		case <-time.After(syncMasterEndpointsTime):
		}
	}
}
