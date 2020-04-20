// Copyright © 2020 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package master

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/terror"
)

type relayResult struct {
	Result     bool        `json:"result"`
	Msg        string      `json:"msg"`
	RelayInfos []relayInfo `json:"relayInfos"`
}

type relayInfo struct {
	Source      string            `json:"source"`
	Worker      string            `json:"worker"`
	Result      *pb.ProcessResult `json:"result"`
	RelayStatus *pb.RelayStatus   `json:"relayStatus"`
	RelayError  *pb.RelayError    `json:"relayError"`
}

// NewQueryRelayCmd represents the queryRelay command
func NewQueryRelayCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "query-relay",
		Short: "query relay status",
		Run:   queryRelayFunc,
	}
	return cmd
}

func queryRelayFunc(cmd *cobra.Command, args []string) {
	// TODO
	sources, err := common.GetSourceArgs(cmd)
	if err != nil {
		common.PrintLines("%s", terror.Message(err))
		return
	}

	cli := common.MasterClient()
	ctx, cancel := context.WithTimeout(context.Background(), common.GlobalConfig().RPCTimeout)
	defer cancel()
	resp, err := cli.QueryRelay(ctx, &pb.QueryRelayListRequest{
		Sources: sources,
	})
	if err != nil {
		common.PrintLines("can not query relay status(in sources %v):\n%s", sources, terror.Message(err))
		return
	}

	result := wrapRelayResult(resp)
	common.PrettyPrintInterface(result)
}

func wrapRelayResult(resp *pb.QueryRelayListResponse) *relayResult {
	// TODO
	relayResult := relayResult{
		Result:     resp.Result,
		Msg:        resp.Msg,
		RelayInfos: []relayInfo{},
	}

	relayInfos := []relayInfo{}
	for _, source := range resp.Sources {
		if source.SourceStatus != nil {
			relayInfo := relayInfo{
				Source:      source.SourceStatus.Source,
				Worker:      source.SourceStatus.Worker,
				Result:      source.SourceStatus.Result,
				RelayStatus: source.SourceStatus.RelayStatus,
				RelayError:  source.SourceError.RelayError,
			}
			relayInfos = append(relayInfos, relayInfo)
		}
	}
	relayResult.RelayInfos = relayInfos

	return &relayResult
}
