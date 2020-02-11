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

package ha

import (
	"context"
	"encoding/json"

	"go.etcd.io/etcd/clientv3"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/pkg/etcdutil"
)

// WorkerInfo represents the node information of the DM-worker.
type WorkerInfo struct {
	Name string `json:"name"` // the name of the node.
	Addr string `json:"addr"` // the client address of the node to advertise.
}

// NewWorkerInfo creates a new WorkerInfo instance.
func NewWorkerInfo(name, addr string) WorkerInfo {
	return WorkerInfo{
		Name: name,
		Addr: addr,
	}
}

// String implements Stringer interface.
func (i WorkerInfo) String() string {
	s, _ := i.toJSON()
	return s
}

// toJSON returns the string of JSON represent.
func (i WorkerInfo) toJSON() (string, error) {
	data, err := json.Marshal(i)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// infoFromJSON constructs Info from its JSON represent.
func infoFromJSON(s string) (i WorkerInfo, err error) {
	err = json.Unmarshal([]byte(s), &i)
	return
}

// PutWorkerInfo puts the DM-worker info into etcd.
// k/v: worker-name -> worker information.
func PutWorkerInfo(cli *clientv3.Client, info WorkerInfo) (int64, error) {
	value, err := info.toJSON()
	if err != nil {
		return 0, err
	}
	key := common.WorkerRegisterKeyAdapter.Encode(info.Name)

	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	resp, err := cli.Put(ctx, key, value)
	if err != nil {
		return 0, err
	}
	return resp.Header.Revision, nil
}

// GetAllWorkerInfo gets all DM-worker info in etcd currently.
// k/v: worker-name -> worker information.
func GetAllWorkerInfo(cli *clientv3.Client) (map[string]WorkerInfo, int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	resp, err := cli.Get(ctx, common.WorkerRegisterKeyAdapter.Path(), clientv3.WithPrefix())
	if err != nil {
		return nil, 0, err
	}

	ifm := make(map[string]WorkerInfo)
	for _, kv := range resp.Kvs {
		info, err2 := infoFromJSON(string(kv.Value))
		if err2 != nil {
			return nil, 0, err2
		}

		ifm[info.Name] = info
	}

	return ifm, resp.Header.Revision, nil
}

// DeleteWorkerInfo deletes the specified DM-worker information.
func DeleteWorkerInfo(cli *clientv3.Client, worker string) (int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	resp, err := cli.Delete(ctx, common.WorkerRegisterKeyAdapter.Encode(worker))
	if err != nil {
		return 0, err
	}
	return resp.Header.Revision, nil
}
