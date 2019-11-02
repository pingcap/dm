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
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"go.etcd.io/etcd/embed"
	"google.golang.org/grpc"
)

const (
	etcdDataDir       = "dm.etcd"
	defaultLpUrl      = "http://127.0.0.1:8269" // TODO: make it as config item.
	defaultClusterKey = "dm-master"             // TODO: adjust in config
)

// startEtcd starts an embedded etcd server.
// reuse
func startEtcd(lcUrl string, lpUrl string,
	gRPCSvr func(*grpc.Server),
	httpHandles map[string]http.Handler) (*embed.Etcd, error) {
	cfg := embed.NewConfig()
	cfg.Dir = etcdDataDir

	// we only use one client listening URL now
	lcUrl2, err := parseUrls(lcUrl)
	if err != nil {
		return nil, err
	}
	cfg.LCUrls = lcUrl2
	cfg.ACUrls = lcUrl2

	//if lpUrl == "" {
	//	lpUrl = defaultLpUrl
	//}
	//lpUrl2, err := parseUrls(lpUrl)
	//if err != nil {
	//	return nil, err
	//}
	//cfg.LPUrls = lpUrl2
	//cfg.APUrls = lpUrl2
	//
	//cfg.InitialCluster = initialClusterFromLpUrls(lpUrl)

	// attach extra gRPC and HTTP server
	if gRPCSvr != nil {
		cfg.ServiceRegister = gRPCSvr
	}
	if httpHandles != nil {
		cfg.UserHandlers = httpHandles
	}

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, err
	}

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(time.Minute):
		e.Server.Stop()
		e.Close()
		return nil, errors.New("start etcd server timeout")
	}
	return e, nil
}

// parseUrls parse a string into multiple urls.
// if the URL in the string without protocol scheme, use `http` as the default.
// if no IP exists in the address, `0.0.0.0` is used.
func parseUrls(s string) ([]url.URL, error) {
	items := strings.Split(s, ",")
	urls := make([]url.URL, 0, len(items))
	for _, item := range items {
		u, err := url.Parse(item)
		if err != nil && strings.Contains(err.Error(), "missing protocol scheme") {
			u, err = url.Parse("http://" + item)
		}
		if err != nil {
			return nil, err
		}
		if strings.Index(u.Host, ":") == 0 {
			u.Host = "0.0.0.0" + u.Host
		}
		urls = append(urls, *u)
	}
	return urls, nil
}

// initialClusterFromLpUrls gets `--initial-cluster` from lp URLs.
func initialClusterFromLpUrls(lpUrls string) string {
	items := strings.Split(lpUrls, ",")
	for i, item := range items {
		items[i] = fmt.Sprintf("%s=%s", defaultClusterKey, item)
	}
	return strings.Join(items, ",")
}
