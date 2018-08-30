// Copyright 2018 PingCAP, Inc.
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
	"net/http"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb-enterprise-tools/pkg/utils"

	// used to enable pprof
	_ "net/http/pprof"
)

// InitStatus initializes the HTTP status server
func InitStatus(addr string) {
	go func() {
		http.HandleFunc("/status", func(w http.ResponseWriter, req *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			text := utils.GetRawInfo()
			w.Write([]byte(text))
		})

		log.Infof("listening on %v for status report.", addr)
		err := http.ListenAndServe(addr, nil)
		if err != nil {
			log.Fatal(err)
		}
	}()
}
