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
	"net"
	"net/http"

	"github.com/ngaut/log"
	"github.com/soheilhy/cmux"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/pkg/utils"
)

type statusHandler struct {
}

func (h *statusHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	text := utils.GetRawInfo()
	_, err := w.Write([]byte(text))
	if err != nil && !common.IsErrNetClosing(err) {
		log.Errorf("[server] write status response error %s", err.Error())
	}
}

// InitStatus initializes the HTTP status server
func InitStatus(lis net.Listener) {
	mux := http.NewServeMux()
	mux.Handle("/status", &statusHandler{})
	httpS := &http.Server{
		Handler: mux,
	}
	err := httpS.Serve(lis)
	if err != nil && !common.IsErrNetClosing(err) && err != cmux.ErrListenerClosed {
		log.Errorf("[server] status server return with error %s", err.Error())
	}
}
