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

package tracer

import (
	"encoding/json"
	"net"
	"net/http"

	"github.com/pingcap/errors"
	"github.com/soheilhy/cmux"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/utils"
)

const defaultStatusAddr = ":8263"

func (s *Server) startHTTPServer(lis net.Listener) {
	router := http.NewServeMux()

	router.HandleFunc("/status", s.handleStatus)

	router.Handle("/events/query", eventHandler{s.eventStore, opQueryEvents})
	router.Handle("/events/scan", eventHandler{s.eventStore, opScanEvents})
	router.Handle("/events/delete", eventHandler{s.eventStore, opDelEvents})

	httpServer := &http.Server{
		Handler: router,
	}
	err := httpServer.Serve(lis)
	if err != nil && common.IsErrNetClosing(err) && err != http.ErrServerClosed {
		log.Errorf("[server] tracer server return with error %s", err.Error())
	}
}

// status of tracing service.
type status struct {
	Version string `json:"version"`
	GitHash string `json:"git_hash"`
}

func (s *Server) handleStatus(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	st := status{
		Version: utils.ReleaseVersion,
		GitHash: utils.GitHash,
	}
	js, err := json.Marshal(st)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Error("Encode json error", err)
	} else {
		_, err = w.Write(js)
		if err != nil {
			log.Error(errors.ErrorStack(err))
		}
	}
}
