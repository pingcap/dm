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
	"net"
	"net/http"
	"net/http/pprof"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/utils"
)

func (s *Server) startHTTPServer(lis net.Listener) {
	router := http.NewServeMux()

	router.HandleFunc("/status", s.handleStatus)
	router.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	router.HandleFunc("/debug/pprof/profile", pprof.Profile)
	router.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	router.HandleFunc("/debug/pprof/trace", pprof.Trace)

	router.Handle("/events/query", eventHandler{s.eventStore, opQueryEvents})
	router.Handle("/events/scan", eventHandler{s.eventStore, opScanEvents})
	router.Handle("/events/delete", eventHandler{s.eventStore, opDelEvents})
	router.Handle("/events/truncate", eventHandler{s.eventStore, opTruncateEvents})

	httpServer := &http.Server{
		Handler: router,
	}
	err := httpServer.Serve(lis)
	if err != nil && !common.IsErrNetClosing(err) && err != http.ErrServerClosed {
		log.L().Error("tracer http server return with error", log.ShortError(err))
	}
}

// status of tracing service.
type status struct {
	Version string `json:"version"`
	GitHash string `json:"git_hash"`
}

func (s *Server) handleStatus(w http.ResponseWriter, req *http.Request) {
	st := status{
		Version: utils.ReleaseVersion,
		GitHash: utils.GitHash,
	}
	writeData(w, st)
}
