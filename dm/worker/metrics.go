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

package worker

import (
	"net"
	"net/http"

	"github.com/ngaut/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/soheilhy/cmux"

	"github.com/pingcap/tidb-enterprise-tools/dm/common"
	"github.com/pingcap/tidb-enterprise-tools/loader"
	"github.com/pingcap/tidb-enterprise-tools/mydumper"
	"github.com/pingcap/tidb-enterprise-tools/pkg/utils"
	"github.com/pingcap/tidb-enterprise-tools/relay"
	"github.com/pingcap/tidb-enterprise-tools/syncer"
)

var (
	taskState = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "worker",
			Name:      "task_state",
			Help:      "state of task, 0 - invalidStage, 1 - New, 2 - Running, 3 - Paused, 4 - Stopped, 5 - Finished",
		}, []string{"task"})
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
	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	registry.MustRegister(prometheus.NewGoCollector())

	registry.MustRegister(taskState)

	relay.RegisterMetrics(registry)
	mydumper.RegisterMetrics(registry)
	loader.RegisterMetrics(registry)
	syncer.RegisterMetrics(registry)
	prometheus.DefaultGatherer = registry

	mux := http.NewServeMux()
	mux.Handle("/status", &statusHandler{})
	mux.Handle("/metrics", prometheus.Handler())

	httpS := &http.Server{
		Handler: mux,
	}
	err := httpS.Serve(lis)
	if err != nil && !common.IsErrNetClosing(err) && err != cmux.ErrListenerClosed {
		log.Errorf("[server] status server return with error %s", err.Error())
	}
}
