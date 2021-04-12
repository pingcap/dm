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

package worker

import (
	"context"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	cpu "github.com/pingcap/tidb-tools/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/dumpling"
	"github.com/pingcap/dm/loader"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/metricsproxy"
	"github.com/pingcap/dm/pkg/utils"
	"github.com/pingcap/dm/relay"
	"github.com/pingcap/dm/syncer"
)

const (
	opErrTypeBeforeOp    = "BeforeAnyOp"
	opErrTypeSourceBound = "SourceBound"
	opErrTypeRelaySource = "RelaySource"
)

var (
	taskState = metricsproxy.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "worker",
			Name:      "task_state",
			Help:      "state of task, 0 - invalidStage, 1 - New, 2 - Running, 3 - Paused, 4 - Stopped, 5 - Finished",
		}, []string{"task", "source_id"})

	// opErrCounter cleans on worker close, which is the same time dm-worker exits, so no explicit clean
	opErrCounter = metricsproxy.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "worker",
			Name:      "operate_error",
			Help:      "number of different operate error",
		}, []string{"worker", "type"})

	cpuUsageGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "worker",
			Name:      "cpu_usage",
			Help:      "the cpu usage of worker",
		})
)

type statusHandler struct {
}

func (h *statusHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	text := utils.GetRawInfo()
	_, err := w.Write([]byte(text))
	if err != nil && !common.IsErrNetClosing(err) {
		log.L().Error("fail to write status response", log.ShortError(err))
	}
}

// Note: handle error inside the function with returning it.
func (s *Server) collectMetrics() {
	// CPU usage metric
	cpuUsage := cpu.GetCPUPercentage()
	cpuUsageGauge.Set(cpuUsage)
}

func (s *Server) runBackgroundJob(ctx context.Context) {
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.collectMetrics()

		case <-ctx.Done():
			return
		}
	}
}

// RegistryMetrics registries metrics for worker
func RegistryMetrics() {
	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	registry.MustRegister(prometheus.NewGoCollector())

	registry.MustRegister(cpuUsageGauge)

	registry.MustRegister(taskState)
	registry.MustRegister(opErrCounter)

	relay.RegisterMetrics(registry)
	dumpling.RegisterMetrics(registry)
	loader.RegisterMetrics(registry)
	syncer.RegisterMetrics(registry)
	prometheus.DefaultGatherer = registry
}

// InitStatus initializes the HTTP status server
func InitStatus(lis net.Listener) {
	mux := http.NewServeMux()
	mux.Handle("/status", &statusHandler{})
	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	httpS := &http.Server{
		Handler: mux,
	}
	err := httpS.Serve(lis)
	if err != nil && !common.IsErrNetClosing(err) && err != http.ErrServerClosed {
		log.L().Error("status server returned", log.ShortError(err))
	}
}

func (st *SubTask) removeLabelValuesWithTaskInMetrics(task string, source string) {
	taskState.DeleteAllAboutLabels(prometheus.Labels{"task": task, "source_id": source})
}
