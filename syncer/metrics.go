// Copyright 2017 PingCAP, Inc.
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

package syncer

import (
	"net/http"
	"strings"
	// For pprof
	_ "net/http/pprof"
	"strconv"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb-enterprise-tools/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	binlogEventsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "syncer",
			Name:      "binlog_events_total",
			Help:      "total number of binlog events",
		}, []string{"type"})

	binlogSkippedEventsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "syncer",
			Name:      "binlog_skipped_events_total",
			Help:      "total number of skipped binlog events",
		}, []string{"type"})

	sqlJobsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "syncer",
			Name:      "sql_jobs_total",
			Help:      "total number of sql jobs",
		}, []string{"type"})

	sqlRetriesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "syncer",
			Name:      "sql_retries_total",
			Help:      "total number of sql retryies",
		}, []string{"type"})

	binlogPos = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "syncer",
			Name:      "binlog_pos",
			Help:      "current binlog pos",
		}, []string{"node"})

	binlogFile = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "syncer",
			Name:      "binlog_file",
			Help:      "current binlog file index",
		}, []string{"node"})

	binlogGTID = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "syncer",
			Name:      "gtid",
			Help:      "current transaction id",
		}, []string{"node"})

	txnCostGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "syncer",
			Name:      "txn_costs_gauge_in_second",
			Help:      "transaction costs gauge in second",
		})
)

// InitStatusAndMetrics register prometheus metrics and listen for status port.
func InitStatusAndMetrics(addr string) {
	prometheus.MustRegister(binlogEventsTotal)
	prometheus.MustRegister(binlogSkippedEventsTotal)
	prometheus.MustRegister(sqlJobsTotal)
	prometheus.MustRegister(sqlRetriesTotal)
	prometheus.MustRegister(binlogPos)
	prometheus.MustRegister(binlogFile)
	prometheus.MustRegister(binlogGTID)
	prometheus.MustRegister(txnCostGauge)

	go func() {
		http.HandleFunc("/status", func(w http.ResponseWriter, req *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			text := utils.GetRawInfo()
			w.Write([]byte(text))
		})

		// HTTP path for prometheus.
		http.Handle("/metrics", prometheus.Handler())
		log.Infof("listening on %v for status and metrics report.", addr)
		err := http.ListenAndServe(addr, nil)
		if err != nil {
			log.Fatal(err)
		}
	}()
}

func getBinlogIndex(filename string) float64 {
	spt := strings.Split(filename, ".")
	if len(spt) == 1 {
		return 0
	}
	idxStr := spt[len(spt)-1]

	idx, err := strconv.ParseFloat(idxStr, 64)
	if err != nil {
		log.Warnf("[syncer] parse binlog index %s, error %s", filename, err)
		return 0
	}
	return idx
}
