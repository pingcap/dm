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
	"net/http"

	"github.com/pingcap/errors"

	"github.com/pingcap/dm/pkg/log"
)

const (
	opQueryEvents = "query-events"
	opScanEvents  = "scan-events"
	opDelEvents   = "del-events"
)

const (
	qTraceID = "trace_id"
)

const (
	headerContentType = "Content-Type"
	contentTypeJSON   = "application/json"
)

type eventHandler struct {
	*EventStore
	op string
}

func writeInternalServerError(w http.ResponseWriter, err error) {
	writeError(w, http.StatusInternalServerError, err)
}

func writeBadRequest(w http.ResponseWriter, err error) {
	writeError(w, http.StatusBadRequest, err)
}

func writeNotFound(w http.ResponseWriter, err error) {
	writeError(w, http.StatusNotFound, err)
}

func writeError(w http.ResponseWriter, statusCode int, err error) {
	w.WriteHeader(statusCode)
	_, err = w.Write([]byte(err.Error()))
	if err != nil {
		log.Error(errors.ErrorStack(err))
	}
}

func writeData(w http.ResponseWriter, data interface{}) {
	js, err := json.MarshalIndent(data, "", " ")
	if err != nil {
		writeInternalServerError(w, err)
		return
	}
	log.Debug(string(js))
	w.Header().Set(headerContentType, contentTypeJSON)
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(js)
	if err != nil {
		log.Error(errors.ErrorStack(err))
	}
}

func (h eventHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch h.op {
	case opQueryEvents:
		h.handleTraceEventQueryRequest(w, req)
	}
}

func (h eventHandler) handleTraceEventQueryRequest(w http.ResponseWriter, req *http.Request) {
	if traceID := req.FormValue(qTraceID); len(traceID) > 0 {
		events := h.queryByTraceID(traceID)
		if events == nil {
			writeNotFound(w, errors.NotFoundf("trace event %s", traceID))
			return
		}
		writeData(w, events)
	} else {
		writeBadRequest(w, errors.New("trace id not provided"))
	}
}
