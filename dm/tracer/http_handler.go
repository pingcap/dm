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
	"strconv"

	"github.com/pingcap/errors"

	"github.com/pingcap/dm/pkg/log"
)

const (
	opQueryEvents = "query-events"
	opScanEvents  = "scan-events"
	opDelEvents   = "del-events"
	defaultLimit  = 10
)

const (
	qTraceID = "trace_id"
	qLimit   = "limit"
	qOffset  = "offset"
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
		log.Errorf("invalid json data: %v, error: %s", data, err)
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
	case opScanEvents:
		h.handleTraceEventScanRequest(w, req)
	case opDelEvents:
		h.handleTraceEventDeleteRequest(w, req)
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

func (h eventHandler) handleTraceEventScanRequest(w http.ResponseWriter, req *http.Request) {
	var (
		limit  int64 = defaultLimit
		offset int64
		err    error
	)
	offsetStr := req.FormValue(qOffset)
	if len(offsetStr) > 0 {
		offset, err = strconv.ParseInt(offsetStr, 0, 64)
		if err != nil {
			writeBadRequest(w, errors.NotValidf("offset: %s", offsetStr))
		}
	}
	limitStr := req.FormValue(qLimit)
	if len(limitStr) > 0 {
		limit, err = strconv.ParseInt(limitStr, 0, 64)
		if err != nil {
			writeBadRequest(w, errors.NotValidf("limit: %s", limitStr))
		}
	}

	events := h.scan(offset, limit)
	if events == nil {
		writeNotFound(w, errors.NotFoundf("offset: %d, limit: %d", offset, limit))
		return
	}
	writeData(w, events)
}

func (h eventHandler) handleTraceEventDeleteRequest(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		writeBadRequest(w, errors.New("post only"))
		return
	}
	if traceID := req.FormValue(qTraceID); len(traceID) > 0 {
		removed := h.removeByTraceID(traceID)
		data := map[string]interface{}{
			"trace_id": traceID,
			"result":   removed,
		}
		writeData(w, data)
	} else {
		writeBadRequest(w, errors.New("trace id not provided"))
	}
}
