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

	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
)

const (
	opQueryEvents    = "query-events"
	opScanEvents     = "scan-events"
	opDelEvents      = "del-events"
	opTruncateEvents = "truncate-events"
	defaultLimit     = 10
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
		log.L().Error("write error", zap.Error(err))
	}
}

func writeData(w http.ResponseWriter, data interface{}) {
	js, err := json.MarshalIndent(data, "", " ")
	if err != nil {
		log.L().Error("invalid json data", zap.Reflect("data", data), zap.Error(err))
		writeInternalServerError(w, err)
		return
	}
	log.L().Debug("write data", zap.ByteString("request data", js))
	w.Header().Set(headerContentType, contentTypeJSON)
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(js)
	if err != nil {
		log.L().Error("fail to write data", zap.Error(err))
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
	case opTruncateEvents:
		h.handleTraceEventTruncateRequest(w, req)
	}
}

func (h eventHandler) handleTraceEventQueryRequest(w http.ResponseWriter, req *http.Request) {
	if traceID := req.FormValue(qTraceID); len(traceID) > 0 {
		events := h.queryByTraceID(traceID)
		if events == nil {
			writeNotFound(w, terror.ErrTracerTraceEventNotFound.Generate(traceID))
			return
		}
		writeData(w, events)
	} else {
		writeBadRequest(w, terror.ErrTracerTraceIDNotProvided.Generate())
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
			writeBadRequest(w, terror.ErrTracerParamNotValid.Generate("offset", offsetStr))
		}
	}
	limitStr := req.FormValue(qLimit)
	if len(limitStr) > 0 {
		limit, err = strconv.ParseInt(limitStr, 0, 64)
		if err != nil {
			writeBadRequest(w, terror.ErrTracerParamNotValid.Generate("limit", limitStr))
		}
	}

	events := h.scan(offset, limit)
	if events == nil {
		writeNotFound(w, terror.ErrTracerTraceEventNotFound.Generatef("offset: %d, limit: %d not found", offset, limit))
		return
	}
	writeData(w, events)
}

func (h eventHandler) handleTraceEventDeleteRequest(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		writeBadRequest(w, terror.ErrTracerPostMethodOnly.Generate())
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
		writeBadRequest(w, terror.ErrTracerTraceIDNotProvided.Generate())
	}
}

func (h eventHandler) handleTraceEventTruncateRequest(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		writeBadRequest(w, terror.ErrTracerPostMethodOnly.Generate())
		return
	}
	h.truncate()
	writeData(w, map[string]interface{}{"result": true})
}
