// Copyright 2021 PingCAP, Inc.
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

// this file implement all of the APIs of the DataMigration service.

package master

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/deepmap/oapi-codegen/pkg/middleware"
	"github.com/getkin/kin-openapi/openapi3"
	"github.com/labstack/echo/v4"
	echomiddleware "github.com/labstack/echo/v4/middleware"
	"go.uber.org/zap"

	"github.com/pingcap/dm/openapi"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
)

const (
	docBasePath     = "/api/v1/docs"
	docJSONBasePath = "/api/v1/dm.json"
)

// InitOpenAPIHandles init openapi handlers.
func (s *Server) InitOpenAPIHandles() {
	swagger, err := openapi.GetSwagger()
	checkServerErr(err)
	swagger.AddServer(&openapi3.Server{URL: fmt.Sprintf("http://%s", s.cfg.AdvertiseAddr)})
	swaggerJSON, err := swagger.MarshalJSON()
	checkServerErr(err)
	docMW, err := openapi.NewSwaggerDocUI(openapi.NewSwaggerConfig(docBasePath, docJSONBasePath, ""), swaggerJSON)
	checkServerErr(err)
	e := echo.New()
	// inject err handler
	e.HTTPErrorHandler = terrorHTTPErrorHandler
	// middlewares
	e.Use(docMW)
	// set logger
	logger := log.L().Logger
	logger = logger.With(zap.String("component", "openapi"))
	e.Use(openapi.ZapLogger(logger))
	e.Use(echomiddleware.Recover())
	// disables swagger server name validation. it seems to work poorly
	swagger.Servers = nil
	// use our validation middleware to check all requests against the OpenAPI schema.
	e.Use(middleware.OapiRequestValidator(swagger))
	openapi.RegisterHandlers(e, s)
	s.echo = e
}

// redirectRequestToLeader is used to redirect the request to leader.
// because the leader has some data in memory, only the leader can process the request.
func (s *Server) redirectRequestToLeader(ctx context.Context) (needRedirect bool, host string, err error) {
	isLeader, _ := s.isLeaderAndNeedForward(ctx)
	if isLeader {
		return false, s.cfg.AdvertiseAddr, nil
	}
	// nolint:dogsled
	_, _, leaderOpenAPIAddr, err := s.election.LeaderInfo(ctx)
	return true, leaderOpenAPIAddr, err
}

// DMAPICreateSource url is:(POST /api/v1/sources).
func (s *Server) DMAPICreateSource(ctx echo.Context) error {
	return nil
}

// DMAPIGetSourceList url is:(GET /api/v1/sources).
func (s *Server) DMAPIGetSourceList(ctx echo.Context) error {
	return nil
}

// DMAPIDeleteSource url is:(DELETE /api/v1/sources).
func (s *Server) DMAPIDeleteSource(ctx echo.Context, sourceName string) error {
	return nil
}

// DMAPIStartRelay url is:(POST /api/v1/sources/{source-id}/relay).
func (s *Server) DMAPIStartRelay(ctx echo.Context, sourceName string) error {
	return nil
}

// DMAPIStopRelay url is:(DELETE /api/v1/sources/{source-id}/relay).
func (s *Server) DMAPIStopRelay(ctx echo.Context, sourceName string) error {
	return nil
}

// DMAPIGetSourceStatus url is:(GET /api/v1/sources/{source-id}/status).
func (s *Server) DMAPIGetSourceStatus(ctx echo.Context, sourceName string) error {
	return nil
}

// DMAPIStartTask url is:(POST /api/v1/tasks).
func (s *Server) DMAPIStartTask(ctx echo.Context) error {
	return nil
}

// DMAPIDeleteTask url is:(DELETE /api/v1/tasks).
func (s *Server) DMAPIDeleteTask(ctx echo.Context, taskName string) error {
	return nil
}

// DMAPIGetTaskList url is:(GET /api/v1/tasks).
func (s *Server) DMAPIGetTaskList(ctx echo.Context) error {
	return nil
}

// DMAPIGetTaskStatus url is:(GET /api/v1/tasks/{task-name}/status).
func (s *Server) DMAPIGetTaskStatus(ctx echo.Context, taskName string) error {
	return nil
}

func terrorHTTPErrorHandler(err error, c echo.Context) {
	var code int
	var msg string
	if tErr, ok := err.(*terror.Error); ok {
		code = int(tErr.Code())
		msg = tErr.Error()
	} else {
		msg = err.Error()
	}
	if sendErr := sendHTTPErrorResp(c, code, msg); sendErr != nil {
		c.Logger().Error(sendErr)
	}
}

func sendHTTPErrorResp(ctx echo.Context, code int, message string) error {
	err := openapi.ErrorWithMessage{ErrorMsg: message, ErrorCode: code}
	return ctx.JSON(http.StatusBadRequest, err)
}

func checkServerErr(err error) {
	if err != nil {
		log.L().Error("fail to start dm-master", zap.Error(err))
		os.Exit(2)
	}
}
