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
	ehcomiddleware "github.com/labstack/echo/v4/middleware"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/openapi"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
)

const (
	docBasePath     = "/api/v1/docs"
	docJSONBasePath = "/api/v1/dm.json"
)

// redirectRequestToLeader is used to redirect the request to leader.
// because the leader has some data in memory, only the leader can process the request.
func (s *Server) redirectRequestToLeader(ctx context.Context) (needRedirect bool, host string, err error) {
	isLeader, _ := s.isLeaderAndNeedForward(ctx)
	if isLeader {
		return false, s.cfg.OpenAPIAddr, nil
	}
	// nolint:dogsled
	_, _, _, leaderOpenAPIAddr, err := s.election.LeaderInfo(ctx)
	return true, leaderOpenAPIAddr, err
}

// DMAPICreateSource url is:(POST /api/v1/sources).
func (s *Server) DMAPICreateSource(ctx echo.Context) error {
	needRedirect, host, err := s.redirectRequestToLeader(ctx.Request().Context())
	if err != nil {
		return err
	}
	if needRedirect {
		return ctx.Redirect(http.StatusPermanentRedirect, fmt.Sprintf("http://%s%s", host, ctx.Request().RequestURI))
	}

	var createSourceReq openapi.Source
	if err := ctx.Bind(&createSourceReq); err != nil {
		return err
	}
	cfg := modelToSourceCfg(createSourceReq)
	if err := checkAndAdjustSourceConfig(ctx.Request().Context(), cfg); err != nil {
		return err
	}
	if err := s.scheduler.AddSourceCfg(cfg); err != nil {
		return err
	}
	return ctx.JSON(http.StatusCreated, createSourceReq)
}

// DMAPIGetSourceList url is:(GET /api/v1/sources).
func (s *Server) DMAPIGetSourceList(ctx echo.Context) error {
	sourceIDS := s.scheduler.GetSourceCfgIDs()
	sourceCfgList := make([]*config.SourceConfig, len(sourceIDS))
	for idx, sourceID := range sourceIDS {
		sourceCfgList[idx] = s.scheduler.GetSourceCfgByID(sourceID)
	}
	resp := make([]openapi.Source, len(sourceCfgList))
	for idx, cfg := range sourceCfgList {
		resp[idx] = sourceCfgToModel(*cfg)
	}
	return ctx.JSON(http.StatusOK, resp)
}

// DMAPIDeleteSource url is:(DELETE /api/v1/sources).
func (s *Server) DMAPIDeleteSource(ctx echo.Context, sourceName string) error {
	if err := s.scheduler.RemoveSourceCfg(sourceName); err != nil {
		return err
	}
	return ctx.NoContent(http.StatusNoContent)
}

// DMAPIStopRelay url is:(DELETE /api/v1/sources/{source-id}/relay).
func (s *Server) DMAPIStopRelay(ctx echo.Context, sourceName string) error {
	panic("not implemented") // TODO: Implement
}

// DMAPIStartRelay url is:(POST /api/v1/sources/{source-id}/relay).
func (s *Server) DMAPIStartRelay(ctx echo.Context, sourceName string) error {
	panic("not implemented") // TODO: Implement
}

// DMAPIGetSourceStatus url is:(GET /api/v1/sources/{source-id}/status).
func (s *Server) DMAPIGetSourceStatus(ctx echo.Context, sourceName string) error {
	panic("not implemented") // TODO: Implement
}

// DMAPIDeleteTask url is:(DELETE /api/v1/tasks).
func (s *Server) DMAPIDeleteTask(ctx echo.Context, taskName string) error {
	panic("not implemented") // TODO: Implement
}

// DMAPIGetTaskList url is:(GET /api/v1/tasks).
func (s *Server) DMAPIGetTaskList(ctx echo.Context) error {
	panic("not implemented") // TODO: Implement
}

// DMAPIStartTask url is:(POST /api/v1/tasks).
func (s *Server) DMAPIStartTask(ctx echo.Context) error {
	panic("not implemented") // TODO: Implement
}

// DMAPIGetTaskStatus url is:(GET /api/v1/tasks/{task-name}/status).
func (s *Server) DMAPIGetTaskStatus(ctx echo.Context, taskName string) error {
	panic("not implemented") // TODO: Implement
}

func sourceCfgToModel(cfg config.SourceConfig) openapi.Source {
	// NOTE we don't return SSL cert here, because we don't want to expose it to the user.
	return openapi.Source{
		CaseSensitive: &cfg.CaseSensitive,
		EnableGtid:    &cfg.EnableGTID,
		Host:          &cfg.From.Host,
		Password:      &cfg.From.Password,
		Port:          &cfg.From.Port,
		SourceName:    &cfg.SourceID,
		User:          &cfg.From.User,
	}
}

func modelToSourceCfg(source openapi.Source) *config.SourceConfig {
	//  TODO add Security
	cfg := config.NewSourceConfig()
	from := config.DBConfig{
		Host:     *source.Host,
		Port:     *source.Port,
		User:     *source.User,
		Password: *source.Password,
	}
	cfg.CaseSensitive = *source.CaseSensitive
	cfg.EnableGTID = *source.EnableGtid
	cfg.SourceID = *source.SourceName
	cfg.From = from
	return cfg
}

// StartOpenAPIServer start OpenAPI server.
func (s *Server) StartOpenAPIServer(ctx context.Context) {
	if s.cfg.OpenAPIAddr == "" {
		return
	}
	swagger, err := openapi.GetSwagger()
	if err != nil {
		exitServer(err)
	}

	swagger.AddServer(&openapi3.Server{URL: fmt.Sprintf("http://%s", s.cfg.OpenAPIAddr)})
	swaggerJSON, err := swagger.MarshalJSON()
	if err != nil {
		exitServer(err)
	}
	docMW := openapi.NewSwaggerDocUI(openapi.NewSwaggerConfig(docBasePath, docJSONBasePath, ""), swaggerJSON)

	// Clear out the servers array in the swagger spec, that skips validating
	// that server names match. We don't know how this thing will be run.
	swagger.Servers = nil

	// Echo instance
	e := echo.New()
	// inject err handler
	e.HTTPErrorHandler = terrorHTTPErrorHandler
	// Middlewares
	e.Use(docMW)
	e.Use(ehcomiddleware.Logger())
	// e.Use(ehcomiddleware.Recover())
	// Use our validation middleware to check all requests against the OpenAPI schema.
	e.Use(middleware.OapiRequestValidator(swagger))
	openapi.RegisterHandlers(e, s)

	// Start server
	go func() {
		if err := e.Start(s.cfg.OpenAPIAddr); err != nil && err != http.ErrServerClosed {
			exitServer(err)
		}
	}()

	// Wait for ctx.Done()
	<-ctx.Done()
	if err := e.Shutdown(ctx); err != nil {
		log.L().Warn("shutdown echo openapi server", zap.Error(err))
	}
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
	err := openapi.ErrorWithMessage{ErrorMsg: &message, ErrorCode: &code}
	return ctx.JSON(http.StatusBadRequest, err)
}

func exitServer(err error) {
	log.L().Error("fail to start dm-master", zap.Error(err))
	os.Exit(2)
}
