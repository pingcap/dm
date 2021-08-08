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
	echomiddleware "github.com/labstack/echo/v4/middleware"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
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
	e.Use(echomiddleware.Logger())
	// e.Logger.SetOutput()
	e.Use(echomiddleware.Recover())
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
		return ctx.Redirect(http.StatusTemporaryRedirect, fmt.Sprintf("http://%s%s", host, ctx.Request().RequestURI))
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
	needRedirect, host, err := s.redirectRequestToLeader(ctx.Request().Context())
	if err != nil {
		return err
	}
	if needRedirect {
		return ctx.Redirect(http.StatusTemporaryRedirect, fmt.Sprintf("http://%s%s", host, ctx.Request().RequestURI))
	}

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
	needRedirect, host, err := s.redirectRequestToLeader(ctx.Request().Context())
	if err != nil {
		return err
	}
	if needRedirect {
		return ctx.Redirect(http.StatusTemporaryRedirect, fmt.Sprintf("http://%s%s", host, ctx.Request().RequestURI))
	}
	if err := s.scheduler.RemoveSourceCfg(sourceName); err != nil {
		return err
	}
	return ctx.NoContent(http.StatusNoContent)
}

// DMAPIStartRelay url is:(POST /api/v1/sources/{source-id}/relay).
func (s *Server) DMAPIStartRelay(ctx echo.Context, sourceName string) error {
	needRedirect, host, err := s.redirectRequestToLeader(ctx.Request().Context())
	if err != nil {
		return err
	}
	if needRedirect {
		return ctx.Redirect(http.StatusTemporaryRedirect, fmt.Sprintf("http://%s%s", host, ctx.Request().RequestURI))
	}

	var req openapi.StartRelayRequest
	if err := ctx.Bind(&req); err != nil {
		return err
	}
	sourceCfg := s.scheduler.GetSourceCfgByID(sourceName)
	if sourceCfg == nil {
		return terror.ErrSchedulerSourceCfgNotExist.Generate(sourceName)
	}
	if req.RelayBinlogName != nil {
		sourceCfg.RelayBinLogName = *req.RelayBinlogName
	}
	if req.RelayBinlogGtid != nil {
		sourceCfg.RelayBinlogGTID = *req.RelayBinlogGtid
	}
	if req.RelayDir != nil {
		sourceCfg.RelayDir = *req.RelayDir
	}
	if purge := req.Purge; purge != nil {
		if purge.Expires != nil {
			sourceCfg.Purge.Expires = *purge.Expires
		}
		if purge.Interval != nil {
			sourceCfg.Purge.Interval = *purge.Interval
		}
		if purge.RemainSpace != nil {
			sourceCfg.Purge.RemainSpace = *purge.RemainSpace
		}
	}
	// update current source relay config before start relay
	if err := s.scheduler.UpdateSourceCfg(sourceCfg); err != nil {
		return err
	}
	return s.scheduler.StartRelay(sourceName, []string{req.WorkerName})
}

// DMAPIStopRelay url is:(DELETE /api/v1/sources/{source-id}/relay).
func (s *Server) DMAPIStopRelay(ctx echo.Context, sourceName string) error {
	needRedirect, host, err := s.redirectRequestToLeader(ctx.Request().Context())
	if err != nil {
		return err
	}
	if needRedirect {
		return ctx.Redirect(http.StatusTemporaryRedirect, fmt.Sprintf("http://%s%s", host, ctx.Request().RequestURI))
	}
	var req openapi.WorkerNameRequest
	if err := ctx.Bind(&req); err != nil {
		return err
	}
	return s.scheduler.StopRelay(sourceName, []string{req.WorkerName})
}

// DMAPIGetSourceStatus url is:(GET /api/v1/sources/{source-id}/status).
func (s *Server) DMAPIGetSourceStatus(ctx echo.Context, sourceName string) error {
	needRedirect, host, err := s.redirectRequestToLeader(ctx.Request().Context())
	if err != nil {
		return err
	}
	if needRedirect {
		return ctx.Redirect(http.StatusTemporaryRedirect, fmt.Sprintf("http://%s%s", host, ctx.Request().RequestURI))
	}

	ret := s.getStatusFromWorkers(ctx.Request().Context(), []string{sourceName}, "", true)
	if len(ret) != 1 {
		// No response from worker and master means that the current query source has not been created.
		return terror.ErrSchedulerSourceCfgNotExist.Generate(sourceName)
	}
	status := ret[0]
	if !status.Result {
		return terror.ErrOpenAPICommonError.New(status.Msg)
	}
	sourceStatus := status.SourceStatus
	relayStatus := sourceStatus.GetRelayStatus()
	enableRelay := relayStatus == nil
	resp := openapi.SourceStatus{
		EnableRelay: enableRelay,
		SourceName:  sourceStatus.Source,
		WorkerName:  sourceStatus.Worker,
	}
	if enableRelay {
		resp.RelayStatus = &openapi.RelayStatus{
			MasterBinlog:       relayStatus.MasterBinlog,
			MasterBinlogGtid:   relayStatus.MasterBinlogGtid,
			RelayBinlogGtid:    relayStatus.RelayBinlogGtid,
			RelayCatchUpMaster: relayStatus.RelayCatchUpMaster,
			RelayDir:           relayStatus.RelaySubDir,
			Stage:              relayStatus.Stage.String(),
		}
	}
	return ctx.JSON(http.StatusOK, resp)
}

// DMAPIStartTask url is:(POST /api/v1/tasks).
func (s *Server) DMAPIStartTask(ctx echo.Context) error {
	needRedirect, host, err := s.redirectRequestToLeader(ctx.Request().Context())
	if err != nil {
		return err
	}
	if needRedirect {
		return ctx.Redirect(http.StatusTemporaryRedirect, fmt.Sprintf("http://%s%s", host, ctx.Request().RequestURI))
	}

	var req openapi.Task
	if err := ctx.Bind(&req); err != nil {
		return err
	}

	return nil
}

// DMAPIDeleteTask url is:(DELETE /api/v1/tasks).
func (s *Server) DMAPIDeleteTask(ctx echo.Context, taskName string) error {
	panic("not implemented") // TODO: Implement
}

// DMAPIGetTaskList url is:(GET /api/v1/tasks).
func (s *Server) DMAPIGetTaskList(ctx echo.Context) error {
	panic("not implemented") // TODO: Implement
}

// DMAPIGetTaskStatus url is:(GET /api/v1/tasks/{task-name}/status).
func (s *Server) DMAPIGetTaskStatus(ctx echo.Context, taskName string) error {
	panic("not implemented") // TODO: Implement
}

func sourceCfgToModel(cfg config.SourceConfig) openapi.Source {
	// NOTE we don't return SSL cert here, because we don't want to expose it to the user.
	return openapi.Source{
		EnableGtid: cfg.EnableGTID,
		Host:       cfg.From.Host,
		Password:   cfg.From.Password,
		Port:       cfg.From.Port,
		SourceName: cfg.SourceID,
		User:       cfg.From.User,
	}
}

func modelToSourceCfg(source openapi.Source) *config.SourceConfig {
	//  TODO add Security
	cfg := config.NewSourceConfig()
	from := config.DBConfig{
		Host:     source.Host,
		Port:     source.Port,
		User:     source.User,
		Password: source.Password,
	}
	cfg.EnableGTID = source.EnableGtid
	cfg.SourceID = source.SourceName
	cfg.From = from
	return cfg
}

func (s *Server) modelToSubTaskConfigs(ctx context.Context, task openapi.Task) ([]*config.SubTaskConfig, error) {
	// applay some default values
	// TODO(ehco) mv this to another func
	if task.MetaSchema == nil {
		defaultMetaSchema := "dm_meta"
		task.MetaSchema = &defaultMetaSchema
	}

	// check target database is valid
	TODbCfg := &config.DBConfig{
		Host:     task.TargetConfig.Host,
		Port:     task.TargetConfig.Port,
		User:     task.TargetConfig.User,
		Password: task.TargetConfig.Password,
	}
	// TODO(ehco): add security
	err := adjustTargetDB(ctx, TODbCfg)
	if err != nil {
		return nil, terror.WithClass(err, terror.ClassDMMaster)
	}
	// get source database config from cluster
	sourceDBCfgMap := make(map[string]config.DBConfig)
	sourceDBMetaMap := make(map[string]*config.Meta)
	for _, cfg := range task.SourceConfig.SourceConf {
		if sourceCfg := s.scheduler.GetSourceCfgByID(cfg.SourceName); sourceCfg != nil {
			sourceCfg.DecryptPassword()
			// new meta from source config
			sourceDBCfgMap[cfg.SourceName] = sourceCfg.From
			var needAddMeta bool
			meta := &config.Meta{}
			if cfg.BinlogGtid != nil {
				sourceDBMetaMap[cfg.SourceName].BinLogGTID = *cfg.BinlogGtid
				needAddMeta = true
			}
			if cfg.BinlogName != nil {
				sourceDBMetaMap[cfg.SourceName].BinLogName = *cfg.BinlogName
				needAddMeta = true
			}
			if cfg.BinlogPos != nil {
				pos := uint32(*cfg.BinlogPos)
				sourceDBMetaMap[cfg.SourceName].BinLogPos = pos
				needAddMeta = true
			}
			if needAddMeta {
				sourceDBMetaMap[cfg.SourceName] = meta
			}
		} else {
			return nil, terror.ErrOpenAPITaskSourceNotFound.Generatef("source name=%s", cfg.SourceName)
		}
	}

	eventFilterTemplateMap := make(map[string]bf.BinlogEventRule)
	if task.EventFilterRule != nil {
		for _, rule := range *task.EventFilterRule {
			ruleT := bf.BinlogEventRule{Action: bf.Ignore}
			if rule.IgnoreEvent != nil {
				events := make([]bf.EventType, len(*rule.IgnoreEvent))
				for i, eventStr := range *rule.IgnoreEvent {
					events[i] = bf.EventType(eventStr)
				}
				ruleT.Events = events
			}
			if rule.IgnoreSql != nil {
				ruleT.SQLPattern = *rule.IgnoreSql
			}
			eventFilterTemplateMap[rule.RuleName] = ruleT
		}
	}

	// start to generate sub task configs
	subTaskCfgList := make([]*config.SubTaskConfig, len(task.SourceConfig.SourceConf))
	for i, sourceCfg := range task.SourceConfig.SourceConf {
		subTaskCfg := config.NewSubTaskConfig()
		// set target db config
		subTaskCfg.To = *TODbCfg
		// set source db config
		subTaskCfg.From = sourceDBCfgMap[sourceCfg.SourceName]
		// set source meta
		subTaskCfg.MetaFile = *task.MetaSchema
		if meta, ok := sourceDBMetaMap[sourceCfg.SourceName]; ok {
			subTaskCfg.Meta = meta
		}
		subTaskCfg.SourceID = sourceCfg.SourceName
		// set task mode
		subTaskCfg.Mode = string(task.TaskMode)
		// set shard config
		if task.ShardMode != nil {
			subTaskCfg.IsSharding = true
			mode := *task.ShardMode
			subTaskCfg.ShardMode = string(mode)
		} else {
			subTaskCfg.IsSharding = false
		}
		// set online ddl pulgin config
		// TODO(ehco) fix this
		subTaskCfg.OnlineDDLScheme = ""

		// set full unit config
		subTaskCfg.MydumperConfig = config.DefaultMydumperConfig()
		subTaskCfg.LoaderConfig = config.DefaultLoaderConfig()
		if fullCfg := task.SourceConfig.FullMigrateConf; fullCfg != nil {
			if fullCfg.ExportThreads != nil {
				subTaskCfg.MydumperConfig.Threads = *fullCfg.ExportThreads
			}
			if fullCfg.DataDir != nil {
				subTaskCfg.Dir = *fullCfg.DataDir
			}
		}
		// set incremental config
		subTaskCfg.SyncerConfig = config.DefaultSyncerConfig()
		if incrCfg := task.SourceConfig.IncrMigrateConf; incrCfg != nil {
			if incrCfg.ReplThreads != nil {
				subTaskCfg.SyncerConfig.WorkerCount = *incrCfg.ReplThreads
			}
			if incrCfg.ReplBatch != nil {
				subTaskCfg.SyncerConfig.Batch = *incrCfg.ReplBatch
			}
		}
		// set filter rule config
		// subTaskCfg.FilterRules = make([]*bf.BinlogEventRule, len(*task.EventFilterRule))
		// for j, name := range inst.FilterRules {
		// 	// cfg.FilterRules[j] = c.Filters[name]
		// }

		// set migrate rule config
		// subTaskCfg.RouteRules = make([]*router.TableRule, len(task.TableMigrateRule))
		// for j, name := range inst.RouteRules {
		// 	cfg.RouteRules[j] = c.Routes[name]
		// }

		// adjust sub task config
		if err := subTaskCfg.Adjust(true); err != nil {
			return nil, terror.Annotatef(err, "source name=%s", sourceCfg.SourceName)
		}
		subTaskCfgList[i] = subTaskCfg
	}
	return subTaskCfgList, nil
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
