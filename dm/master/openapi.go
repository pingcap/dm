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

	"github.com/deepmap/oapi-codegen/pkg/middleware"
	"github.com/labstack/echo/v4"
	echomiddleware "github.com/labstack/echo/v4/middleware"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/pingcap/tidb-tools/pkg/filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"go.uber.org/zap"

	"github.com/pingcap/dm/checker"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/master/scheduler"
	"github.com/pingcap/dm/openapi"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
)

const (
	docJSONBasePath = "/api/v1/dm.json"
)

// redirectRequestToLeaderMW a middleware auto redirect request to leader.
// because the leader has some data in memory, only the leader can process the request.
func (s *Server) redirectRequestToLeaderMW() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(ctx echo.Context) error {
			ctx2 := ctx.Request().Context()
			isLeader, _ := s.isLeaderAndNeedForward(ctx2)
			if isLeader {
				return next(ctx)
			}
			// nolint:dogsled
			_, _, leaderOpenAPIAddr, err := s.election.LeaderInfo(ctx2)
			if err != nil {
				return err
			}
			return ctx.Redirect(http.StatusTemporaryRedirect, fmt.Sprintf("http://%s%s", leaderOpenAPIAddr, ctx.Request().RequestURI))
		}
	}
}

// InitOpenAPIHandles init openapi handlers.
func (s *Server) InitOpenAPIHandles() error {
	swagger, err := openapi.GetSwagger()
	if err != nil {
		return err
	}
	e := echo.New()
	// inject err handler
	e.HTTPErrorHandler = terrorHTTPErrorHandler
	// middlewares
	logger := log.L().WithFields(zap.String("component", "openapi")).Logger
	// set logger
	e.Use(openapi.ZapLogger(logger))
	e.Use(echomiddleware.Recover())
	e.Use(s.redirectRequestToLeaderMW())
	// disables swagger server name validation. it seems to work poorly
	swagger.Servers = nil
	// use our validation middleware to check all requests against the OpenAPI schema.
	e.Use(middleware.OapiRequestValidator(swagger))
	openapi.RegisterHandlers(e, s)
	s.echo = e
	return nil
}

// GetDocJSON url is:(GET /api/v1/dm.json).
func (s *Server) GetDocJSON(ctx echo.Context) error {
	swaggerJSON, err := openapi.GetSwaggerJSON()
	if err != nil {
		return err
	}
	return ctx.JSONBlob(200, swaggerJSON)
}

// GetDocHTML url is:(GET /api/v1/docs).
func (s *Server) GetDocHTML(ctx echo.Context) error {
	html, err := openapi.GetSwaggerHTML(openapi.NewSwaggerConfig(docJSONBasePath, ""))
	if err != nil {
		return err
	}
	return ctx.HTML(http.StatusOK, html)
}

// DMAPICreateSource url is:(POST /api/v1/sources).
func (s *Server) DMAPICreateSource(ctx echo.Context) error {
	var createSourceReq openapi.Source
	if err := ctx.Bind(&createSourceReq); err != nil {
		return err
	}
	cfg := modelToSourceCfg(createSourceReq)
	if err := checkAndAdjustSourceConfigFunc(ctx.Request().Context(), cfg); err != nil {
		return err
	}
	if err := s.scheduler.AddSourceCfg(cfg); err != nil {
		return err
	}
	return ctx.JSON(http.StatusCreated, createSourceReq)
}

// DMAPIGetSourceList url is:(GET /api/v1/sources).
func (s *Server) DMAPIGetSourceList(ctx echo.Context) error {
	sourceMap := s.scheduler.GetSourceCfgs()
	sourceList := []openapi.Source{}
	for key := range sourceMap {
		sourceList = append(sourceList, sourceCfgToModel(sourceMap[key]))
	}
	resp := openapi.GetSourceListResponse{Total: len(sourceList), Data: sourceList}
	return ctx.JSON(http.StatusOK, resp)
}

// DMAPIDeleteSource url is:(DELETE /api/v1/sources).
func (s *Server) DMAPIDeleteSource(ctx echo.Context, sourceName string) error {
	if err := s.scheduler.RemoveSourceCfg(sourceName); err != nil {
		return err
	}
	return ctx.NoContent(http.StatusNoContent)
}

// DMAPIStartRelay url is:(POST /api/v1/sources/{source-id}/relay).
func (s *Server) DMAPIStartRelay(ctx echo.Context, sourceName string) error {
	var req openapi.StartRelayRequest
	if err := ctx.Bind(&req); err != nil {
		return err
	}
	sourceCfg := s.scheduler.GetSourceCfgByID(sourceName)
	if sourceCfg == nil {
		return terror.ErrSchedulerSourceCfgNotExist.Generate(sourceName)
	}
	needUpdate := false
	// update relay related in source cfg
	if req.RelayBinlogName != nil && sourceCfg.RelayBinLogName != *req.RelayBinlogName {
		sourceCfg.RelayBinLogName = *req.RelayBinlogName
		needUpdate = true
	}
	if req.RelayBinlogGtid != nil && sourceCfg.RelayBinlogGTID != *req.RelayBinlogGtid {
		sourceCfg.RelayBinlogGTID = *req.RelayBinlogGtid
		needUpdate = true
	}
	if req.RelayDir != nil && sourceCfg.RelayDir != *req.RelayDir {
		sourceCfg.RelayDir = *req.RelayDir
		needUpdate = true
	}
	if needUpdate {
		// update current source relay config before start relay
		if err := s.scheduler.UpdateSourceCfg(sourceCfg); err != nil {
			return err
		}
	}
	return s.scheduler.StartRelay(sourceName, req.WorkerNameList)
}

// DMAPIStopRelay url is:(DELETE /api/v1/sources/{source-id}/relay).
func (s *Server) DMAPIStopRelay(ctx echo.Context, sourceName string) error {
	var req openapi.StopRelayRequest
	if err := ctx.Bind(&req); err != nil {
		return err
	}
	return s.scheduler.StopRelay(sourceName, req.WorkerNameList)
}

// DMAPIGetSourceStatus url is:(GET /api/v1/sources/{source-id}/status).
func (s *Server) DMAPIGetSourceStatus(ctx echo.Context, sourceName string) error {
	sourceCfg := s.scheduler.GetSourceCfgByID(sourceName)
	if sourceCfg == nil {
		return terror.ErrSchedulerSourceCfgNotExist.Generate(sourceName)
	}
	var resp openapi.GetSourceStatusResponse
	worker := s.scheduler.GetWorkerBySource(sourceName)
	// current this source not bound to any worker
	if worker == nil {
		resp.Data = append(resp.Data, openapi.SourceStatus{SourceName: sourceName})
		resp.Total = len(resp.Data)
		return ctx.JSON(http.StatusOK, resp)
	}
	// get status from worker
	workerRespList := s.getStatusFromWorkers(ctx.Request().Context(), []string{sourceName}, "", true)
	for _, workerStatus := range workerRespList {
		if workerStatus == nil {
			// this should not happen unless the rpc in the worker server has been modified
			return terror.ErrOpenAPICommonError.New("worker's query-status response is nil")
		}
		sourceStatus := openapi.SourceStatus{SourceName: sourceName, WorkerName: workerStatus.SourceStatus.Worker}
		if !workerStatus.Result {
			sourceStatus.ErrorMsg = &workerStatus.Msg
		} else if relayStatus := workerStatus.SourceStatus.GetRelayStatus(); relayStatus != nil {
			sourceStatus.RelayStatus = &openapi.RelayStatus{
				MasterBinlog:       relayStatus.MasterBinlog,
				MasterBinlogGtid:   relayStatus.MasterBinlogGtid,
				RelayBinlogGtid:    relayStatus.RelayBinlogGtid,
				RelayCatchUpMaster: relayStatus.RelayCatchUpMaster,
				RelayDir:           relayStatus.RelaySubDir,
				Stage:              relayStatus.Stage.String(),
			}
		}
		resp.Data = append(resp.Data, sourceStatus)
	}
	resp.Total = len(resp.Data)
	return ctx.JSON(http.StatusOK, resp)
}

// DMAPIStartTask url is:(POST /api/v1/tasks).
func (s *Server) DMAPIStartTask(ctx echo.Context) error {
	var req openapi.CreateTaskRequest
	if err := ctx.Bind(&req); err != nil {
		return err
	}
	task := req.Task
	if err := adjustModelTask(&task); err != nil {
		return err
	}
	// prepare target db config
	newCtx := ctx.Request().Context()
	toDBCfg, err := getAndAdjustTaskTargetDBCfg(newCtx, &task)
	if err != nil {
		return err
	}
	// generate sub task config list
	subTaskConfigList, err := s.modelToSubTaskConfigList(toDBCfg, &task)
	if err != nil {
		return err
	}
	var needStartSubTaskList []config.SubTaskConfig
	// check subtask config
	subTaskConfigPList := make([]*config.SubTaskConfig, len(subTaskConfigList))
	for i := range subTaskConfigList {
		subTaskConfigPList[i] = &subTaskConfigList[i]
	}
	if err = checker.CheckSyncConfigFunc(newCtx, subTaskConfigPList,
		common.DefaultErrorCnt, common.DefaultWarnCnt); err != nil {
		return terror.WithClass(err, terror.ClassDMMaster)
	}
	// specify only start task on partial sources
	if req.SourceNameList != nil {
		// source name -> sub task config
		subTaskCfgM := make(map[string]*config.SubTaskConfig, len(subTaskConfigList))
		for idx := range subTaskConfigList {
			cfg := subTaskConfigList[idx]
			subTaskCfgM[cfg.SourceID] = &cfg
		}
		for _, sourceName := range *req.SourceNameList {
			subTaskCfg, ok := subTaskCfgM[sourceName]
			if !ok {
				return terror.ErrOpenAPICommonError.Generatef("source %s not found.", sourceName)
			}
			needStartSubTaskList = append(needStartSubTaskList, *subTaskCfg)
		}
	} else {
		needStartSubTaskList = subTaskConfigList
	}
	// end all pre-check, start to create task
	var (
		latched = false
		release scheduler.ReleaseFunc
	)
	if req.RemoveMeta {
		// use same latch for remove-meta and start-task
		release, err = s.scheduler.AcquireSubtaskLatch(task.Name)
		if err != nil {
			return terror.ErrSchedulerLatchInUse.Generate("RemoveMeta", task.Name)
		}
		defer release()
		latched = true
		err = s.removeMetaData(newCtx, task.Name, *task.MetaSchema, toDBCfg)
		if err != nil {
			return terror.Annotate(err, "while removing metadata")
		}
	}
	err = s.scheduler.AddSubTasks(latched, needStartSubTaskList...)
	if err != nil {
		return err
	}
	if release != nil {
		release()
	}
	return ctx.JSON(http.StatusCreated, task)
}

// DMAPIDeleteTask url is:(DELETE /api/v1/tasks).
func (s *Server) DMAPIDeleteTask(ctx echo.Context, taskName string, params openapi.DMAPIDeleteTaskParams) error {
	var sourceList []string
	if params.SourceNameList != nil {
		sourceList = *params.SourceNameList
	} else {
		sourceList = s.getTaskResources(taskName)
	}
	if len(sourceList) == 0 {
		return terror.ErrSchedulerTaskNotExist.Generate(taskName)
	}
	if err := s.scheduler.RemoveSubTasks(taskName, sourceList...); err != nil {
		return err
	}
	return ctx.NoContent(http.StatusNoContent)
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

func sourceCfgToModel(cfg *config.SourceConfig) openapi.Source {
	// PM's requirement, we always return obfuscated password to user
	source := openapi.Source{
		EnableGtid: cfg.EnableGTID,
		Host:       cfg.From.Host,
		Password:   "******",
		Port:       cfg.From.Port,
		SourceName: cfg.SourceID,
		User:       cfg.From.User,
		Purge: &openapi.Purge{
			Expires:     &cfg.Purge.Expires,
			Interval:    &cfg.Purge.Interval,
			RemainSpace: &cfg.Purge.RemainSpace,
		},
	}
	if cfg.From.Security != nil {
		// NOTE we don't return security content here, because we don't want to expose it to the user.
		var certAllowedCn []string
		certAllowedCn = append(certAllowedCn, cfg.From.Security.CertAllowedCN...)
		source.Security = &openapi.Security{CertAllowedCn: &certAllowedCn}
	}
	return source
}

func modelToSourceCfg(source openapi.Source) *config.SourceConfig {
	cfg := config.NewSourceConfig()
	from := config.DBConfig{
		Host:     source.Host,
		Port:     source.Port,
		User:     source.User,
		Password: source.Password,
	}
	if source.Security != nil {
		from.Security = &config.Security{
			SSLCABytes:   []byte(source.Security.SslCaContent),
			SSLKEYBytes:  []byte(source.Security.SslKeyContent),
			SSLCertBytes: []byte(source.Security.SslCertContent),
		}
		if source.Security.CertAllowedCn != nil {
			from.Security.CertAllowedCN = *source.Security.CertAllowedCn
		}
	}
	cfg.From = from
	cfg.EnableGTID = source.EnableGtid
	cfg.SourceID = source.SourceName
	if purge := source.Purge; purge != nil {
		if purge.Expires != nil {
			cfg.Purge.Expires = *purge.Expires
		}
		if purge.Interval != nil {
			cfg.Purge.Interval = *purge.Interval
		}
		if purge.RemainSpace != nil {
			cfg.Purge.RemainSpace = *purge.RemainSpace
		}
	}
	return cfg
}

func (s *Server) modelToSubTaskConfigList(toDBCfg *config.DBConfig, task *openapi.Task) ([]config.SubTaskConfig, error) {
	// prepare source db config source name -> source config
	sourceCfgMap := make(map[string]*config.SourceConfig)
	for _, cfg := range task.SourceConfig.SourceConf {
		if sourceCfg := s.scheduler.GetSourceCfgByID(cfg.SourceName); sourceCfg != nil {
			sourceCfgMap[cfg.SourceName] = sourceCfg
		} else {
			return nil, terror.ErrOpenAPITaskSourceNotFound.Generatef("source name=%s", cfg.SourceName)
		}
	}
	// source name -> meta config
	sourceDBMetaMap := make(map[string]*config.Meta)
	for _, cfg := range task.SourceConfig.SourceConf {
		// check if need to set source meta
		var needAddMeta bool
		meta := &config.Meta{}
		if cfg.BinlogGtid != nil {
			meta.BinLogGTID = *cfg.BinlogGtid
			needAddMeta = true
		}
		if cfg.BinlogName != nil {
			meta.BinLogName = *cfg.BinlogName
			needAddMeta = true
		}
		if cfg.BinlogPos != nil {
			pos := uint32(*cfg.BinlogPos)
			meta.BinLogPos = pos
			needAddMeta = true
		}
		if needAddMeta {
			sourceDBMetaMap[cfg.SourceName] = meta
		}
	}
	// source name -> migrate rule list
	tableMigrateRuleMap := make(map[string][]openapi.TaskTableMigrateRule)
	for _, rule := range task.TableMigrateRule {
		tableMigrateRuleMap[rule.Source.SourceName] = append(tableMigrateRuleMap[rule.Source.SourceName], rule)
	}
	// rule name -> rule template
	eventFilterTemplateMap := make(map[string]bf.BinlogEventRule)
	if task.BinlogFilterRule != nil {
		for _, rule := range *task.BinlogFilterRule {
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
	subTaskCfgList := make([]config.SubTaskConfig, len(task.SourceConfig.SourceConf))
	for i, sourceCfg := range task.SourceConfig.SourceConf {
		subTaskCfg := config.NewSubTaskConfig()
		// set task name and mode
		subTaskCfg.Name = task.Name
		subTaskCfg.Mode = string(task.TaskMode)
		// set task meta
		subTaskCfg.MetaFile = *task.MetaSchema
		if meta, ok := sourceDBMetaMap[sourceCfg.SourceName]; ok {
			subTaskCfg.Meta = meta
		}
		// set shard config
		if task.ShardMode != nil {
			subTaskCfg.IsSharding = true
			mode := *task.ShardMode
			subTaskCfg.ShardMode = string(mode)
		} else {
			subTaskCfg.IsSharding = false
		}
		// set online ddl pulgin config
		subTaskCfg.OnlineDDL = task.EnhanceOnlineSchemaChange
		// set case sensitive from source
		subTaskCfg.CaseSensitive = sourceCfgMap[sourceCfg.SourceName].CaseSensitive
		// set source db config
		subTaskCfg.SourceID = sourceCfg.SourceName
		subTaskCfg.From = sourceCfgMap[sourceCfg.SourceName].From
		// set target db config
		subTaskCfg.To = *toDBCfg.Clone()
		// TODO set meet error policy
		// TODO ExprFilter
		// set full unit config
		subTaskCfg.MydumperConfig = config.DefaultMydumperConfig()
		subTaskCfg.LoaderConfig = config.DefaultLoaderConfig()
		if fullCfg := task.SourceConfig.FullMigrateConf; fullCfg != nil {
			if fullCfg.ExportThreads != nil {
				subTaskCfg.MydumperConfig.Threads = *fullCfg.ExportThreads
			}
			if fullCfg.ImportThreads != nil {
				subTaskCfg.LoaderConfig.PoolSize = *fullCfg.ImportThreads
			}
			if fullCfg.DataDir != nil {
				subTaskCfg.LoaderConfig.Dir = *fullCfg.DataDir
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
		// set route,blockAllowList,filter config
		doDBs := []string{}
		doTables := []*filter.Table{}
		routeRules := []*router.TableRule{}
		filterRules := []*bf.BinlogEventRule{}
		for _, rule := range tableMigrateRuleMap[sourceCfg.SourceName] {
			// route
			routeRules = append(routeRules, &router.TableRule{
				SchemaPattern: rule.Source.Schema, TablePattern: rule.Source.Table,
				TargetSchema: rule.Target.Schema, TargetTable: rule.Target.Table,
			})
			// filter
			if rule.EventFilterName != nil {
				for _, name := range *rule.EventFilterName {
					filterRule, ok := eventFilterTemplateMap[name] // NOTE: this return a copied value
					if !ok {
						return nil, terror.ErrOpenAPICommonError.Generatef("filter rule name %s not found.", name)
					}
					filterRule.SchemaPattern = rule.Source.Schema
					filterRule.TablePattern = rule.Source.Table
					filterRules = append(filterRules, &filterRule)
				}
			}
			// BlockAllowList
			doDBs = append(doDBs, rule.Source.Schema)
			doTables = append(doTables, &filter.Table{Schema: rule.Source.Schema, Name: rule.Source.Table})
		}
		subTaskCfg.RouteRules = routeRules
		subTaskCfg.FilterRules = filterRules
		subTaskCfg.BAList = &filter.Rules{DoDBs: removeDuplication(doDBs), DoTables: doTables}
		// adjust sub task config
		if err := subTaskCfg.Adjust(true); err != nil {
			return nil, terror.Annotatef(err, "source name=%s", sourceCfg.SourceName)
		}
		subTaskCfgList[i] = *subTaskCfg
	}
	return subTaskCfgList, nil
}

func adjustModelTask(task *openapi.Task) error {
	defaultMetaSchema := "dm_meta"
	if task.MetaSchema == nil {
		task.MetaSchema = &defaultMetaSchema
	}
	// check some not implemented features
	if task.OnDuplicate != openapi.TaskOnDuplicateError {
		return terror.ErrOpenAPICommonError.Generate("`on_duplicate` only supports `error` for now.")
	}
	return nil
}

func getAndAdjustTaskTargetDBCfg(ctx context.Context, task *openapi.Task) (*config.DBConfig, error) {
	toDBCfg := &config.DBConfig{
		Host:     task.TargetConfig.Host,
		Port:     task.TargetConfig.Port,
		User:     task.TargetConfig.User,
		Password: task.TargetConfig.Password,
	}
	if task.TargetConfig.Security != nil {
		var certAllowedCn []string
		if task.TargetConfig.Security.CertAllowedCn != nil {
			certAllowedCn = append(certAllowedCn, *task.TargetConfig.Security.CertAllowedCn...)
		}
		toDBCfg.Security = &config.Security{
			SSLCABytes:    []byte(task.TargetConfig.Security.SslCaContent),
			SSLKEYBytes:   []byte(task.TargetConfig.Security.SslKeyContent),
			SSLCertBytes:  []byte(task.TargetConfig.Security.SslCertContent),
			CertAllowedCN: certAllowedCn,
		}
	}
	if adjustDBErr := adjustTargetDB(ctx, toDBCfg); adjustDBErr != nil {
		return nil, terror.WithClass(adjustDBErr, terror.ClassDMMaster)
	}
	return toDBCfg, nil
}

func removeDuplication(in []string) []string {
	m := make(map[string]struct{}, len(in))
	j := 0
	for _, v := range in {
		_, ok := m[v]
		if ok {
			continue
		}
		m[v] = struct{}{}
		in[j] = v
		j++
	}
	return in[:j]
}
