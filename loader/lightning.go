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

package loader

import (
	"context"
	"github.com/pingcap/dm/pkg/binlog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/pingcap/failpoint"

	"github.com/pingcap/dm/pkg/conn"

	"go.etcd.io/etcd/clientv3"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/unit"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/ha"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/utils"

	"github.com/pingcap/tidb/br/pkg/lightning"
	lcfg "github.com/pingcap/tidb/br/pkg/lightning/config"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	lightningCheckpointFile  = "lightning.checkpoint.0.sql"
	lightningCheckpointDB    = "lightning"
	lightningCheckpointTable = "checkpoint"
)

// LightningLoader can load your mydumper data into TiDB database.
type LightningLoader struct {
	sync.RWMutex

	cfg             *config.SubTaskConfig
	cli             *clientv3.Client
	checkPoint      CheckPoint
	workerName      string
	logger          log.Logger
	core            *lightning.Lightning
	toDB            *conn.BaseDB
	toDBConns       []*DBConn
	lightningConfig *lcfg.GlobalConfig

	finish atomic.Bool
	closed atomic.Bool
	cancel context.CancelFunc // for per task context, which maybe different from lightning context
}

// NewLightningLoader creates a new Loader importing data with lightning.
func NewLightningLoader(cfg *config.SubTaskConfig, cli *clientv3.Client, workerName string) *LightningLoader {
	lightningCfg := makeGlobalConfig(cfg)
	core := lightning.New(lightningCfg)
	loader := &LightningLoader{
		cfg:             cfg,
		cli:             cli,
		core:            core,
		lightningConfig: lightningCfg,
		logger:          log.With(zap.String("task", cfg.Name), zap.String("unit", "lightning-load")),
		workerName:      workerName,
	}
	return loader
}

func makeGlobalConfig(cfg *config.SubTaskConfig) *lcfg.GlobalConfig {
	lightningCfg := lcfg.NewGlobalConfig()
	if cfg.To.Security != nil {
		lightningCfg.Security.CAPath = cfg.To.Security.SSLCA
		lightningCfg.Security.CertPath = cfg.To.Security.SSLCert
		lightningCfg.Security.KeyPath = cfg.To.Security.SSLKey
	}
	lightningCfg.TiDB.Host = cfg.To.Host
	lightningCfg.TiDB.Psw = cfg.To.Password
	lightningCfg.TiDB.User = cfg.To.User
	lightningCfg.TiDB.Port = cfg.To.Port
	lightningCfg.TiDB.StatusPort = cfg.TiDB.StatusPort
	lightningCfg.TiDB.PdAddr = cfg.TiDB.PdAddr
	lightningCfg.TikvImporter.Backend = cfg.TiDB.Backend
	lightningCfg.PostRestore.Checksum = lcfg.OpLevelOff
	if cfg.TiDB.Backend == lcfg.BackendLocal {
		lightningCfg.TikvImporter.SortedKVDir = cfg.Dir
	}
	lightningCfg.Mydumper.SourceDir = cfg.Dir
	return lightningCfg
}

// Type implements Unit.Type.
func (l *LightningLoader) Type() pb.UnitType {
	return pb.UnitType_Load
}

// Init initializes loader for a load task, but not start Process.
// if fail, it should not call l.Close.
func (l *LightningLoader) Init(ctx context.Context) (err error) {
	tctx := tcontext.NewContext(ctx, l.logger)
	checkpoint, err := newRemoteCheckPoint(tctx, l.cfg, l.checkpointID())
	failpoint.Inject("ignoreLoadCheckpointErr", func(_ failpoint.Value) {
		l.logger.Info("", zap.String("failpoint", "ignoreLoadCheckpointErr"))
		err = nil
	})
	l.checkPoint = checkpoint
	l.toDB, l.toDBConns, err = createConns(tctx, l.cfg, 1)
	return err
}

func (l *LightningLoader) restore(ctx context.Context) error {
	tctx := tcontext.NewContext(ctx, l.logger)
	if err := l.putLoadTask(); err != nil {
		return err
	}
	if err := l.checkPoint.Init(tctx, lightningCheckpointFile, 1); err != nil {
		return err
	}
	if err := l.checkPoint.Load(tctx); err != nil {
		return err
	}
	db2Tables := make(map[string]Tables2DataFiles)
	tables := make(Tables2DataFiles)
	files := make(DataFiles, 0, 1)
	files = append(files, lightningCheckpointFile)
	tables[lightningCheckpointTable] = files
	db2Tables[lightningCheckpointDB] = tables
	var err error
	if err = l.checkPoint.CalcProgress(db2Tables); err != nil {
		return err
	}
	if !l.checkPoint.IsTableFinished(lightningCheckpointDB, lightningCheckpointTable) {
		cfg := lcfg.NewConfig()
		if err = cfg.LoadFromGlobal(l.lightningConfig); err != nil {
			return err
		}
		cfg.Routes = l.cfg.RouteRules
		if err = cfg.Adjust(ctx); err != nil {
			return err
		}
		l.Lock()
		taskCtx, cancel := context.WithCancel(ctx)
		l.cancel = cancel
		l.Unlock()
		err = l.core.RunOnce(taskCtx, cfg, nil)
		if err == nil {
			l.finish.Store(true)
			offsetSQL := l.checkPoint.GenSQL(lightningCheckpointFile, 1)
			err = l.toDBConns[0].executeSQL(tctx, []string{offsetSQL})
			_ = l.checkPoint.UpdateOffset(lightningCheckpointFile, 1)
		}
	} else {
		l.finish.Store(true)
	}
	if l.cfg.Mode == config.ModeFull {
		_ = l.delLoadTask()
	}
	if l.cfg.CleanDumpFile {
		l.cleanDumpFiles()
	}
	return err
}

// Process implements Unit.Process.
func (l *LightningLoader) Process(ctx context.Context, pr chan pb.ProcessResult) {
	l.logger.Info("lightning load start")
	errs := make([]*pb.ProcessError, 0, 1)
	if err := l.restore(ctx); err != nil && !utils.IsContextCanceledError(err) {
		errs = append(errs, unit.NewProcessError(err))
	}
	isCanceled := false
	select {
	case <-ctx.Done():
		isCanceled = true
	default:
	}
	l.logger.Info("lightning load end")
	pr <- pb.ProcessResult{
		IsCanceled: isCanceled,
		Errors:     errs,
	}
}

func (l *LightningLoader) isClosed() bool {
	return l.closed.Load()
}

// IsFreshTask implements Unit.IsFreshTask.
func (l *LightningLoader) IsFreshTask(ctx context.Context) (bool, error) {
	count, err := l.checkPoint.Count(tcontext.NewContext(ctx, l.logger))
	return count == 0, err
}

// Close does graceful shutdown.
func (l *LightningLoader) Close() {
	l.Pause()
	l.closed.Store(true)
}

// Pause pauses the process, and it can be resumed later
// should cancel context from external.
func (l *LightningLoader) Pause() {
	l.Lock()
	defer l.Unlock()
	if l.isClosed() {
		l.logger.Warn("try to pause, but already closed")
		return
	}
	if l.cancel != nil {
		l.cancel()
	}
	l.core.Stop()
}

// Resume resumes the paused process.
func (l *LightningLoader) Resume(ctx context.Context, pr chan pb.ProcessResult) {
	if l.isClosed() {
		l.logger.Warn("try to resume, but already closed")
		return
	}
	l.core = lightning.New(l.lightningConfig)
	// continue the processing
	l.Process(ctx, pr)
}

// Update implements Unit.Update
// now, only support to update config for routes, filters, column-mappings, block-allow-list
// now no config diff implemented, so simply re-init use new config
// no binlog filter for loader need to update.
func (l *LightningLoader) Update(cfg *config.SubTaskConfig) error {
	// update l.cfg
	l.cfg.BAList = cfg.BAList
	l.cfg.RouteRules = cfg.RouteRules
	l.cfg.ColumnMappingRules = cfg.ColumnMappingRules
	return nil
}

// Status returns the unit's current status.
func (l *LightningLoader) Status(_ *binlog.SourceStatus) interface{} {
	finished, total := l.core.Status()
	progress := percent(finished, total, l.finish.Load())
	s := &pb.LoadStatus{
		FinishedBytes:  finished,
		TotalBytes:     total,
		Progress:       progress,
		MetaBinlog:     "0",
		MetaBinlogGTID: "0",
	}
	return s
}

// checkpointID returns ID which used for checkpoint table.
func (l *LightningLoader) checkpointID() string {
	if len(l.cfg.SourceID) > 0 {
		return l.cfg.SourceID
	}
	dir, err := filepath.Abs(l.cfg.Dir)
	if err != nil {
		l.logger.Warn("get abs dir", zap.String("directory", l.cfg.Dir), log.ShortError(err))
		return l.cfg.Dir
	}
	return shortSha1(dir)
}

// cleanDumpFiles is called when finish restoring data, to clean useless files.
func (l *LightningLoader) cleanDumpFiles() {
	l.logger.Info("clean dump files")
	if l.cfg.Mode == config.ModeFull {
		// in full-mode all files won't be need in the future
		if err := os.RemoveAll(l.cfg.Dir); err != nil {
			l.logger.Warn("error when remove loaded dump folder", zap.String("data folder", l.cfg.Dir), zap.Error(err))
		}
	} else {
		// leave metadata file and table structure files, only delete data files
		files, err := utils.CollectDirFiles(l.cfg.Dir)
		if err != nil {
			l.logger.Warn("fail to collect files", zap.String("data folder", l.cfg.Dir), zap.Error(err))
		}
		var lastErr error
		for f := range files {
			if strings.HasSuffix(f, ".sql") {
				// TODO: table structure files are not used now, but we plan to used them in future so not delete them
				if strings.HasSuffix(f, "-schema-create.sql") || strings.HasSuffix(f, "-schema.sql") {
					continue
				}
				lastErr = os.Remove(filepath.Join(l.cfg.Dir, f))
			}
		}
		if lastErr != nil {
			l.logger.Warn("show last error when remove loaded dump sql files", zap.String("data folder", l.cfg.Dir), zap.Error(lastErr))
		}
	}
}

// putLoadTask is called when start restoring data, to put load worker in etcd.
func (l *LightningLoader) putLoadTask() error {
	_, err := ha.PutLoadTask(l.cli, l.cfg.Name, l.cfg.SourceID, l.workerName)
	if err != nil {
		return err
	}
	l.logger.Info("put load worker in etcd", zap.String("task", l.cfg.Name), zap.String("source", l.cfg.SourceID), zap.String("worker", l.workerName))
	return nil
}

// delLoadTask is called when finish restoring data, to delete load worker in etcd.
func (l *LightningLoader) delLoadTask() error {
	_, _, err := ha.DelLoadTask(l.cli, l.cfg.Name, l.cfg.SourceID)
	if err != nil {
		return err
	}
	l.logger.Info("delete load worker in etcd for full mode", zap.String("task", l.cfg.Name), zap.String("source", l.cfg.SourceID), zap.String("worker", l.workerName))
	return nil
}
