package lightning

import (
	"context"
	"github.com/pingcap/dm/dm/unit"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/utils"
	lightningConfig "github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/mydump"
	"github.com/pingcap/tidb-lightning/lightning/restore"
	"go.uber.org/zap"
)

// Lightning is used to replace Loader.
type Lightning struct {
	rc     *restore.RestoreController
	mdl    *mydump.MDLoader
	logCtx *tcontext.Context

	cfg    *config.SubTaskConfig
	ltnCfg *lightningConfig.Config
}

// NewLightning return new Lightning object.
func NewLightning(cfg *config.SubTaskConfig) *Lightning {
	logCtx := tcontext.Background().WithLogger(log.With(zap.String("task", cfg.Name), zap.String("unit", "lightning")))
	lightningCf := lightningConfig.NewConfig()

	return &Lightning{
		cfg:    cfg,
		logCtx: logCtx,
		ltnCfg: lightningCf,
	}
}

// Init implements Unit.Init
func (l *Lightning) Init(ctx context.Context) error {
	lightningCf := l.ltnCfg
	cfg := l.cfg

	// Set TiDB backend config.
	lightningCf.TikvImporter.Backend = lightningConfig.BackendTiDB
	lightningCf.TiDB.Host = cfg.To.Host
	lightningCf.TiDB.User = cfg.To.User
	lightningCf.TiDB.Port = cfg.To.Port
	psw, err := utils.Decrypt(cfg.To.Password)
	if err != nil {
		l.logCtx.L().Error("TiDB password format error", log.ShortError(err))
		return err
	}
	lightningCf.TiDB.Psw = psw

	// Set black & white list config.
	lightningCf.BWList = cfg.BWList

	// Set router rules.
	lightningCf.Routes = cfg.RouteRules

	// Set MyDumper config.
	lightningCf.Mydumper.SourceDir = cfg.Dir

	// Set cocurrency size.
	lightningCf.App.IndexConcurrency = cfg.PoolSize
	lightningCf.App.TableConcurrency = cfg.PoolSize

	// Set checkpoint config.
	lightningCf.Checkpoint.Driver = lightningConfig.CheckpointDriverMySQL

	// Set misc config.
	lightningCf.App.CheckRequirements = false

	err = lightningCf.Adjust()
	return err
}

// Process implements Unit.Process
func (l *Lightning) Process(ctx context.Context, pr chan pb.ProcessResult) {
	mdl, err := mydump.NewMyDumpLoader(l.ltnCfg)
	if err != nil {
		l.logCtx.L().Error("create MyDumperLoader failed", log.ShortError(err))
		pr <- pb.ProcessResult{
			Errors: []*pb.ProcessError{unit.NewProcessError(pb.ErrorType_UnknownError, err)},
		}
		return
	}
	l.mdl = mdl
	dbMetas := mdl.GetDatabases()
	procedure, err := restore.NewRestoreController(ctx, dbMetas, l.ltnCfg)
	if err != nil {
		l.logCtx.L().Error("create RestoreController failed", log.ShortError(err))
		pr <- pb.ProcessResult{
			Errors: []*pb.ProcessError{unit.NewProcessError(pb.ErrorType_UnknownError, err)},
		}
		return
	}
	l.rc = procedure
	err = l.rc.Run(ctx)
	l.rc.Wait()
	if err != nil {
		l.logCtx.L().Error("error occur during restore", log.ShortError(err))
		pr <- pb.ProcessResult{
			Errors: []*pb.ProcessError{unit.NewProcessError(pb.ErrorType_UnknownError, err)},
		}
		return
	}
}

// Close implements Unit.Close
func (l *Lightning) Close() {
	l.rc.Close()
}

// Pause implements Unit.Pause
func (l *Lightning) Pause() {

}

// Resume implements Unit.Resume
func (l *Lightning) Resume(ctx context.Context, pr chan pb.ProcessResult) {

}

// Update implements Unit.Update
func (l *Lightning) Update(cfg *config.SubTaskConfig) error {
	return nil
}

// Status implements Unit.Status
func (l *Lightning) Status() interface{} {
	return nil
}

// Error implements Unit.Error
func (l *Lightning) Error() interface{} {
	return nil
}

// Type implements Unit.Type
func (l *Lightning) Type() pb.UnitType {
	return pb.UnitType_Lightning
}

// IsFreshTask implements Unit.IsFreshTask
func (l *Lightning) IsFreshTask(ctx context.Context) (bool, error) {
	return false, nil
}
