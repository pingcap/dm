package checker

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-enterprise-tools/dm/config"
	"github.com/pingcap/tidb-enterprise-tools/dm/pb"
	"github.com/pingcap/tidb-enterprise-tools/dm/unit"
	"github.com/pingcap/tidb-tools/pkg/check"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/siddontang/go/sync2"
	"golang.org/x/net/context"
)

// Checker performs pre-check of data synchronization
type Checker struct {
	closed sync2.AtomicBool

	sourceDB *sql.DB
	targetDB *sql.DB

	sourceDBinfo *dbutil.DBConfig
	targetDBInfo *dbutil.DBConfig

	checkList []check.Checker
	result    struct {
		sync.RWMutex
		detail *check.Results
	}
}

// NewChecker returns a checker
func NewChecker(cfg *config.SubTaskConfig) *Checker {
	c := &Checker{}
	c.sourceDBinfo = &dbutil.DBConfig{
		Host:     cfg.From.Host,
		Port:     cfg.From.Port,
		User:     cfg.From.User,
		Password: cfg.From.Password,
	}
	c.targetDBInfo = &dbutil.DBConfig{
		Host:     cfg.To.Host,
		Port:     cfg.To.Port,
		User:     cfg.To.User,
		Password: cfg.To.Password,
	}
	return c
}

// Init implements Unit interface
func (c *Checker) Init() error {
	var err error
	c.sourceDB, err = dbutil.OpenDB(*c.sourceDBinfo)
	if err != nil {
		return errors.Trace(err)
	}

	c.targetDB, err = dbutil.OpenDB(*c.targetDBInfo)
	if err != nil {
		return errors.Trace(err)
	}

	c.checkList = append(c.checkList, check.NewMySQLBinlogEnableChecker(c.sourceDB, c.sourceDBinfo))
	c.checkList = append(c.checkList, check.NewMySQLBinlogFormatChecker(c.sourceDB, c.sourceDBinfo))
	c.checkList = append(c.checkList, check.NewMySQLBinlogRowImageChecker(c.sourceDB, c.sourceDBinfo))
	c.checkList = append(c.checkList, check.NewSourcePrivilegeChecker(c.sourceDB, c.sourceDBinfo))
	return nil
}

// Process implements Unit interface
func (c *Checker) Process(ctx context.Context, pr chan pb.ProcessResult) {
	cctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	isCanceled := false
	errs := make([]*pb.ProcessError, 0, 1)
	result, _ := check.Do(cctx, c.checkList)
	if !result.Summary.Passed {
		errs = append(errs, unit.NewProcessError(pb.ErrorType_CheckFailed, "check was failed, please see detail"))

	}

	select {
	case <-cctx.Done():
		isCanceled = true
	default:
	}

	rawResult, err := json.Marshal(result)
	if err != nil {
		errs = append(errs, unit.NewProcessError(pb.ErrorType_UnknownError, fmt.Sprintf("marshal error %v", err)))
	}

	c.result.Lock()
	c.result.detail = result
	c.result.Unlock()

	pr <- pb.ProcessResult{
		IsCanceled: isCanceled,
		Errors:     errs,
		Detail:     rawResult,
	}
}

// Close implements Unit interface
func (c *Checker) Close() {
	if c.closed.Get() {
		return
	}

	if c.sourceDB != nil {
		if err := dbutil.CloseDB(c.sourceDB); err != nil {
			log.Infof("close source db %+v error %v", c.sourceDBinfo, err)
		}
	}

	if c.targetDB != nil {
		if err := dbutil.CloseDB(c.targetDB); err != nil {
			log.Infof("close target db %+v error %v", c.targetDBInfo, err)
		}
	}

	c.closed.Set(true)
}

// Pause implements Unit interface
func (c *Checker) Pause() {
	if c.closed.Get() {
		log.Warn("[checker] try to pause, but already closed")
		return
	}
}

// Resume resumes the paused process
func (c *Checker) Resume(ctx context.Context, upr chan pb.ProcessResult) {
	if c.closed.Get() {
		log.Warn("[checker] try to resume, but already closed")
		return
	}

	c.Process(ctx, upr)
}

// Type implements Unit interface
func (c *Checker) Type() pb.UnitType {
	return pb.UnitType_Check
}

// Status implements Unit interface
func (c *Checker) Status() interface{} {
	c.result.RLock()
	res := c.result.detail
	c.result.RUnlock()

	var errs []*pb.ProcessError
	if !res.Summary.Passed {
		errs = append(errs, unit.NewProcessError(pb.ErrorType_CheckFailed, "check was failed, please see detail"))
	}
	rawResult, err := json.Marshal(res)
	if err != nil {
		errs = append(errs, unit.NewProcessError(pb.ErrorType_UnknownError, fmt.Sprintf("marshal error %v", err)))
	}

	return &pb.CheckStatus{
		Passed:     res.Summary.Passed,
		Total:      int32(res.Summary.Total),
		Failed:     int32(res.Summary.Failed),
		Successful: int32(res.Summary.Successful),
		Warning:    int32(res.Summary.Warning),
		Detail:     rawResult,
	}
}
