package check

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/utils"
)

// MySQLVersionChecker checks mysql/mariadb/rds,... version.
type MySQLVersionChecker struct {
	db     *sql.DB
	dbinfo *dbutil.DBConfig
}

// NewMySQLVersionChecker returns a Checker
func NewMySQLVersionChecker(db *sql.DB, dbinfo *dbutil.DBConfig) Checker {
	return &MySQLVersionChecker{db: db, dbinfo: dbinfo}
}

// MinVersion is mysql minimal version required
var MinVersion = [3]uint{5, 5, 0}

// Check implements the Checker interface.
// we only support version >= 5.5
func (pc *MySQLVersionChecker) Check(ctx context.Context) *Result {
	result := &Result{
		Name:  pc.Name(),
		Desc:  "checks whether mysql version is satisfied",
		State: StateFailure,
		Extra: fmt.Sprintf("%s:%d", pc.dbinfo.Host, pc.dbinfo.Port),
	}
	defer log.Infof("check mysql version, result %+v", result)

	value, err := dbutil.ShowVersion(ctx, pc.db)
	if err != nil {
		markCheckError(result, err)
		return result
	}

	version := toMySQLVersion(value)
	if !version.IsAtLeast(MinVersion) {
		result.ErrorMsg = fmt.Sprintf("version required at least %v but got %v", MinVersion, version)
		result.Instruction = "Please upgrade your database system"
		return result
	}

	result.State = StateSuccess
	return result
}

// Name implements the Checker interface.
func (pc *MySQLVersionChecker) Name() string {
	return "mysql_version"
}

/*****************************************************/

// MySQLServerIDChecker checks mysql/mariadb server ID.
type MySQLServerIDChecker struct {
	db     *sql.DB
	dbinfo *dbutil.DBConfig
}

// NewMySQLServerIDChecker returns a Checker
func NewMySQLServerIDChecker(db *sql.DB, dbinfo *dbutil.DBConfig) Checker {
	return &MySQLServerIDChecker{db: db, dbinfo: dbinfo}
}

// Check implements the Checker interface.
func (pc *MySQLServerIDChecker) Check(ctx context.Context) *Result {
	result := &Result{
		Name:  pc.Name(),
		Desc:  "checks whether mysql server_id has been set > 1",
		State: StateFailure,
		Extra: fmt.Sprintf("%s:%d", pc.dbinfo.Host, pc.dbinfo.Port),
	}
	defer log.Infof("check mysql version, result %+v", result)

	serverID, err := dbutil.ShowServerID(ctx, pc.db)
	if err != nil {
		if utils.OriginError(err) == sql.ErrNoRows {
			result.ErrorMsg = "server_id not set"
			result.Instruction = "please set server_id in your database"
		} else {
			markCheckError(result, err)
		}

		return result
	}

	if serverID == 0 {
		result.ErrorMsg = "server_id is 0"
		result.Instruction = "please set server_id greater than 0"
		return result
	}
	result.State = StateSuccess
	return result
}

// Name implements the Checker interface.
func (pc *MySQLServerIDChecker) Name() string {
	return "mysql_server_id"
}
