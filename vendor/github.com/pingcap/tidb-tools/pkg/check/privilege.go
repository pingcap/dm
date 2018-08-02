package check

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/parser"
)

/*****************************************************/

// SourcePrivilegeChecker checks data source privileges.
type SourcePrivilegeChecker struct {
	db     *sql.DB
	dbinfo *dbutil.DBConfig
}

// NewSourcePrivilegeChecker returns a Checker.
func NewSourcePrivilegeChecker(db *sql.DB, dbinfo *dbutil.DBConfig) Checker {
	return &SourcePrivilegeChecker{db: db, dbinfo: dbinfo}
}

// Check implements the Checker interface.
// We only check REPLICATION SLAVE, REPLICATION CLIENT, RELOAD privileges.
// REPLICATION SLAVE and REPLICATION CLIENT are required.
// RELOAD is strongly suggested to have.
func (pc *SourcePrivilegeChecker) Check(ctx context.Context) *Result {
	result := &Result{
		Name:  pc.Name(),
		Desc:  "checks data source privileges",
		State: StateFailure,
		Extra: fmt.Sprintf("%s:%d", pc.dbinfo.Host, pc.dbinfo.Port),
	}

	grants, err := dbutil.ShowGrants(ctx, pc.db, "", "")
	if err != nil {
		markCheckError(result, err)
		return result
	}
	if len(grants) == 0 {
		result.ErrorMsg = "there is no such grant defined for current user on host '%'"
		return result
	}

	log.Debugf("grants %+v", grants)

	// get username and hostname
	node, err := parser.New().ParseOneStmt(grants[0], "", "")
	if err != nil {
		result.ErrorMsg = errors.ErrorStack(errors.Annotatef(err, "grants[0] %s", grants[0]))
		return result
	}
	grantStmt, ok := node.(*ast.GrantStmt)
	if !ok {
		result.ErrorMsg = fmt.Sprintf("%s is not grant stament", grants[0])
		return result
	}

	if len(grantStmt.Users) == 0 {
		result.ErrorMsg = fmt.Sprintf("grant has not user %s", grantStmt.Text())
		return result
	}

	var (
		hasPrivilegeOfReplicationSlave  bool
		hasPrivilegeOfReplicationClient bool
		hasPrivilegeOfReload            bool
	)

	// TODO: user tidb parser(which not works very well now)
	for _, grant := range grants {
		if strings.Contains(grant, "ALL PRIVILEGES") {
			result.State = StateSuccess
			return result
		}
		if strings.Contains(grant, "REPLICATION SLAVE") {
			hasPrivilegeOfReplicationSlave = true
		}
		if strings.Contains(grant, "REPLICATION CLIENT") {
			hasPrivilegeOfReplicationClient = true
		}
		if strings.Contains(grant, "RELOAD") {
			hasPrivilegeOfReload = true
		}
	}

	user := grantStmt.Users[0]
	lackOfPrivileges := make([]string, 0, 3)
	if !hasPrivilegeOfReplicationSlave {
		lackOfPrivileges = append(lackOfPrivileges, "REPLICATION SLAVE")
	}
	if !hasPrivilegeOfReplicationClient {
		lackOfPrivileges = append(lackOfPrivileges, "REPLICATION CLIENT")
	}
	if !hasPrivilegeOfReload {
		lackOfPrivileges = append(lackOfPrivileges, "RELOAD")
	}
	if len(lackOfPrivileges) != 0 {
		privileges := strings.Join(lackOfPrivileges, ",")
		result.ErrorMsg = fmt.Sprintf("lack of %s privilege", privileges)
		result.Instruction = fmt.Sprintf("GRANT %s ON *.* TO '%s'@'%s';", privileges, user.User.Username, "%")
		if !hasPrivilegeOfReload && hasPrivilegeOfReplicationClient && hasPrivilegeOfReplicationSlave {
			result.State = StateWarning
			result.Instruction += "Or you use no-lock option to dump data in next step"
		}
		return result
	}

	result.State = StateSuccess
	return result
}

// Name implements the Checker interface.
func (pc *SourcePrivilegeChecker) Name() string {
	return "source_db_privilege"
}
