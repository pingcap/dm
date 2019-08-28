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
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/baseconn"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/retry"
	"github.com/pingcap/dm/pkg/terror"
	tutils "github.com/pingcap/dm/pkg/utils"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	tmysql "github.com/pingcap/parser/mysql"
	"go.uber.org/zap"
)

// Conn represents a live DB connection
type Conn struct {
	cfg      *config.SubTaskConfig
	baseConn *baseconn.BaseConn
}

func (conn *Conn) querySQL(ctx *tcontext.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if conn == nil || conn.baseConn == nil {
		return nil, terror.ErrDBUnExpect.Generate("database connection not valid")
	}

	params := retry.Params{
		RetryCount:         10,
		FirstRetryDuration: time.Second,
		BackoffStrategy:    retry.Stable,
		IsRetryableFn: func(retryTime int, err error) bool {
			if retry.IsRetryableError(err) {
				ctx.L().Warn("query statement", zap.Int("retry", retryTime), zap.String("query", query), zap.Reflect("argument", args), log.ShortError(err))
				return true
			}
			return false
		},
	}

	ret, _, err := conn.baseConn.ApplyRetryStrategy(
		ctx,
		params,
		func(ctx *tcontext.Context) (interface{}, error) {
			startTime := time.Now()
			ret, err := conn.baseConn.QuerySQL(ctx, query, args...)
			if err == nil {
				cost := time.Since(startTime)
				queryHistogram.WithLabelValues(conn.cfg.Name).Observe(cost.Seconds())
				if cost > 1 {
					ctx.L().Warn("query statement", zap.String("query", query), zap.Reflect("argument", args), zap.Duration("cost time", cost))
				}
			}
			return ret, err
		})
	if err != nil {
		ctx.L().Error("query statement failed after retry", zap.String("query", query), zap.Reflect("argument", args), log.ShortError(err))
		return nil, err
	}
	return ret.(*sql.Rows), nil
}

func (conn *Conn) executeSQL(ctx *tcontext.Context, queries []string, args ...[]interface{}) error {
	if len(queries) == 0 {
		return nil
	}

	if conn == nil || conn.baseConn == nil {
		return terror.ErrDBUnExpect.Generate("database connection not valid")
	}

	params := retry.Params{
		RetryCount:         10,
		FirstRetryDuration: 2 * time.Second,
		BackoffStrategy:    retry.LinearIncrease,
		IsRetryableFn: func(retryTime int, err error) bool {
			ctx.L().Warn("execute statements", zap.Int("retry", retryTime), zap.Strings("queries", queries), zap.Reflect("arguments", args), log.ShortError(err))
			tidbExecutionErrorCounter.WithLabelValues(conn.cfg.Name).Inc()
			return retry.IsRetryableError(err)
		},
	}
	_, _, err := conn.baseConn.ApplyRetryStrategy(
		ctx,
		params,
		func(ctx *tcontext.Context) (interface{}, error) {
			startTime := time.Now()
			_, err := conn.baseConn.ExecuteSQL(ctx, queries, args...)
			failpoint.Inject("LoadExecCreateTableFailed", func(val failpoint.Value) {
				errCode, err1 := strconv.ParseUint(val.(string), 10, 16)
				if err1 != nil {
					ctx.L().Fatal("failpoint LoadExecCreateTableFailed's value is invalid", zap.String("val", val.(string)))
				}

				if len(queries) == 1 && strings.Contains(queries[0], "CREATE TABLE") {
					err = &mysql.MySQLError{uint16(errCode), ""}
					ctx.L().Warn("executeSQL failed", zap.String("failpoint", "LoadExecCreateTableFailed"), zap.Error(err))
				}
			})
			if err == nil {
				cost := time.Since(startTime)
				txnHistogram.WithLabelValues(conn.cfg.Name).Observe(cost.Seconds())
				if cost > 1 {
					ctx.L().Warn("transaction execute successfully", zap.Duration("cost time", cost))
				}
			}
			return nil, err
		})

	if err != nil {
		ctx.L().Error("execute statements failed after retry", zap.Strings("queries", queries), zap.Reflect("arguments", args), log.ShortError(err))
	}

	return err
}

func (conn *Conn) executeSQLForInsertIgnore(ctx *tcontext.Context, queries []string) error {
	if conn == nil || conn.baseConn == nil {
		return terror.ErrDBUnExpect.Generate("database connection not valid")
	}

	params := retry.Params{
		RetryCount:         10,
		FirstRetryDuration: 2 * time.Second,
		BackoffStrategy:    retry.LinearIncrease,
		IsRetryableFn: func(retryTime int, err error) bool {
			ctx.L().Warn("execute statements", zap.Int("retry", retryTime), zap.Strings("queries", queries), log.ShortError(err))
			tidbExecutionErrorCounter.WithLabelValues(conn.cfg.Name).Inc()
			return retry.IsRetryableError(err)
		},
	}
	_, _, err := conn.baseConn.ApplyRetryStrategy(
		ctx,
		params,
		func(ctx *tcontext.Context) (interface{}, error) {
			var err error
			startTime := time.Now()
			err = executeSQL(ctx, conn, conn.baseConn.GetDb(), queries)
			if err == nil {
				cost := time.Since(startTime)
				txnHistogram.WithLabelValues(conn.cfg.Name).Observe(cost.Seconds())
				if cost > 1 {
					ctx.L().Warn("transaction execute successfully", zap.Duration("cost time", cost))
				}
			}
			return nil, err
		})

	if err != nil {
		ctx.L().Error("execute statements failed after retry", zap.Strings("queries", queries), log.ShortError(err))
	}

	return err
}

func createConn(cfg *config.SubTaskConfig) (*Conn, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&maxAllowedPacket=%d",
		cfg.To.User, cfg.To.Password, cfg.To.Host, cfg.To.Port, *cfg.To.MaxAllowedPacket)
	baseConn, err := baseconn.NewBaseConn(dbDSN, &retry.FiniteRetryStrategy{})
	if err != nil {
		return nil, terror.WithScope(terror.DBErrorAdapt(err, terror.ErrDBDriverError), terror.ScopeDownstream)
	}

	return &Conn{baseConn: baseConn, cfg: cfg}, nil
}

func closeConn(conn *Conn) error {
	if conn.baseConn == nil {
		return nil
	}

	return terror.DBErrorAdapt(conn.baseConn.Close(), terror.ErrDBDriverError)
}

func isErrDBExists(err error) bool {
	return isMySQLError(err, tmysql.ErrDBCreateExists)
}

func isErrTableExists(err error) bool {
	return isMySQLError(err, tmysql.ErrTableExists)
}

func isErrDupEntry(err error) bool {
	return isMySQLError(err, tmysql.ErrDupEntry)
}

func isWriteConflict(err error) bool {
	return isMySQLError(err, tmysql.ErrWriteConflictInTiDB)
}

func isMySQLError(err error, code uint16) bool {
	err = errors.Cause(err)
	e, ok := err.(*mysql.MySQLError)
	return ok && e.Number == code
}

// executeSQL executes SQL from the fetched db connection on real DB
func executeSQL(ctx *tcontext.Context, conn *Conn, db *sql.DB, queries []string) error {
	if db == nil {
		return terror.ErrDBUnExpect.Generate("database connection not valid")
	}

	txn, err := db.Begin()
	if err != nil {
		ctx.L().Error("fail to begin a transaction", zap.String("sqls", fmt.Sprintf("%-.200v", queries)), zap.Error(err))
		return terror.ErrDBExecuteFailed.Delegate(err, "begin")
	}

	for i, query := range queries {
		ctx.L().Debug("execute statement", zap.String("query", query))

		_, err = txn.ExecContext(ctx.Context(), query)
		if err != nil {
			ctx.L().Error("execute statement failed", zap.String("query", query), log.ShortError(err))
			rerr := txn.Rollback()
			if rerr != nil {
				ctx.L().Error("rollback failed", zap.String("query", query), log.ShortError(rerr))
			}
			// we should return the exec err, instead of the rollback rerr.
			return terror.ErrDBExecuteFailed.Delegate(err, query)
		}
		if i == 1 {
			processErrSQL(ctx, conn, txn, query)
		}
	}
	err = txn.Commit()
	if err != nil {
		return terror.ErrDBExecuteFailed.Delegate(err, "commit")
	}
	return nil
}

func processErrSQL(ctx *tcontext.Context, conn *Conn, txn *sql.Tx, sql string) {
	var (
		dupData     string
		table       string
		messages    []string
		tmpDupData  string
		keyName     string
		tmpKeyName  string
		dupDataPair string
		flag        bool
		query       string
	)

	reg := regexp.MustCompile(`^INSERT IGNORE INTO .*?VALUES`)
	if reg.MatchString(sql) {
		tmpTable := reg.FindAllString(sql, 1)
		table = tmpTable[0][20 : len(tmpTable[0])-8]
	} else {
		ctx.L().Warn("get duplicate entry failed, can't get table name", zap.String("sql", sql))
		return
	}

	processTmpErr := func(msg string, err error) {
		ctx.L().Warn("get duplicate entry", zap.String("dupEntry information", msg), zap.Error(err))
	}

	addEscapeCharacter := func(tmpStr string) string {
		// The "'", "\", """ in the data obtained from the error message has no escape character \ , so the escape character must be added after obtaining data
		finalStr := ""
		for i := 0; i < len(tmpStr); i++ {
			if tmpStr[i] == '\'' || tmpStr[i] == '"' || tmpStr[i] == '\\' {
				finalStr += "\\" + string(tmpStr[i])
			} else {
				finalStr += string(tmpStr[i])
			}
		}
		return finalStr
	}

	warns, err := tutils.ShowWarnings(txn)
	if err != nil {
		ctx.L().Warn("get duplicate entry", zap.Error(err))
		return
	}

	for i := 0; i < len(warns); i++ {
		if warns[i].Level == "Warning" && warns[i].Code == tmysql.ErrDupEntry {
			messages = append(messages, warns[i].Message)
		}
	}

	for i := 0; i < len(messages); i++ {
		tmpDupData, tmpKeyName, err = getDupEntry(messages[i]) // Get duplicate data and index key name
		if err != nil {
			processTmpErr(messages[i], err)
			continue
		}

		dupData = addEscapeCharacter(tmpDupData)
		keyName = addEscapeCharacter(tmpKeyName)

		var queryCondition = []tutils.Conditions{
			{"table_name", table},
			{"constraint_name", keyName},
		}
		query = tutils.GetDbInfoFromInfoSchema("column_name", "KEY_COLUMN_USAGE", queryCondition)
		rows, err := txn.Query(query)

		if err != nil {
			processTmpErr(messages[i], terror.DBErrorAdapt(err, terror.ErrDBQueryFailed, query))
			rows.Close()
			continue
		}

		var columnName string
		for rows.Next() {
			err = rows.Scan(&columnName)
			if err != nil {
				processTmpErr(messages[i], terror.DBErrorAdapt(err, terror.ErrDBDriverError))
				flag = true
				break
			}
		}

		if rows.Err() != nil {
			processTmpErr(messages[i], terror.DBErrorAdapt(rows.Err(), terror.ErrDBDriverError))
			rows.Close()
			continue
		}
		rows.Close()

		if flag {
			flag = false
			continue
		}

		queryCondition = []tutils.Conditions{
			{"table_name", table},
		}
		query = tutils.GetDbInfoFromInfoSchema("column_name", "COLUMNS", queryCondition)
		rows, err = txn.Query(query)
		if err != nil {
			processTmpErr(messages[i], terror.DBErrorAdapt(err, terror.ErrDBQueryFailed, query))
			rows.Close()
			continue
		}
		pos := 1

		for rows.Next() {
			var tmpColumnName string
			err = rows.Scan(&tmpColumnName)
			if err != nil {
				processTmpErr(messages[i], terror.DBErrorAdapt(err, terror.ErrDBDriverError))
				flag = true
				break
			}
			if tmpColumnName == columnName {
				break
			}
			pos++
		}

		if rows.Err() != nil {
			processTmpErr(messages[i], terror.DBErrorAdapt(rows.Err(), terror.ErrDBDriverError))
			rows.Close()
			continue
		}
		rows.Close()

		if flag {
			flag = false
			continue
		}

		dupDataPair, err = findDataPair(sql, dupData, pos) // Get the complete data pair
		if err != nil {
			processTmpErr(messages[i], err)
			continue
		}

		if conn.cfg.ErrDataFile != "" {
			err = dupEntryLog(conn.cfg.ErrDataFile, dupDataPair, table, dupData, columnName, strconv.Itoa(pos)) // Write dupdata to the dupEntryLog
			if err != nil {
				processTmpErr(messages[i], err)
				continue
			}
		} else {
			ctx.L().Info("record duplicate entry", zap.String("column", columnName), zap.String("pos", strconv.Itoa(pos)), zap.String("data", dupData), zap.String("statement", "INSERT INTO `"+table+"` VALUES"+dupDataPair+";"))
		}
	}
}

func getDupEntry(mes string) (string, string, error) {
	var (
		keyName    string
		tmpKeyName []string
		dupData    string
		reg        = regexp.MustCompile(`for key '[^ ]*'`)
	)

	if reg.MatchString(mes) {
		tmpKeyName = reg.FindAllString(mes, -1)
		keyName = tmpKeyName[len(tmpKeyName)-1][9 : len(tmpKeyName[len(tmpKeyName)-1])-1]
	} else {
		return "", "", errors.New("Can't find dupEntry key")
	}
	dupData = mes[17 : len(mes)-len(tmpKeyName[len(tmpKeyName)-1])-2]
	return dupData, keyName, nil
}

func findDataPair(target, dupData string, pos int) (string, error) {
	var trxStart int
	var dupDataPair string // Duplicate data pair
	trxStart = 0
	dataIn := false // Use a bool type to record whether the current data ends or not, avoiding encountering ( and ) in the data that causes the current data pair to be considered finished or restarted
	dataNow := 0    // Record the start of each match
	nowPos := 0     // Record the location of the current data, and compare whether the data is equal to dupData only when it is equal to pos
	isSymbol := 0   // Records whether the current data carries ', Used to record data locations and compare data, 0 for stateless, 1 for currently traversing string data, 2 for currently traversing non-string data
	isFind := false // Use a variable to record whether duplicate values have been found
	for i := 0; i < len(target); i++ {
		switch target[i] {
		case '(':
			if dataIn == false {
				trxStart = i             // Left edge position of deleted data
				if target[i+1] != '\'' { // For non-string data after (
					nowPos++
					dataNow = i
					isSymbol = 2
				}
			}
		case '\\': // When an escape character is encountered, need to skip the next character to avoid the effect of characters such as ' "
			i++
			continue
		case '\'':
			if dataIn == false {
				dataIn = true
				isSymbol = 1
				nowPos++
				dataNow = i
			} else {
				dataIn = false
				isSymbol = 0
				if nowPos == pos { // Find a string data
					if target[dataNow+1:i] == dupData {
						isFind = true
					}
				}
			}
		case ',':
			if isSymbol == 0 && dataIn == false && target[i-1] != ')' && target[i+1] != '\'' { // No "," can appear before the beginning of non-string data, and cannot between the string data, but between the "(" and ")", and the last bit cannot be a "'"
				nowPos++
				dataNow = i
				isSymbol = 2
			} else if isSymbol == 2 {
				isSymbol = 0
				if nowPos == pos { // Find a non-string data
					if target[dataNow+1:i] == dupData {
						isFind = true
					}
				}
				if target[i+1] != '\'' { // The "," may be the beginning of another data at the same time as the end of the data
					nowPos++
					dataNow = i
					isSymbol = 2
				}
			}
		case ')':
			if dataIn == false {
				if isSymbol == 2 { // For non-string data before )
					isSymbol = 0
					if nowPos == pos { // Find a non-string data
						if target[dataNow+1:i] == dupData {
							isFind = true
						}
					}
				}
				nowPos = 0
				if isFind == true {
					dupDataPair = target[trxStart : i+1]
					return dupDataPair, nil
				}
			}
		}
	}
	return "", errors.New("Can't find dup Error")
}

func dupEntryLog(errDataFile, dupDataPair, errTable, errData, errColumn, errPos string) error {
	var dupDataFile *os.File

	tmpDir := strings.Split(errDataFile, "/")
	dir := errDataFile[:len(errDataFile)-len(tmpDir[len(tmpDir)-1])-1]

	_, err := os.Stat(dir)
	if os.IsNotExist(err) {
		err = os.Mkdir(dir, 0755)
		if err != nil {
			return err
		}
	}
	_, err = os.Stat(errDataFile) // Detects if the file exists
	if os.IsNotExist(err) {
		dupDataFile, err = os.Create(errDataFile)
		if err != nil {
			return err
		}
	} else {
		dupDataFile, err = os.OpenFile(errDataFile, os.O_APPEND|os.O_WRONLY, 0666)
		if err != nil {
			return err
		}
	}
	errTime := fmt.Sprintf("%d-%d-%d %d:%d:%d", time.Now().Year(), time.Now().Month(), time.Now().Day(), time.Now().Hour(), time.Now().Minute(), time.Now().Second())
	_, err = dupDataFile.WriteString("*********************\n" + "Time: " + errTime + "\n" + "Column: " + errColumn + "\n" + "Data: " + errData + "\n" + "Pos: " + errPos + "\n")
	if err != nil {
		closeErr := dupDataFile.Close()
		if closeErr != nil {
			return closeErr
		}
		return err
	}
	_, err = dupDataFile.WriteString("INSERT INTO `" + errTable + "` VALUES" + dupDataPair + ";\n")
	if err != nil {
		closeErr := dupDataFile.Close()
		if closeErr != nil {
			return closeErr
		}
		return err
	}
	err = dupDataFile.Close()
	if err != nil {
		return err
	}
	return nil
}
