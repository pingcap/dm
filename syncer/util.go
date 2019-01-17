package syncer

import (
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/parser/ast"
)

func toBinlogType(bt string) BinlogType {
	bt = strings.ToLower(bt)
	switch bt {
	case "local":
		return LocalBinlog
	case "remote":
		return RemoteBinlog
	default:
		return RemoteBinlog
	}
}

// tableNameForDML gets table name from INSERT/UPDATE/DELETE statement
func tableNameForDML(dml ast.DMLNode) (schema, table string, err error) {
	switch dml.(type) {
	case *ast.InsertStmt:
		is := dml.(*ast.InsertStmt)
		if is.Table == nil || is.Table.TableRefs == nil || is.Table.TableRefs.Left == nil {
			return "", "", errors.NotValidf("INSERT statement %s", is.Text())
		}
		schema, table, err = tableNameResultSet(is.Table.TableRefs.Left)
		return schema, table, errors.Annotatef(err, "INSERT statement %s", is.Text())
	case *ast.UpdateStmt:
		us := dml.(*ast.UpdateStmt)
		if us.TableRefs == nil || us.TableRefs.TableRefs == nil || us.TableRefs.TableRefs.Left == nil {
			return "", "", errors.NotValidf("UPDATE statement %s", us.Text())
		}
		schema, table, err = tableNameResultSet(us.TableRefs.TableRefs.Left)
		return schema, table, errors.Annotatef(err, "UPDATE statement %s", us.Text())
	case *ast.DeleteStmt:
		ds := dml.(*ast.DeleteStmt)
		if ds.TableRefs == nil || ds.TableRefs.TableRefs == nil || ds.TableRefs.TableRefs.Left == nil {
			return "", "", errors.NotValidf("DELETE statement %s", ds.Text())
		}
		schema, table, err = tableNameResultSet(ds.TableRefs.TableRefs.Left)
		return schema, table, errors.Annotatef(err, "DELETE statement %s", ds.Text())
	}
	return "", "", errors.NotSupportedf("DMLNode %v", dml)
}

func tableNameResultSet(rs ast.ResultSetNode) (schema, table string, err error) {
	ts, ok := rs.(*ast.TableSource)
	if !ok {
		return "", "", errors.NotValidf("ResultSetNode %s", rs.Text())
	}
	tn, ok := ts.Source.(*ast.TableName)
	if !ok {
		return "", "", errors.NotValidf("TableSource %s", ts.Text())
	}
	return tn.Schema.O, tn.Name.O, nil
}
