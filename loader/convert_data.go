package loader

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"unsafe"

	"github.com/juju/errors"
	cm "github.com/pingcap/tidb-tools/pkg/column-mapping"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/parser"
)

const (
	defReadBlockSize int64 = 1024 * 8
)

func bytes2str(bs []byte) string {
	return *(*string)(unsafe.Pointer(&bs))
}

// poor man's parse insert stmtement code
// learn from tidb-lightning and refactor it as format of mydumper file
// https://github.com/maxbube/mydumper/blob/master/mydumper.c#L2853
// later let it a package
func parseInsertStmt(sql []byte, table *tableInfo, columnMapping *cm.Mapping) ([][]string, error) {
	var s, e, size int
	var rows = make([][]string, 0, 1024)

	for {
		sql = sql[s:]
		size = len(sql)

		// seek start "("
		s = bytes.IndexByte(sql, '(')
		if s < 0 {
			break
		}

		// seek one line, it's one row
		for e = s + 3; e < size; e++ {
			if sql[e] == '\n' && (sql[e-1] == ',' || sql[e-1] == ';') && sql[e-2] == ')' {
				break
			}
		}
		if e == size {
			return nil, errors.New("not found cooresponding ending of sql: ')'")
		}

		rp := e - 2
		// extract columns' values
		row, err := parseRowValues(sql[s+1:rp], table, columnMapping)
		if err != nil {
			return nil, errors.Trace(err)
		}
		rows = append(rows, row)

		s = e + 1
		if s >= size {
			break
		}
	}

	return rows, nil
}

func parseRowValues(str []byte, table *tableInfo, columnMapping *cm.Mapping) ([]string, error) {
	// values are seperated by comma, but we can not split using comma directly
	// string is enclosed by single quote

	// a poor implementation, may be more robust later.
	values := make([]interface{}, 0, len(table.columnNameList))
	row := make([]string, 0, len(table.columnNameList))
	isChars := make([]byte, 0, len(table.columnNameList))
	size := len(str)
	var ch byte
	for i := 0; i < size; {
		ch = str[i]
		if ch == ' ' || ch == '\n' {
			i++
			continue
		}

		if ch != '\'' && ch != '"' {
			// no string, read until comma
			j := i + 1
			for ; j < size && str[j] != ','; j++ {
			}

			val := bytes.TrimSpace(str[i:j])

			values = append(values, bytes2str(val)) // ?? no need to trim ??
			isChars = append(isChars, 0x0)
			// skip ,
			i = j + 1
		} else {
			// read string until another single quote
			j := i + 1

			sch := ch
			for j < size {
				if str[j] == '\\' {
					// skip escaped character
					j += 2
					continue
				} else if str[j] == sch {
					// matchup ending
					break
				} else {
					j++
				}
			}

			if j >= size {
				return nil, errors.New("parse quote values error")
			}

			val := str[i+1 : j]
			values = append(values, bytes2str(val))
			isChars = append(isChars, sch)

			i = j + 2 // skip ' and ,
		}
	}

	if columnMapping != nil {
		var err error
		values, _, err = columnMapping.HandleRowValue(table.sourceSchema, table.sourceTable, table.columnNameList, values)
		if err != nil {
			return nil, errors.Annotatef(err, "mapping row data %v for table %+v", values, table)
		}
	}

	for i := range values {
		val, ok := values[i].(string)
		if !ok {
			panic(fmt.Sprintf("%v is not string", values[i]))
		}
		if isChars[i] != 0x0 {
			columnVal := make([]byte, 0, len(val)+2)
			columnVal = append(columnVal, isChars[i])
			columnVal = append(columnVal, val...)
			columnVal = append(columnVal, isChars[i])
			row = append(row, string(columnVal))
		} else {
			row = append(row, val)
		}

	}

	return row, nil
}

// ExportStatement returns schema structure in sqlFile
func ExportStatement(sqlFile string) ([]byte, error) {
	fd, err := os.Open(sqlFile)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer fd.Close()

	br := bufio.NewReader(fd)
	f, err := os.Stat(sqlFile)
	if err != nil {
		return nil, errors.Trace(err)
	}

	data := make([]byte, 0, f.Size()+1)
	buffer := make([]byte, 0, f.Size()+1)
	for {
		line, err := br.ReadString('\n')
		if errors.Cause(err) == io.EOF {
			break
		}

		line = strings.TrimSpace(line[:len(line)-1])
		if len(line) == 0 {
			continue
		}

		buffer = append(buffer, []byte(line)...)
		if buffer[len(buffer)-1] == ';' {
			statment := string(buffer)
			if !(strings.HasPrefix(statment, "/*") && strings.HasSuffix(statment, "*/;")) {
				data = append(data, buffer...)
			}
			buffer = buffer[:0]
		} else {
			buffer = append(buffer, '\n')
		}
	}

	return data, nil
}

func tableName(schema, table string) string {
	return fmt.Sprintf("`%s`.`%s`", schema, table)
}

func parseTable(r *router.Table, schema, table, file string) (*tableInfo, error) {
	statement, err := ExportStatement(file)
	if err != nil {
		return nil, errors.Annotatef(err, "read table info from file %s", file)
	}

	stmts, err := parser.New().Parse(string(statement), "", "")
	if err != nil {
		return nil, errors.Annotatef(err, "parser statement %s", statement)
	}

	var (
		ct                 *ast.CreateTableStmt
		hasCreateTableStmt bool
	)
	for _, stmt := range stmts {
		ct, hasCreateTableStmt = stmt.(*ast.CreateTableStmt)
		if hasCreateTableStmt {
			break
		}
	}
	if !hasCreateTableStmt {
		return nil, errors.Errorf("statement %s for %s/%s is not create table statement", statement, schema, table)
	}

	columns := make([]string, 0, len(ct.Cols))
	for _, col := range ct.Cols {
		columns = append(columns, col.Name.Name.O)
	}

	dstSchema, dstTable := fetchMatchedLiteral(r, schema, table)
	return &tableInfo{
		sourceSchema:   schema,
		sourceTable:    table,
		targetSchema:   dstSchema,
		targetTable:    dstTable,
		columnNameList: columns,
		insertHeadStmt: fmt.Sprintf("INSERT INTO `%s` VALUES", dstTable),
	}, nil
}

// refine it later
func reassemble(data []byte, table *tableInfo, columnMapping *cm.Mapping) (string, error) {
	rows, err := parseInsertStmt(data, table, columnMapping)
	if err != nil {
		return "", errors.Trace(err)
	}

	query := bytes.NewBuffer(make([]byte, 0, len(data)))
	fmt.Fprint(query, table.insertHeadStmt)
	seq := ","

	for i, row := range rows {
		if i == len(rows)-1 {
			seq = ";"
		}

		fmt.Fprintf(query, "(%s)%s", strings.Join(row, ","), seq)
	}

	return query.String(), nil
}
