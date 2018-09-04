package dbutil

import (
	"database/sql"

	"github.com/juju/errors"
)

// ScanRowsToInterfaces scans rows to interface arrary.
func ScanRowsToInterfaces(rows *sql.Rows) ([][]interface{}, error) {
	var rowsData [][]interface{}
	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Trace(err)
	}

	for rows.Next() {
		colVals := make([]interface{}, len(cols))

		err = rows.Scan(colVals...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		rowsData = append(rowsData, colVals)
	}

	return rowsData, nil
}

// ScanRow scans rows into a map, and another map specify the value is null or not.
func ScanRow(rows *sql.Rows) (map[string][]byte, map[string]bool, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	colVals := make([][]byte, len(cols))
	colValsI := make([]interface{}, len(colVals))
	for i := range colValsI {
		colValsI[i] = &colVals[i]
	}

	err = rows.Scan(colValsI...)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	result := make(map[string][]byte)
	null := make(map[string]bool)
	for i := range colVals {
		result[cols[i]] = colVals[i]
		null[cols[i]] = (colVals[i] == nil)
	}

	return result, null, nil
}
