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

// ScanRow scans rows into a map.
func ScanRow(rows *sql.Rows) (map[string][]byte, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Trace(err)
	}

	colVals := make([][]byte, len(cols))
	colValsI := make([]interface{}, len(colVals))
	for i := range colValsI {
		colValsI[i] = &colVals[i]
	}

	err = rows.Scan(colValsI...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	result := make(map[string][]byte)
	for i := range colVals {
		result[cols[i]] = colVals[i]
	}

	return result, nil
}
