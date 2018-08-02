package dbutil

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/model"
)

// IndexInfo contains information of table index.
type IndexInfo struct {
	Table       string
	NoneUnique  bool
	KeyName     string
	SeqInIndex  int
	ColumnName  string
	Cardinality int
}

// ShowIndex returns result of executing `show index`
func ShowIndex(ctx context.Context, db *sql.DB, schemaName string, table string) ([]*IndexInfo, error) {
	/*
		show index example result:
		mysql> show index from test;
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
		| Table | Non_unique | Key_name | Seq_in_index | Column_name | Collation | Cardinality | Sub_part | Packed | Null | Index_type | Comment | Index_comment |
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
		| test  | 0          | PRIMARY  | 1            | id          | A         | 0           | NULL     | NULL   |      | BTREE      |         |               |
		| test  | 0          | aid      | 1            | aid         | A         | 0           | NULL     | NULL   | YES  | BTREE      |         |               |
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
	*/
	indices := make([]*IndexInfo, 0, 3)
	query := fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", schemaName, table)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	for rows.Next() {
		fields, err1 := ScanRow(rows)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		seqInINdex, err1 := strconv.Atoi(string(fields["Seq_in_index"]))
		if err != nil {
			return nil, errors.Trace(err1)
		}
		cardinality, err1 := strconv.Atoi(string(fields["Cardinality"]))
		if err != nil {
			return nil, errors.Trace(err1)
		}
		index := &IndexInfo{
			Table:       string(fields["Table"]),
			NoneUnique:  string(fields["Non_unique"]) == "1",
			KeyName:     string(fields["Key_name"]),
			ColumnName:  string(fields["Column_name"]),
			SeqInIndex:  seqInINdex,
			Cardinality: cardinality,
		}
		indices = append(indices, index)
	}

	return indices, nil
}

// FindSuitableIndex returns first column of a suitable index.
// The priority is
// * primary key
// * unique key
// * normal index which has max cardinality
func FindSuitableIndex(ctx context.Context, db *sql.DB, schemaName string, tableInfo *model.TableInfo) (*model.ColumnInfo, error) {
	// find primary key
	for _, index := range tableInfo.Indices {
		if index.Primary {
			return FindColumnByName(tableInfo.Columns, index.Columns[0].Name.O), nil
		}
	}

	// no primary key found, seek unique index
	for _, index := range tableInfo.Indices {
		if index.Unique {
			return FindColumnByName(tableInfo.Columns, index.Columns[0].Name.O), nil
		}
	}

	// no unique index found, seek index with max cardinality
	indices, err := ShowIndex(ctx, db, schemaName, tableInfo.Name.O)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var c *model.ColumnInfo
	var maxCardinality int
	for _, indexInfo := range indices {
		// just use the first column in the index, otherwise can't hit the index when select
		if indexInfo.SeqInIndex != 1 {
			continue
		}

		if indexInfo.Cardinality > maxCardinality {
			column := FindColumnByName(tableInfo.Columns, indexInfo.ColumnName)
			if column == nil {
				return nil, errors.NotFoundf("column %s in %s.%s", indexInfo.ColumnName, schemaName, tableInfo.Name.O)
			}
			maxCardinality = indexInfo.Cardinality
			c = column
		}
	}

	return c, nil
}

// SelectUniqueOrderKey returns some columns for order by condition.
func SelectUniqueOrderKey(tbInfo *model.TableInfo) ([]string, []*model.ColumnInfo) {
	keys := make([]string, 0, 2)
	keyCols := make([]*model.ColumnInfo, 0, 2)

	for _, index := range tbInfo.Indices {
		if index.Primary {
			for _, indexCol := range index.Columns {
				keys = append(keys, indexCol.Name.O)
				keyCols = append(keyCols, tbInfo.Columns[indexCol.Offset])
			}
		}
	}

	if len(keys) == 0 {
		// no primary key found, use all fields as order by key
		for _, col := range tbInfo.Columns {
			keys = append(keys, col.Name.O)
			keyCols = append(keyCols, col)
		}
	}

	return keys, keyCols
}
