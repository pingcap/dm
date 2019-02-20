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

// binlog events generator for MySQL used to generate some binlog events for tests.
// Readability takes precedence over performance.

package event

import (
	"bytes"
	"encoding/binary"

	"github.com/pingcap/errors"
	gmysql "github.com/siddontang/go-mysql/mysql"
)

// encodeTableMapColumnMeta generates the column_meta_def according to the column_type_def.
// NOTE: we should pass more arguments for some type def later, now simply hard-code them.
// ref: https://dev.mysql.com/doc/internals/en/table-map-event.html
// ref: https://github.com/siddontang/go-mysql/blob/88e9cd7f6643b246b4dcc0e3206e9a169dd0ac96/replication/row_event.go#L100
func encodeTableMapColumnMeta(columnType []byte) ([]byte, error) {
	buf := new(bytes.Buffer)
	for _, t := range columnType {
		switch t {
		case gmysql.MYSQL_TYPE_STRING:
			buf.WriteByte(0xfe) // real type
			buf.WriteByte(0xff) // pack or field length
		case gmysql.MYSQL_TYPE_NEWDECIMAL:
			buf.WriteByte(0x12) // precision, 18
			buf.WriteByte(0x09) // decimals, 9
		case gmysql.MYSQL_TYPE_VAR_STRING, gmysql.MYSQL_TYPE_VARCHAR, gmysql.MYSQL_TYPE_BIT:
			buf.WriteByte(0xff)
			buf.WriteByte(0xff)
		case gmysql.MYSQL_TYPE_BLOB, gmysql.MYSQL_TYPE_DOUBLE, gmysql.MYSQL_TYPE_FLOAT, gmysql.MYSQL_TYPE_GEOMETRY, gmysql.MYSQL_TYPE_JSON,
			gmysql.MYSQL_TYPE_TIME2, gmysql.MYSQL_TYPE_DATETIME2, gmysql.MYSQL_TYPE_TIMESTAMP2:
			buf.WriteByte(0xff)
		case gmysql.MYSQL_TYPE_NEWDATE, gmysql.MYSQL_TYPE_ENUM, gmysql.MYSQL_TYPE_SET, gmysql.MYSQL_TYPE_TINY_BLOB, gmysql.MYSQL_TYPE_MEDIUM_BLOB, gmysql.MYSQL_TYPE_LONG_BLOB:
			return nil, errors.NotSupportedf("column type %d in binlog", t)
		}
	}
	return gmysql.PutLengthEncodedString(buf.Bytes()), nil
}

// decodeTableMapColumnMeta generates the column_meta_def to uint16 slices.
// ref: https://github.com/siddontang/go-mysql/blob/88e9cd7f6643b246b4dcc0e3206e9a169dd0ac96/replication/row_event.go#L100
func decodeTableMapColumnMeta(data []byte, columnType []byte) ([]uint16, error) {
	pos := 0
	columnMeta := make([]uint16, len(columnType))
	for i, t := range columnType {
		switch t {
		case gmysql.MYSQL_TYPE_STRING:
			var x = uint16(data[pos]) << 8 //real type
			x += uint16(data[pos+1])       //pack or field length
			columnMeta[i] = x
			pos += 2
		case gmysql.MYSQL_TYPE_NEWDECIMAL:
			var x = uint16(data[pos]) << 8 //precision
			x += uint16(data[pos+1])       //decimals
			columnMeta[i] = x
			pos += 2
		case gmysql.MYSQL_TYPE_VAR_STRING, gmysql.MYSQL_TYPE_VARCHAR, gmysql.MYSQL_TYPE_BIT:
			columnMeta[i] = binary.LittleEndian.Uint16(data[pos:])
			pos += 2
		case gmysql.MYSQL_TYPE_BLOB, gmysql.MYSQL_TYPE_DOUBLE, gmysql.MYSQL_TYPE_FLOAT, gmysql.MYSQL_TYPE_GEOMETRY, gmysql.MYSQL_TYPE_JSON,
			gmysql.MYSQL_TYPE_TIME2, gmysql.MYSQL_TYPE_DATETIME2, gmysql.MYSQL_TYPE_TIMESTAMP2:
			columnMeta[i] = uint16(data[pos])
			pos++
		case gmysql.MYSQL_TYPE_NEWDATE, gmysql.MYSQL_TYPE_ENUM, gmysql.MYSQL_TYPE_SET, gmysql.MYSQL_TYPE_TINY_BLOB, gmysql.MYSQL_TYPE_MEDIUM_BLOB, gmysql.MYSQL_TYPE_LONG_BLOB:
			return nil, errors.NotSupportedf("column type %d in binlog", t)
		default:
			columnMeta[i] = 0
		}
	}

	return columnMeta, nil
}

// bitmapByteSize returns the byte length of bitmap for columnCount.
func bitmapByteSize(columnCount int) int {
	return int(columnCount+7) / 8
}

// nullBytes returns a n-length null bytes slice
func nullBytes(n int) []byte {
	buf := new(bytes.Buffer)
	for i := 0; i < n; i++ {
		buf.WriteByte(0x00)
	}
	return buf.Bytes()
}

// fullBytes returns a n-length full bytes slice (all bits are set)
func fullBytes(n int) []byte {
	buf := new(bytes.Buffer)
	for i := 0; i < n; i++ {
		buf.WriteByte(0xff)
	}
	return buf.Bytes()
}

// encodeColumnValue encodes value to bytes
// ref: https://github.com/siddontang/go-mysql/blob/88e9cd7f6643b246b4dcc0e3206e9a169dd0ac96/replication/row_event.go#L368
// TODO: add more tp support
func encodeColumnValue(v interface{}, tp byte, meta uint16) ([]byte, error) {
	var buf = new(bytes.Buffer)
	switch tp {
	case gmysql.MYSQL_TYPE_NULL:
		return nil, nil
	case gmysql.MYSQL_TYPE_LONG:
		err := binary.Write(buf, binary.LittleEndian, v.(int32))
		return buf.Bytes(), errors.Annotatef(err, "value %v with type %d", v, tp)
	}
	return nil, nil
}

// combineHeaderPayload combines header, postHeader and payload together.
func combineHeaderPayload(buf *bytes.Buffer, header, postHeader, payload []byte) error {
	err := binary.Write(buf, binary.LittleEndian, header)
	if err != nil {
		return errors.Annotatef(err, "write event header % X", header)
	}

	if len(postHeader) > 0 { // postHeader maybe empty
		err = binary.Write(buf, binary.LittleEndian, postHeader)
		if err != nil {
			return errors.Annotatef(err, "write event post-header % X", postHeader)
		}
	}

	err = binary.Write(buf, binary.LittleEndian, payload)
	if err != nil {
		return errors.Annotatef(err, "write event payload % X", payload)
	}

	return nil
}
