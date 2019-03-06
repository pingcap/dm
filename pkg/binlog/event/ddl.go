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

package event

import (
	"bytes"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/replication"

	"github.com/pingcap/dm/pkg/gtid"
)

// GenCreateDatabase generates binlog events for `CREATE DATABASE`.
// events: [GTIDEvent, QueryEvent]
func GenCreateDatabase(flavor string, serverID uint32, latestPos uint32, latestGTID gtid.Set, schema string) (*DDLDMLResult, error) {
	query := fmt.Sprintf("CREATE DATABASE `%s`", schema)
	return genDDLEvents(flavor, serverID, latestPos, latestGTID, schema, query)
}

// GenDropDatabase generates binlog events for `DROP DATABASE`.
// events: [GTIDEvent, QueryEvent]
func GenDropDatabase(flavor string, serverID uint32, latestPos uint32, latestGTID gtid.Set, schema string) (*DDLDMLResult, error) {
	query := fmt.Sprintf("DROP DATABASE `%s`", schema)
	return genDDLEvents(flavor, serverID, latestPos, latestGTID, schema, query)
}

// GenCreateTable generates binlog events for `CREATE TABLE`.
// events: [GTIDEvent, QueryEvent]
// NOTE: we do not support all `column type` and `column meta` for DML now, so the caller should restrict the `query` statement.
func GenCreateTable(flavor string, serverID uint32, latestPos uint32, latestGTID gtid.Set, schema string, query string) (*DDLDMLResult, error) {
	return genDDLEvents(flavor, serverID, latestPos, latestGTID, schema, query)
}

// GenDropTable generates binlog events for `DROP TABLE`.
// events: [GTIDEvent, QueryEvent]
func GenDropTable(flavor string, serverID uint32, latestPos uint32, latestGTID gtid.Set, schema string, table string) (*DDLDMLResult, error) {
	query := fmt.Sprintf("DROP TABLE `%s`.`%s`", schema, table)
	return genDDLEvents(flavor, serverID, latestPos, latestGTID, schema, query)
}

func genDDLEvents(flavor string, serverID uint32, latestPos uint32, latestGTID gtid.Set, schema string, query string) (*DDLDMLResult, error) {
	// GTIDEvent, increase GTID first
	latestGTID, err := GTIDIncrease(flavor, latestGTID)
	if err != nil {
		return nil, errors.Annotatef(err, "increase GTID %s", latestGTID)
	}
	gtidEv, err := GenCommonGTIDEvent(flavor, serverID, latestPos, latestGTID)
	if err != nil {
		return nil, errors.Annotatef(err, "generate GTIDEvent")
	}
	latestPos = gtidEv.Header.LogPos

	// QueryEvent
	header := &replication.EventHeader{
		Timestamp: uint32(time.Now().Unix()),
		ServerID:  serverID,
		Flags:     defaultHeaderFlags,
	}
	queryEv, err := GenQueryEvent(header, latestPos, defaultSlaveProxyID, defaultExecutionTime, defaultErrorCode, defaultStatusVars, []byte(schema), []byte(query))
	if err != nil {
		return nil, errors.Annotatef(err, "generate QueryEvent for schema %s, query %s", schema, query)
	}
	latestPos = queryEv.Header.LogPos

	var buf bytes.Buffer
	_, err = buf.Write(gtidEv.RawData)
	if err != nil {
		return nil, errors.Annotatef(err, "write GTIDEvent data % X", gtidEv.RawData)
	}
	_, err = buf.Write(queryEv.RawData)
	if err != nil {
		return nil, errors.Annotatef(err, "write QueryEvent data % X", queryEv.RawData)
	}

	return &DDLDMLResult{
		Events:     []*replication.BinlogEvent{gtidEv, queryEv},
		Data:       buf.Bytes(),
		LatestPos:  latestPos,
		LatestGTID: latestGTID,
	}, nil
}
