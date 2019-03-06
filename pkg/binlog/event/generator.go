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
	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/replication"

	"github.com/pingcap/dm/pkg/gtid"
)

// Generator represents a binlog events generator.
type Generator struct {
	flavor        string
	serverID      uint32
	latestPos     uint32
	latestGTID    gtid.Set
	previousGTIDs gtid.Set
}

// NewGenerator creates a new instance of Generator.
func NewGenerator(flavor string, serverID uint32, latestPos uint32, latestGTID gtid.Set, previousGTIDs gtid.Set) *Generator {
	return &Generator{
		flavor:        flavor,
		serverID:      serverID,
		latestPos:     latestPos,
		latestGTID:    latestGTID,
		previousGTIDs: previousGTIDs,
	}
}

// GenFileHeader generates a binlog file header, including to PreviousGTIDsEvent/MariadbGTIDListEvent.
// for MySQL:
//   1. BinLogFileHeader, [ fe `bin` ]
//   2. FormatDescriptionEvent
//   3. PreviousGTIDsEvent
// for MariaDB:
//   1. BinLogFileHeader, [ fe `bin` ]
//   2. FormatDescriptionEvent
//   3. MariadbGTIDListEvent
func (g *Generator) GenFileHeader() ([]*replication.BinlogEvent, []byte, error) {
	events, data, err := GenCommonFileHeader(g.flavor, g.serverID, g.previousGTIDs)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	g.latestPos = uint32(len(data)) // if generate a binlog file header then reset latest pos
	return events, data, nil
}

// GenCreateDatabaseEvents generates binlog events for `CREATE DATABASE`.
// events: [GTIDEvent, QueryEvent]
func (g *Generator) GenCreateDatabaseEvents(schema string) ([]*replication.BinlogEvent, []byte, error) {
	result, err := GenCreateDatabaseEvents(g.flavor, g.serverID, g.latestPos, g.latestGTID, schema)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	g.updateLatestPosGTID(result.LatestPos, result.LatestGTID)
	return result.Events, result.Data, nil
}

// GenDropDatabaseEvents generates binlog events for `DROP DATABASE`.
// events: [GTIDEvent, QueryEvent]
func (g *Generator) GenDropDatabaseEvents(schema string) ([]*replication.BinlogEvent, []byte, error) {
	result, err := GenDropDatabaseEvents(g.flavor, g.serverID, g.latestPos, g.latestGTID, schema)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	g.updateLatestPosGTID(result.LatestPos, result.LatestGTID)
	return result.Events, result.Data, nil
}

// GenCreateTableEvents generates binlog events for `CREATE TABLE`.
// events: [GTIDEvent, QueryEvent]
func (g *Generator) GenCreateTableEvents(schema string, query string) ([]*replication.BinlogEvent, []byte, error) {
	result, err := GenCreateTableEvents(g.flavor, g.serverID, g.latestPos, g.latestGTID, schema, query)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	g.updateLatestPosGTID(result.LatestPos, result.LatestGTID)
	return result.Events, result.Data, nil
}

// GenDropTableEvents generates binlog events for `DROP TABLE`.
// events: [GTIDEvent, QueryEvent]
func (g *Generator) GenDropTableEvents(schema string, table string) ([]*replication.BinlogEvent, []byte, error) {
	result, err := GenDropTableEvents(g.flavor, g.serverID, g.latestPos, g.latestGTID, schema, table)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	g.updateLatestPosGTID(result.LatestPos, result.LatestGTID)
	return result.Events, result.Data, nil
}

// GenDMLEvents generates binlog events for `INSERT`/`UPDATE`/`DELETE`.
// events: [GTIDEvent, QueryEvent, TableMapEvent, RowsEvent, ..., XIDEvent]
// NOTE: multi <TableMapEvent, RowsEvent> pairs can be in events.
func (g *Generator) GenDMLEvents(eventType replication.EventType, xid uint64, dmlData []*DMLData) ([]*replication.BinlogEvent, []byte, error) {
	result, err := GenDMLEvents(g.flavor, g.serverID, g.latestPos, g.latestGTID, eventType, xid, dmlData)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	g.updateLatestPosGTID(result.LatestPos, result.LatestGTID)
	return result.Events, result.Data, nil
}

func (g *Generator) updateLatestPosGTID(latestPos uint32, latestGTID gtid.Set) {
	g.latestPos = latestPos
	g.latestGTID = latestGTID
}
