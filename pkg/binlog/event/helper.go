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
	gmysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"

	"github.com/pingcap/dm/pkg/gtid"
)

// GTIDsFromPreviousGTIDsEvent get GTID set from a PreviousGTIDsEvent.
func GTIDsFromPreviousGTIDsEvent(e *replication.BinlogEvent) (gtid.Set, error) {
	var payload []byte
	switch ev := e.Event.(type) {
	case *replication.GenericEvent:
		payload = ev.Data
	default:
		return nil, errors.Errorf("PreviousGTIDsEvent should be a GenericEvent in go-mysql")
	}

	if e.Header.EventType != replication.PREVIOUS_GTIDS_EVENT {
		return nil, errors.Errorf("invalid event type %d, expect %d", e.Header.EventType, replication.PREVIOUS_GTIDS_EVENT)
	}

	set, err := gmysql.DecodeMysqlGTIDSet(payload)
	if err != nil {
		return nil, errors.Annotatef(err, "decode from % X", payload)
	}

	// always MySQL for PreviousGTIDsEvent
	gSet, err := gtid.ParserGTID(gmysql.MySQLFlavor, "")
	if err != nil {
		return nil, errors.Annotatef(err, "parse empty GTID set")
	}
	err = gSet.Set(set)
	if err != nil {
		return nil, errors.Annotatef(err, "replace GTID set with set %v", set)
	}

	return gSet, nil
}
