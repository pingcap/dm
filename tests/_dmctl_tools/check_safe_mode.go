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
//
// script used for checking safeMode in DML is as expected
//
// In test case we send `update` SQL before, in and after a specific sharding DDL
// procedure, and this script checks that update binlog event added during sharding
// DDL not synced phase has safeMode true, otherwise the safeMode is false.
package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"strings"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/mysql"

	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/tracer"
)

const (
	opTypeUpdate = 2
	opTypeDDL    = 4
)

func scan_events(offset, limit int) ([]byte, error) {
	uri := fmt.Sprintf("http://127.0.0.1:8264/events/scan?offset=%d&limit=%d", offset, limit)
	resp, err := http.Get(uri)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return body, nil
}

func traceSourceExtract(ev *pb.SyncerBinlogEvent) string {
	return strings.Split(ev.Base.TraceID, ".")[0]
}

func binlogPosition(ev *pb.SyncerBinlogEvent) mysql.Position {
	return mysql.Position{
		Name: ev.State.CurrentPos.Name,
		Pos:  ev.State.CurrentPos.Pos,
	}
}

func checkSafeMode(obtained, expected bool, ev, startEv, endEv *pb.SyncerBinlogEvent) {
	if obtained != expected {
		panic(fmt.Sprintf("safe_mode should be %v, got %v\nev: %v\nstart_ev: %v\nend_ev: %v\n",
			expected, obtained, ev, startEv, endEv))
	}
}

type BinlogEventSlice []*pb.SyncerBinlogEvent

func (c BinlogEventSlice) Len() int {
	return len(c)
}

func (c BinlogEventSlice) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

func (c BinlogEventSlice) Less(i, j int) bool {
	posi := binlogPosition(c[i])
	posj := binlogPosition(c[j])
	return posi.Compare(posj) < 0
}

func main() {
	var (
		raw    []byte
		err    error
		offset int
		limit  int = 10
		count  int
	)
	checkInstance := fmt.Sprintf("mysql-replica-0%s", os.Args[1])
	updateEvents := make(map[string][]*pb.SyncerBinlogEvent)
	ddlEvents := make(map[string][]*pb.SyncerBinlogEvent)
	latestDDL := make(map[string][]*pb.SyncerBinlogEvent)
	fmt.Printf("check safe mode for instance %s\n", checkInstance)
	for {
		raw, err = scan_events(offset, limit)
		if err != nil {
			panic(errors.ErrorStack(err))
		}
		var events [][]tracer.TraceEvent
		err = json.Unmarshal(raw, &events)
		if err != nil {
			panic(err)
		}

		for _, event_group := range events {
			for _, e := range event_group {
				if e.Type == pb.TraceType_BinlogEvent {
					ev := &pb.SyncerBinlogEvent{}
					jsonE, _ := json.Marshal(e.Event)
					err = jsonpb.UnmarshalString(string(jsonE), ev)
					if err != nil {
						panic(err)
					}
					worker := traceSourceExtract(ev)
					if ev.OpType == opTypeUpdate {
						if _, ok2 := updateEvents[worker]; !ok2 {
							updateEvents[worker] = make([]*pb.SyncerBinlogEvent, 0)
						}
						updateEvents[worker] = append(updateEvents[worker], ev)
					} else if ev.OpType == opTypeDDL {
						if _, ok2 := ddlEvents[worker]; !ok2 {
							ddlEvents[worker] = make([]*pb.SyncerBinlogEvent, 0)
						}
						ddlEvents[worker] = append(ddlEvents[worker], ev)
					}
				}
			}
		}

		offset += len(events)
		if len(events) < limit {
			break
		}
	}

	// sort and get last sharding DDL tracing events
	for worker, events := range ddlEvents {
		sort.Sort(BinlogEventSlice(events))
		latestDDL[worker] = make([]*pb.SyncerBinlogEvent, 2)
		// we check the second round of sharding DDL only
		if len(events) < 2 {
			panic(fmt.Sprintf("invalid ddl events count, events: %v", events))
		}
		latestDDL[worker][1] = events[len(events)-1]
		endPos := binlogPosition(latestDDL[worker][1])
		for i := len(events) - 2; i >= 0; i-- {
			pos := binlogPosition(events[i])
			if pos.Compare(endPos) != 0 {
				latestDDL[worker][0] = events[i]
				break
			}
		}
		if latestDDL[worker][0] == nil {
			panic(fmt.Sprintf("first sharding DDL not found, events: %v", events))
		}
	}

	// check safe mode of each `update` binlog event is reasonable
	for worker, evs := range latestDDL {
		if worker != checkInstance {
			continue
		}
		startPos := binlogPosition(evs[0])
		endPos := binlogPosition(evs[1])
		for _, ev := range updateEvents[worker] {
			pos := binlogPosition(ev)
			safeMode := ev.State.SafeMode
			if pos.Compare(endPos) > 0 || (pos.Compare(startPos) >= 0 && ev.Base.Tso > evs[1].Base.Tso) {
				checkSafeMode(safeMode, false, ev, evs[0], evs[1])
			} else if pos.Compare(startPos) >= 0 {
				checkSafeMode(safeMode, true, ev, evs[0], evs[1])
			}
			count += 1
		}
	}

	fmt.Printf("check %d update events passed!\n", count)
}
