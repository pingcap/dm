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

package tracer

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"

	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/tracing"
)

type HTTPHandlerTestSuite struct {
	server *Server
	cfg    *Config
	tracer *tracing.Tracer
}

type SyncerJobEventResp struct {
	Type  pb.TraceType      `json:"type"`
	Event pb.SyncerJobEvent `json:"event"`
}

type SyncerBinlogEventResp struct {
	Type  pb.TraceType         `json:"type"`
	Event pb.SyncerBinlogEvent `json:"event"`
}

var _ = Suite(new(HTTPHandlerTestSuite))

func TestSuite(t *testing.T) {
	TestingT(t)
}

func (ts *HTTPHandlerTestSuite) startServer(c *C) {
	ts.cfg = NewConfig()
	ts.server = NewServer(ts.cfg)
	go ts.server.Start()
	err := waitUntilServerOnline(ts.cfg.TracerAddr)
	c.Assert(err, IsNil)
}

func (ts *HTTPHandlerTestSuite) stopServer(c *C) {
	if ts.server != nil {
		ts.server.Close()
	}
	ts.tracer.Stop()
}

func (ts *HTTPHandlerTestSuite) prepare(c *C) {
	cfg := tracing.Config{
		Enable:     true,
		Source:     "mysql-replica-01",
		TracerAddr: fmt.Sprintf("127.0.0.1%s", ts.cfg.TracerAddr),
	}
	ts.tracer = tracing.InitTracerHub(cfg)
	ts.tracer.Start()
}

const retryTime = 100

func waitUntilServerOnline(bindAddr string) error {
	statusURL := fmt.Sprintf("http://127.0.0.1%s/status", bindAddr)
	for i := 0; i < retryTime; i++ {
		resp, err := http.Get(statusURL)
		if err == nil {
			ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			return nil
		}
		time.Sleep(time.Millisecond * 10)
	}
	return errors.Errorf("failed to connect http status for %d retries in every 10ms", retryTime)
}

func (ts *HTTPHandlerTestSuite) TestTraceEventQuery(c *C) {
	ts.startServer(c)
	defer ts.stopServer(c)
	ts.prepare(c)

	traceIDs := []string{
		"mysql-replica-01.syncer.test.1",
		"mysql-replica-01.syncer.test.2",
	}

	testCases := []struct {
		traceID   string
		jobs      []*tracing.Job
		traceType pb.TraceType
	}{
		{
			traceIDs[0],
			[]*tracing.Job{
				&tracing.Job{
					Tp: tracing.EventSyncerJob,
					Event: &pb.SyncerJobEvent{
						Base: &pb.BaseEvent{
							Filename: "/path/to/test.go",
							Line:     100,
							Tso:      time.Now().UnixNano(),
							TraceID:  traceIDs[0],
							Type:     pb.TraceType_JobEvent,
						},
						OpType: 1,
						Pos: &pb.MySQLPosition{
							Name: "bin|000001.000004",
							Pos:  1626,
						},
						CurrentPos: &pb.MySQLPosition{
							Name: "bin|000001.000004",
							Pos:  1873,
						},
						Sql:         "REPLACE INTO `test`.`t_target` (`id`,`ct`,`name`) VALUES (?,?,?);",
						QueueBucket: "q_1",
						State:       pb.SyncerJobState_queued,
					},
				},
				&tracing.Job{
					Tp: tracing.EventSyncerJob,
					Event: &pb.SyncerJobEvent{
						Base: &pb.BaseEvent{
							Filename: "/path/to/test.go",
							Line:     100,
							Tso:      time.Now().UnixNano(),
							TraceID:  traceIDs[0],
							Type:     pb.TraceType_JobEvent,
						},
						OpType: 1,
						Pos: &pb.MySQLPosition{
							Name: "bin|000001.000004",
							Pos:  1626,
						},
						CurrentPos: &pb.MySQLPosition{
							Name: "bin|000001.000004",
							Pos:  1873,
						},
						Sql:         "REPLACE INTO `test`.`t_target` (`id`,`ct`,`name`) VALUES (?,?,?);",
						QueueBucket: "q_1",
						State:       pb.SyncerJobState_success,
					},
				},
			},
			pb.TraceType_JobEvent,
		},
		{
			traceIDs[1],
			[]*tracing.Job{
				&tracing.Job{
					Tp: tracing.EventSyncerBinlog,
					Event: &pb.SyncerBinlogEvent{
						Base: &pb.BaseEvent{
							Filename: "/path/to/test.go",
							Line:     100,
							Tso:      time.Now().UnixNano(),
							TraceID:  traceIDs[1],
							Type:     pb.TraceType_BinlogEvent,
						},
						State: &pb.SyncerState{
							SafeMode:  true,
							TryReSync: true,
							LastPos: &pb.MySQLPosition{
								Name: "bin|000001.000004",
								Pos:  1626,
							},
						},
						OpType:    1,
						EventType: 2,
						CurrentPos: &pb.MySQLPosition{
							Name: "bin|000001.000004",
							Pos:  1873,
						},
					},
				},
			},
			pb.TraceType_BinlogEvent,
		},
	}

	for _, tc := range testCases {
		var (
			queryURL = fmt.Sprintf("http://127.0.0.1%s/events/query?trace_id=%s", ts.cfg.TracerAddr, tc.traceID)
			resp     *http.Response
			err      error
			raw      []byte
		)
		resp, err = http.Get(queryURL)
		c.Assert(err, IsNil, Commentf("url:%s", queryURL))
		c.Assert(resp.StatusCode, Equals, http.StatusNotFound)
		raw, err = ioutil.ReadAll(resp.Body)
		c.Assert(err, IsNil)
		c.Assert(string(raw), Equals, fmt.Sprintf("trace event %s not found", tc.traceID))
		resp.Body.Close()

		err = ts.tracer.ProcessTraceEvents(tc.jobs)
		c.Assert(err, IsNil)

		resp, err = http.Get(queryURL)
		c.Assert(err, IsNil, Commentf("url:%s", queryURL))
		decoder := json.NewDecoder(resp.Body)

		switch tc.traceType {
		case pb.TraceType_JobEvent:
			data := make([]SyncerJobEventResp, 0)
			err = decoder.Decode(&data)
			c.Assert(err, IsNil)
			c.Assert(len(data), Equals, len(tc.jobs))
			for idx := range data {
				ev, _ := tc.jobs[idx].Event.(*pb.SyncerJobEvent)
				c.Assert(data[idx].Event.String(), Equals, ev.String())
			}
		case pb.TraceType_BinlogEvent:
			data := make([]SyncerBinlogEventResp, 0)
			err = decoder.Decode(&data)
			c.Assert(err, IsNil)
			c.Assert(len(data), Equals, len(tc.jobs))
			for idx := range data {
				ev, _ := tc.jobs[idx].Event.(*pb.SyncerBinlogEvent)
				c.Assert(data[idx].Event.String(), Equals, ev.String())
			}
		}

		resp.Body.Close()
	}
}
