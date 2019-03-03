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
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/pingcap/dm/dm/pb"
)

type HTTPHandlerTestSuite struct {
	server *Server
	cfg    *Config
	cli    pb.TracerClient
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
}

func (ts *HTTPHandlerTestSuite) prepare(c *C) {
	conn, err := grpc.Dial(ts.cfg.TracerAddr, grpc.WithInsecure(), grpc.WithBackoffMaxDelay(3*time.Second))
	c.Assert(err, IsNil)
	ts.cli = pb.NewTracerClient(conn)
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

func (ts *HTTPHandlerTestSuite) uploadSyncerJobEvent(events []*pb.SyncerJobEvent, c *C) {
	req := &pb.UploadSyncerJobEventRequest{Events: events}
	resp, err := ts.cli.UploadSyncerJobEvent(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.Result, IsTrue)
}

func (ts *HTTPHandlerTestSuite) uploadSyncerBinlogEvent(events []*pb.SyncerBinlogEvent, c *C) {
	req := &pb.UploadSyncerBinlogEventRequest{Events: events}
	resp, err := ts.cli.UploadSyncerBinlogEvent(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.Result, IsTrue)
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
		events    []interface{}
		traceType pb.TraceType
	}{
		{
			traceIDs[0],
			[]interface{}{
				&pb.SyncerJobEvent{
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
				&pb.SyncerJobEvent{
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
			pb.TraceType_JobEvent,
		},
		{
			traceIDs[1],
			[]interface{}{
				&pb.SyncerBinlogEvent{
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

		switch tc.traceType {
		case pb.TraceType_JobEvent:
			evs := make([]*pb.SyncerJobEvent, 0, len(tc.events))
			for _, event := range tc.events {
				e, _ := event.(*pb.SyncerJobEvent)
				evs = append(evs, e)
			}
			ts.uploadSyncerJobEvent(evs, c)

			resp, err = http.Get(queryURL)
			c.Assert(err, IsNil, Commentf("url:%s", queryURL))
			decoder := json.NewDecoder(resp.Body)

			data := make([]SyncerJobEventResp, 0)
			err = decoder.Decode(&data)
			c.Assert(err, IsNil)
			for idx := range data {
				c.Assert(data[idx].Event.String(), Equals, evs[idx].String())
			}

		case pb.TraceType_BinlogEvent:
			evs := make([]*pb.SyncerBinlogEvent, 0, len(tc.events))
			for _, event := range tc.events {
				e, _ := event.(*pb.SyncerBinlogEvent)
				evs = append(evs, e)
			}
			ts.uploadSyncerBinlogEvent(evs, c)

			resp, err = http.Get(queryURL)
			c.Assert(err, IsNil, Commentf("url:%s", queryURL))
			decoder := json.NewDecoder(resp.Body)

			data := make([]SyncerBinlogEventResp, 0)
			err = decoder.Decode(&data)
			c.Assert(err, IsNil)
			for idx := range data {
				c.Assert(data[idx].Event.String(), Equals, evs[idx].String())
			}

		}

		resp.Body.Close()
	}
}
