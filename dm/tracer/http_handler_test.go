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
	"net/url"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"

	"github.com/pingcap/dm/dm/pb"
)

type HTTPHandlerTestSuite struct {
	server *Server
	cfg    *Config
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

func (ts *HTTPHandlerTestSuite) startServer(c *C, offset int) {
	ts.cfg = NewConfig()
	ts.cfg.TracerAddr = fmt.Sprintf(":%d", 8263+offset)

	ts.server = NewServer(ts.cfg)
	go ts.server.Start()

	err := ts.waitUntilServerOnline()
	c.Assert(err, IsNil)
}

func (ts *HTTPHandlerTestSuite) stopServer(c *C) {
	if ts.server != nil {
		ts.server.Close()
	}
}

const retryTime = 100

func (ts *HTTPHandlerTestSuite) waitUntilServerOnline() error {
	statusURL := fmt.Sprintf("http://127.0.0.1%s/status", ts.cfg.TracerAddr)
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
	ts.startServer(c, 1)
	defer ts.stopServer(c)

	traceIDs := []string{
		"mysql-replica-01.syncer.test.1",
		"mysql-replica-01.syncer.test.2",
	}

	testCases := []struct {
		traceID   string
		events    []*TraceEvent
		traceType pb.TraceType
	}{
		{
			traceIDs[0],
			[]*TraceEvent{
				{
					Type: pb.TraceType_JobEvent,
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
				{
					Type: pb.TraceType_JobEvent,
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
			[]*TraceEvent{
				{
					Type: pb.TraceType_BinlogEvent,
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
							CurrentPos: &pb.MySQLPosition{
								Name: "bin|000001.000004",
								Pos:  1873,
							},
						},
						OpType:    1,
						EventType: 2,
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
		resp.Body.Close()
		c.Assert(err, IsNil)
		c.Assert(string(raw), Matches, fmt.Sprintf(".*trace event %s not found", tc.traceID))

		for _, ev := range tc.events {
			err = ts.server.eventStore.addNewEvent(ev)
			c.Assert(err, IsNil)
		}

		resp, err = http.Get(queryURL)
		c.Assert(err, IsNil, Commentf("url:%s", queryURL))
		decoder := json.NewDecoder(resp.Body)

		switch tc.traceType {
		case pb.TraceType_JobEvent:
			data := make([]SyncerJobEventResp, 0)
			err = decoder.Decode(&data)
			c.Assert(err, IsNil)
			c.Assert(len(data), Equals, len(tc.events))
			for idx := range data {
				ev, _ := tc.events[idx].Event.(*pb.SyncerJobEvent)
				c.Assert(data[idx].Event.String(), Equals, ev.String())
			}
		case pb.TraceType_BinlogEvent:
			data := make([]SyncerBinlogEventResp, 0)
			err = decoder.Decode(&data)
			c.Assert(err, IsNil)
			c.Assert(len(data), Equals, len(tc.events))
			for idx := range data {
				ev, _ := tc.events[idx].Event.(*pb.SyncerBinlogEvent)
				c.Assert(data[idx].Event.String(), Equals, ev.String())
			}
		}
		resp.Body.Close()
	}

	// test bad request error
	var (
		queryURL = fmt.Sprintf("http://127.0.0.1%s/events/query", ts.cfg.TracerAddr)
		resp     *http.Response
		err      error
	)
	resp, err = http.Get(queryURL)
	c.Assert(err, IsNil, Commentf("url:%s", queryURL))
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)
	resp.Body.Close()
}

func (ts *HTTPHandlerTestSuite) TestTraceEventScan(c *C) {
	ts.startServer(c, 2)
	defer ts.stopServer(c)

	var (
		traceIDFmt = "test.trace_id.%d"
		err        error
		count      = 15
	)
	for i := 0; i < count; i++ {
		err = ts.server.eventStore.addNewEvent(&TraceEvent{
			Type: pb.TraceType_BinlogEvent,
			Event: &pb.SyncerBinlogEvent{
				Base: &pb.BaseEvent{
					Filename: "/path/to/test.go",
					Line:     100,
					Tso:      time.Now().UnixNano(),
					TraceID:  fmt.Sprintf(traceIDFmt, i),
					Type:     pb.TraceType_BinlogEvent,
				},
			},
		})
		c.Assert(err, IsNil)
	}

	testCases := []struct {
		offset   int64
		limit    int64
		expected int
	}{
		{0, 0, 0},
		{3, 5, 5},
		{3, 20, 12},
		{10, 10, 5},
		{15, 10, 0},
	}

	for _, t := range testCases {
		var (
			scanURL = fmt.Sprintf("http://127.0.0.1%s/events/scan?offset=%d&limit=%d", ts.cfg.TracerAddr, t.offset, t.limit)
			resp    *http.Response
			err     error
			data    [][]SyncerBinlogEventResp
		)

		resp, err = http.Get(scanURL)
		c.Assert(err, IsNil, Commentf("url:%s", scanURL))
		decoder := json.NewDecoder(resp.Body)

		err = decoder.Decode(&data)
		c.Assert(err, IsNil)
		c.Assert(len(data), Equals, t.expected)
		for idx, events := range data {
			for _, event := range events {
				c.Assert(event.Event.Base.TraceID, Equals, fmt.Sprintf(traceIDFmt, int(t.offset)+idx))
			}
		}
		resp.Body.Close()
	}
}

func (ts *HTTPHandlerTestSuite) TestTraceEventDelete(c *C) {
	ts.startServer(c, 3)
	defer ts.stopServer(c)

	var (
		traceID   = "test.delete.trace_id"
		err       error
		queryURL  = fmt.Sprintf("http://127.0.0.1%s/events/query?trace_id=%s", ts.cfg.TracerAddr, traceID)
		deleteURL = fmt.Sprintf("http://127.0.0.1%s/events/delete", ts.cfg.TracerAddr)
		resp      *http.Response
		data      []SyncerBinlogEventResp
		raw       []byte
	)

	err = ts.server.eventStore.addNewEvent(&TraceEvent{
		Type: pb.TraceType_BinlogEvent,
		Event: &pb.SyncerBinlogEvent{
			Base: &pb.BaseEvent{
				Filename: "/path/to/test.go",
				Line:     100,
				Tso:      time.Now().UnixNano(),
				TraceID:  traceID,
				Type:     pb.TraceType_BinlogEvent,
			},
		},
	})
	c.Assert(err, IsNil)

	resp, err = http.Get(queryURL)
	c.Assert(err, IsNil, Commentf("url:%s", queryURL))
	decoder := json.NewDecoder(resp.Body)

	err = decoder.Decode(&data)
	resp.Body.Close()
	c.Assert(err, IsNil)
	for _, event := range data {
		c.Assert(event.Event.Base.TraceID, Equals, traceID)
	}

	form := make(url.Values)
	form.Set("trace_id", traceID)
	resp, err = http.PostForm(deleteURL, form)
	resp.Body.Close()
	c.Assert(err, IsNil, Commentf("url:%s", deleteURL))

	resp, err = http.Get(queryURL)
	c.Assert(err, IsNil, Commentf("url:%s", queryURL))
	c.Assert(resp.StatusCode, Equals, http.StatusNotFound)
	raw, err = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	c.Assert(err, IsNil)
	c.Assert(string(raw), Matches, fmt.Sprintf(".*trace event %s not found", traceID))
}

func (ts *HTTPHandlerTestSuite) TestTraceEventTruncate(c *C) {
	ts.startServer(c, 4)
	defer ts.stopServer(c)

	var (
		traceID     = "test.delete.trace_id"
		err         error
		queryURL    = fmt.Sprintf("http://127.0.0.1%s/events/query?trace_id=%s", ts.cfg.TracerAddr, traceID)
		truncateURL = fmt.Sprintf("http://127.0.0.1%s/events/truncate", ts.cfg.TracerAddr)
		resp        *http.Response
		data        []SyncerBinlogEventResp
		raw         []byte
	)

	err = ts.server.eventStore.addNewEvent(&TraceEvent{
		Type: pb.TraceType_BinlogEvent,
		Event: &pb.SyncerBinlogEvent{
			Base: &pb.BaseEvent{
				Filename: "/path/to/test.go",
				Line:     100,
				Tso:      time.Now().UnixNano(),
				TraceID:  traceID,
				Type:     pb.TraceType_BinlogEvent,
			},
		},
	})
	c.Assert(err, IsNil)

	resp, err = http.Get(queryURL)
	c.Assert(err, IsNil, Commentf("url:%s", queryURL))
	decoder := json.NewDecoder(resp.Body)

	err = decoder.Decode(&data)
	resp.Body.Close()
	c.Assert(err, IsNil)
	for _, event := range data {
		c.Assert(event.Event.Base.TraceID, Equals, traceID)
	}

	form := make(url.Values)
	resp, err = http.PostForm(truncateURL, form)
	resp.Body.Close()
	c.Assert(err, IsNil, Commentf("url:%s", truncateURL))

	resp, err = http.Get(queryURL)
	c.Assert(err, IsNil, Commentf("url:%s", queryURL))
	c.Assert(resp.StatusCode, Equals, http.StatusNotFound)
	raw, err = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	c.Assert(err, IsNil)
	c.Assert(string(raw), Matches, fmt.Sprintf(".*trace event %s not found", traceID))
}
