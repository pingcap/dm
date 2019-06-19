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

package relay

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	gmysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/binlog"
	"github.com/pingcap/dm/pkg/binlog/event"
	"github.com/pingcap/dm/pkg/utils"
	"github.com/pingcap/dm/relay/reader"
	"github.com/pingcap/dm/relay/transformer"
	"github.com/pingcap/dm/relay/writer"
)

var _ = Suite(&testRelaySuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testRelaySuite struct {
}

func getDBConfigForTest() config.DBConfig {
	host := os.Getenv("MYSQL_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port, _ := strconv.Atoi(os.Getenv("MYSQL_PORT"))
	if port == 0 {
		port = 3306
	}
	user := os.Getenv("MYSQL_USER")
	if user == "" {
		user = "root"
	}
	password := os.Getenv("MYSQL_PSWD")
	return config.DBConfig{
		Host:     host,
		Port:     port,
		User:     user,
		Password: password,
	}
}

func openDBForTest() (*sql.DB, error) {
	cfg := getDBConfigForTest()

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4", cfg.User, cfg.Password, cfg.Host, cfg.Port)
	return sql.Open("mysql", dsn)
}

// mockReader is used only for relay testing.
type mockReader struct {
	result reader.Result
	err    error
}

func (r *mockReader) Start() error {
	return nil
}

func (r *mockReader) Close() error {
	return nil
}

func (r *mockReader) GetEvent(ctx context.Context) (reader.Result, error) {
	select {
	case <-ctx.Done():
		return reader.Result{ErrIgnorable: true}, ctx.Err()
	default:
	}
	return r.result, r.err
}

// mockWriter is used only for relay testing.
type mockWriter struct {
	result      writer.Result
	err         error
	latestEvent *replication.BinlogEvent
}

func (w *mockWriter) Start() error {
	return nil
}

func (w *mockWriter) Close() error {
	return nil
}

func (w *mockWriter) Recover() (*writer.RecoverResult, error) {
	return nil, nil
}

func (w *mockWriter) WriteEvent(ev *replication.BinlogEvent) (*writer.Result, error) {
	w.latestEvent = ev // hold it
	return &w.result, w.err
}

func (w *mockWriter) Flush() error {
	return nil
}

func (t *testRelaySuite) TestHandleEvent(c *C) {
	// NOTE: we can test metrics later.
	var (
		reader2      = &mockReader{}
		transformer2 = transformer.NewTransformer(parser.New())
		writer2      = &mockWriter{}
		relayCfg     = &Config{
			RelayDir: c.MkDir(),
			Flavor:   gmysql.MariaDBFlavor,
		}
		r = NewRelay(relayCfg).(*Relay)

		eventHeader = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
		}
		binlogPos   = gmysql.Position{"mysql-bin.666888", 4}
		rotateEv, _ = event.GenRotateEvent(eventHeader, 123, []byte(binlogPos.Name), uint64(binlogPos.Pos))
		queryEv, _  = event.GenQueryEvent(eventHeader, 123, 0, 0, 0, nil, nil, []byte("BEGIN"))
	)
	// NOTE: we can mock meta later.
	c.Assert(r.meta.Load(), IsNil)
	c.Assert(r.meta.AddDir("24ecd093-8cec-11e9-aa0d-0242ac170002", nil, nil), IsNil)

	// attach GTID sets to QueryEv
	queryEv2 := queryEv.Event.(*replication.QueryEvent)
	queryEv2.GSet, _ = gmysql.ParseGTIDSet(relayCfg.Flavor, "1-2-3")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	// reader return with ignorable error
	reader2.result.ErrIgnorable = true
	reader2.err = errors.New("reader error for testing")
	// return with `nil`
	err := r.handleEvents(ctx, reader2, transformer2, writer2)
	c.Assert(err, IsNil)

	// reader return with non-ignorable error
	reader2.result.ErrIgnorable = false
	// return with the annotated reader error
	for _, reader2.err = range []error{
		errors.New("reader error for testing"),
		replication.ErrChecksumMismatch,
		replication.ErrSyncClosed,
		replication.ErrNeedSyncAgain,
	} {
		err = r.handleEvents(ctx, reader2, transformer2, writer2)
		c.Assert(errors.Cause(err), Equals, reader2.err)
	}

	// reader return valid event
	reader2.err = nil
	reader2.result.Event = rotateEv

	// writer return error
	writer2.err = errors.New("writer error for testing")
	// return with the annotated writer error
	err = r.handleEvents(ctx, reader2, transformer2, writer2)
	c.Assert(errors.Cause(err), Equals, writer2.err)

	// writer without error
	writer2.err = nil
	err = r.handleEvents(ctx, reader2, transformer2, writer2) // returned when ctx timeout
	c.Assert(err, IsNil)
	// check written event
	c.Assert(writer2.latestEvent, Equals, reader2.result.Event)
	// check meta
	_, pos := r.meta.Pos()
	_, gs := r.meta.GTID()
	c.Assert(pos, DeepEquals, binlogPos)
	c.Assert(gs.String(), Equals, "") // no GTID sets in event yet

	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel2()

	// write a QueryEvent with GTID sets
	reader2.result.Event = queryEv
	err = r.handleEvents(ctx2, reader2, transformer2, writer2)
	c.Assert(err, IsNil)
	// check written event
	c.Assert(writer2.latestEvent, Equals, reader2.result.Event)
	// check meta
	_, pos = r.meta.Pos()
	_, gs = r.meta.GTID()
	c.Assert(pos.Name, Equals, binlogPos.Name)
	c.Assert(pos.Pos, Equals, queryEv.Header.LogPos)
	c.Assert(gs.Origin(), DeepEquals, queryEv2.GSet) // got GTID sets

	// reader return retryable error
	reader2.result.ErrRetryable = true
	reader2.err = errors.New("reader error for testing")
	ctx3, cancel3 := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel3()
	err = r.handleEvents(ctx3, reader2, transformer2, writer2)
	c.Assert(err, IsNil)
	select {
	case <-ctx3.Done():
	default:
		c.Fatalf("retryable error for reader not retried")
	}

	// transformer return ignorable for the event
	reader2.result.ErrIgnorable = false
	reader2.err = nil
	reader2.result.Event = &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.HEARTBEAT_EVENT},
		Event:  &replication.GenericEvent{}}
	ctx4, cancel4 := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel4()
	err = r.handleEvents(ctx4, reader2, transformer2, writer2)
	c.Assert(err, IsNil)
	select {
	case <-ctx4.Done():
	default:
		c.Fatalf("ignorable event for transformer not ignored")
	}

	// writer return ignorable for the event
	reader2.result.Event = queryEv
	writer2.result.Ignore = true
	ctx5, cancel5 := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel5()
	err = r.handleEvents(ctx5, reader2, transformer2, writer2)
	c.Assert(err, IsNil)
	select {
	case <-ctx5.Done():
	default:
		c.Fatalf("ignorable event for writer not ignored")
	}
}

func (t *testRelaySuite) TestReSetupMeta(c *C) {
	var (
		relayCfg = &Config{
			RelayDir: c.MkDir(),
			Flavor:   gmysql.MySQLFlavor,
		}
		r = NewRelay(relayCfg).(*Relay)
	)
	// empty metadata
	c.Assert(r.meta.Load(), IsNil)
	t.verifyMetadata(c, r, "", minCheckpoint, "", nil)

	// open connected DB and get its UUID
	db, err := openDBForTest()
	c.Assert(err, IsNil)
	r.db = db
	defer func() {
		r.db.Close()
		r.db = nil
	}()
	uuid, err := utils.GetServerUUID(r.db, r.cfg.Flavor)
	c.Assert(err, IsNil)

	// re-setup meta with start pos adjusted
	r.cfg.EnableGTID = true
	r.cfg.BinlogGTID = "24ecd093-8cec-11e9-aa0d-0242ac170002:1-23"
	r.cfg.BinLogName = "mysql-bin.000005"
	c.Assert(r.reSetupMeta(), IsNil)
	uuid001 := fmt.Sprintf("%s.000001", uuid)
	t.verifyMetadata(c, r, uuid001, minCheckpoint, r.cfg.BinlogGTID, []string{uuid001})

	// re-setup meta again, often happen when connecting a server behind a VIP.
	c.Assert(r.reSetupMeta(), IsNil)
	uuid002 := fmt.Sprintf("%s.000002", uuid)
	t.verifyMetadata(c, r, uuid002, minCheckpoint, r.cfg.BinlogGTID, []string{uuid001, uuid002})

}

func (t *testRelaySuite) verifyMetadata(c *C, r *Relay, uuidExpected string,
	posExpected gmysql.Position, gsStrExpected string, UUIDsExpected []string) {
	uuid, pos := r.meta.Pos()
	_, gs := r.meta.GTID()
	c.Assert(uuid, Equals, uuidExpected)
	c.Assert(pos, DeepEquals, posExpected)
	c.Assert(gs.String(), Equals, gsStrExpected)

	indexFile := filepath.Join(r.cfg.RelayDir, utils.UUIDIndexFilename)
	UUIDs, err := utils.ParseUUIDIndex(indexFile)
	c.Assert(err, IsNil)
	c.Assert(UUIDs, DeepEquals, UUIDsExpected)
}

func (t *testRelaySuite) TestProcess(c *C) {
	var (
		dbCfg    = getDBConfigForTest()
		relayCfg = &Config{
			EnableGTID: false, // position mode, so auto-positioning can work
			Flavor:     gmysql.MySQLFlavor,
			RelayDir:   c.MkDir(),
			ServerID:   12321,
			From: DBConfig{
				Host:     dbCfg.Host,
				Port:     dbCfg.Port,
				User:     dbCfg.User,
				Password: dbCfg.Password,
			},
		}
		r = NewRelay(relayCfg).(*Relay)
	)
	db, err := openDBForTest()
	c.Assert(err, IsNil)
	r.db = db
	defer func() {
		r.db.Close()
		r.db = nil
	}()

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = r.process(ctx)
		if !utils.IsErrBinlogPurged(err) {
			// we can tolerate `ERROR 1236` caused by `RESET MASTER` in other test cases.
			c.Assert(err, IsNil)
		}
	}()

	time.Sleep(3 * time.Second) // waiting for get events from upstream
	cancel()                    // stop processing
	wg.Wait()

	// check whether have binlog file in relay directory
	// and check for events already done in `TestHandleEvent`
	uuid, err := utils.GetServerUUID(r.db, r.cfg.Flavor)
	c.Assert(err, IsNil)
	files, err := ioutil.ReadDir(filepath.Join(relayCfg.RelayDir, fmt.Sprintf("%s.000001", uuid)))
	c.Assert(err, IsNil)
	var binlogFileCount int
	for _, f := range files {
		if binlog.VerifyFilename(f.Name()) {
			binlogFileCount++
		}
	}
	c.Assert(binlogFileCount, Greater, 0)
}
