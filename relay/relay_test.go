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
	"bytes"
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
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/streamer"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
	"github.com/pingcap/dm/relay/reader"
	"github.com/pingcap/dm/relay/retry"
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
		return reader.Result{}, ctx.Err()
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

func (w *mockWriter) Recover() (writer.RecoverResult, error) {
	return writer.RecoverResult{}, nil
}

func (w *mockWriter) WriteEvent(ev *replication.BinlogEvent) (writer.Result, error) {
	w.latestEvent = ev // hold it
	return w.result, w.err
}

func (w *mockWriter) Flush() error {
	return nil
}

func (t *testRelaySuite) TestTryRecoverLatestFile(c *C) {
	var (
		uuid               = "24ecd093-8cec-11e9-aa0d-0242ac170002"
		uuidWithSuffix     = fmt.Sprintf("%s.000001", uuid)
		previousGTIDSetStr = "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14,53bfca22-690d-11e7-8a62-18ded7a37b78:1-495,406a3f61-690d-11e7-87c5-6c92bf46f384:123-456"
		latestGTIDStr1     = "3ccc475b-2343-11e7-be21-6c0b84d59f30:14"
		latestGTIDStr2     = "53bfca22-690d-11e7-8a62-18ded7a37b78:495"
		genGTIDSetStr      = "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-17,53bfca22-690d-11e7-8a62-18ded7a37b78:1-505,406a3f61-690d-11e7-87c5-6c92bf46f384:123-456"
		greaterGITDSetStr  = "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-20,53bfca22-690d-11e7-8a62-18ded7a37b78:1-510,406a3f61-690d-11e7-87c5-6c92bf46f384:123-456"
		filename           = "mysql-bin.000001"
		startPos           = gmysql.Position{Name: filename, Pos: 123}

		parser2  = parser.New()
		relayCfg = &Config{
			RelayDir: c.MkDir(),
			Flavor:   gmysql.MySQLFlavor,
		}
		r = NewRelay(relayCfg).(*Relay)
	)
	// purge old relay dir
	f, err := os.Create(filepath.Join(r.cfg.RelayDir, "old_relay_log"))
	c.Assert(err, IsNil)
	f.Close()
	c.Assert(r.purgeRelayDir(), IsNil)
	files, err := ioutil.ReadDir(r.cfg.RelayDir)
	c.Assert(err, IsNil)
	c.Assert(files, HasLen, 0)

	c.Assert(r.meta.Load(), IsNil)

	// no file specified, no need to recover
	c.Assert(r.tryRecoverLatestFile(parser2), IsNil)

	// save position into meta
	c.Assert(r.meta.AddDir(uuid, &startPos, nil), IsNil)

	// relay log file does not exists, no need to recover
	c.Assert(r.tryRecoverLatestFile(parser2), IsNil)

	// use a generator to generate some binlog events
	previousGTIDSet, err := gtid.ParserGTID(relayCfg.Flavor, previousGTIDSetStr)
	c.Assert(err, IsNil)
	latestGTID1, err := gtid.ParserGTID(relayCfg.Flavor, latestGTIDStr1)
	c.Assert(err, IsNil)
	latestGTID2, err := gtid.ParserGTID(relayCfg.Flavor, latestGTIDStr2)
	c.Assert(err, IsNil)
	g, _, data := genBinlogEventsWithGTIDs(c, relayCfg.Flavor, previousGTIDSet, latestGTID1, latestGTID2)

	// write events into relay log file
	err = ioutil.WriteFile(filepath.Join(r.meta.Dir(), filename), data, 0600)
	c.Assert(err, IsNil)

	// all events/transactions are complete, no need to recover
	c.Assert(r.tryRecoverLatestFile(parser2), IsNil)
	// now, we do not update position/GTID set in meta if not recovered
	t.verifyMetadata(c, r, uuidWithSuffix, startPos, "", []string{uuidWithSuffix})

	// write some invalid data into the relay log file
	f, err = os.OpenFile(filepath.Join(r.meta.Dir(), filename), os.O_WRONLY|os.O_APPEND, 0600)
	c.Assert(err, IsNil)
	_, err = f.Write([]byte("invalid event data"))
	c.Assert(err, IsNil)
	f.Close()

	// GTID sets in meta data does not contain the GTID sets in relay log, invalid
	c.Assert(terror.ErrGTIDTruncateInvalid.Equal(r.tryRecoverLatestFile(parser2)), IsTrue)

	// write some invalid data into the relay log file again
	f, err = os.OpenFile(filepath.Join(r.meta.Dir(), filename), os.O_WRONLY|os.O_APPEND, 0600)
	c.Assert(err, IsNil)
	_, err = f.Write([]byte("invalid event data"))
	c.Assert(err, IsNil)
	f.Close()

	// write a greater GTID sets in meta
	greaterGITDSet, err := gtid.ParserGTID(relayCfg.Flavor, greaterGITDSetStr)
	c.Assert(err, IsNil)
	c.Assert(r.meta.Save(startPos, greaterGITDSet), IsNil)

	// invalid data truncated, meta updated
	c.Assert(r.tryRecoverLatestFile(parser2), IsNil)
	_, latestPos := r.meta.Pos()
	c.Assert(latestPos, DeepEquals, gmysql.Position{Name: filename, Pos: g.LatestPos})
	_, latestGTIDs := r.meta.GTID()
	genGTIDSet, err := gtid.ParserGTID(relayCfg.Flavor, genGTIDSetStr)
	c.Assert(err, IsNil)
	c.Assert(latestGTIDs.Equal(genGTIDSet), IsTrue) // verifyMetadata is not enough

	// no relay log file need to recover
	c.Assert(r.meta.Save(minCheckpoint, latestGTIDs), IsNil)
	c.Assert(r.tryRecoverLatestFile(parser2), IsNil)
	_, latestPos = r.meta.Pos()
	c.Assert(latestPos, DeepEquals, minCheckpoint)
	_, latestGTIDs = r.meta.GTID()
	c.Assert(latestGTIDs.Contain(g.LatestGTID), IsTrue)
}

// genBinlogEventsWithGTIDs generates some binlog events used by testFileUtilSuite and testFileWriterSuite.
// now, its generated events including 3 DDL and 10 DML.
func genBinlogEventsWithGTIDs(c *C, flavor string, previousGTIDSet, latestGTID1, latestGTID2 gtid.Set) (*event.Generator, []*replication.BinlogEvent, []byte) {
	var (
		serverID  uint32 = 11
		latestPos uint32
		latestXID uint64 = 10

		allEvents = make([]*replication.BinlogEvent, 0, 50)
		allData   bytes.Buffer
	)

	// use a binlog event generator to generate some binlog events.
	g, err := event.NewGenerator(flavor, serverID, latestPos, latestGTID1, previousGTIDSet, latestXID)
	c.Assert(err, IsNil)

	// file header with FormatDescriptionEvent and PreviousGTIDsEvent
	events, data, err := g.GenFileHeader()
	c.Assert(err, IsNil)
	allEvents = append(allEvents, events...)
	allData.Write(data)

	// CREATE DATABASE/TABLE, 3 DDL
	queries := []string{
		"CREATE DATABASE `db`",
		"CREATE TABLE `db`.`tbl1` (c1 INT)",
		"CREATE TABLE `db`.`tbl2` (c1 INT)",
	}
	for _, query := range queries {
		events, data, err = g.GenDDLEvents("db", query)
		c.Assert(err, IsNil)
		allEvents = append(allEvents, events...)
		allData.Write(data)
	}

	// DMLs, 10 DML
	g.LatestGTID = latestGTID2 // use another latest GTID with different SID/DomainID
	var (
		tableID    uint64 = 8
		columnType        = []byte{gmysql.MYSQL_TYPE_LONG}
		eventType         = replication.WRITE_ROWS_EVENTv2
		schema            = "db"
		table             = "tbl1"
	)
	for i := 0; i < 10; i++ {
		insertRows := make([][]interface{}, 0, 1)
		insertRows = append(insertRows, []interface{}{int32(i)})
		dmlData := []*event.DMLData{
			{
				TableID:    tableID,
				Schema:     schema,
				Table:      table,
				ColumnType: columnType,
				Rows:       insertRows,
			},
		}
		events, data, err = g.GenDMLEvents(eventType, dmlData)
		c.Assert(err, IsNil)
		allEvents = append(allEvents, events...)
		allData.Write(data)
	}

	return g, allEvents, allData.Bytes()
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
		queryEv, _  = event.GenQueryEvent(eventHeader, 123, 0, 0, 0, nil, nil, []byte("CREATE DATABASE db_relay_test"))
	)
	// NOTE: we can mock meta later.
	c.Assert(r.meta.Load(), IsNil)
	c.Assert(r.meta.AddDir("24ecd093-8cec-11e9-aa0d-0242ac170002", nil, nil), IsNil)

	// attach GTID sets to QueryEv
	queryEv2 := queryEv.Event.(*replication.QueryEvent)
	queryEv2.GSet, _ = gmysql.ParseGTIDSet(relayCfg.Flavor, "1-2-3")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	// reader return with an error
	for _, reader2.err = range []error{
		errors.New("reader error for testing"),
		replication.ErrChecksumMismatch,
		replication.ErrSyncClosed,
		replication.ErrNeedSyncAgain,
	} {
		err := r.handleEvents(ctx, reader2, transformer2, writer2)
		c.Assert(errors.Cause(err), Equals, reader2.err)
	}

	// reader return valid event
	reader2.err = nil
	reader2.result.Event = rotateEv

	// writer return error
	writer2.err = errors.New("writer error for testing")
	// return with the annotated writer error
	err := r.handleEvents(ctx, reader2, transformer2, writer2)
	c.Assert(errors.Cause(err), Equals, writer2.err)

	// writer without error
	writer2.err = nil
	err = r.handleEvents(ctx, reader2, transformer2, writer2) // returned when ctx timeout
	c.Assert(errors.Cause(err), Equals, ctx.Err())
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
	c.Assert(errors.Cause(err), Equals, ctx.Err())
	// check written event
	c.Assert(writer2.latestEvent, Equals, reader2.result.Event)
	// check meta
	_, pos = r.meta.Pos()
	_, gs = r.meta.GTID()
	c.Assert(pos.Name, Equals, binlogPos.Name)
	c.Assert(pos.Pos, Equals, queryEv.Header.LogPos)
	c.Assert(gs.Origin(), DeepEquals, queryEv2.GSet) // got GTID sets

	// transformer return ignorable for the event
	reader2.err = nil
	reader2.result.Event = &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.HEARTBEAT_EVENT},
		Event:  &replication.GenericEvent{}}
	ctx4, cancel4 := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel4()
	err = r.handleEvents(ctx4, reader2, transformer2, writer2)
	c.Assert(errors.Cause(err), Equals, ctx.Err())
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
	c.Assert(errors.Cause(err), Equals, ctx.Err())
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
	t.verifyMetadata(c, r, uuid001, gmysql.Position{Name: r.cfg.BinLogName, Pos: 4}, r.cfg.BinlogGTID, []string{uuid001})

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
			ReaderRetry: retry.ReaderRetryConfig{
				BackoffRollback: 200 * time.Millisecond,
				BackoffMax:      1 * time.Second,
				BackoffMin:      1 * time.Millisecond,
				BackoffJitter:   true,
				BackoffFactor:   2,
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
		err2 := r.process(ctx)
		if !utils.IsErrBinlogPurged(err2) {
			// we can tolerate `ERROR 1236` caused by `RESET MASTER` in other test cases.
			c.Assert(err2, IsNil)
		}
	}()

	time.Sleep(1 * time.Second) // waiting for get events from upstream

	// kill the binlog dump connection
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	connID, err := getBinlogDumpConnID(ctx2, r.db)
	c.Assert(err, IsNil)
	_, err = r.db.ExecContext(ctx2, fmt.Sprintf(`KILL %d`, connID))
	c.Assert(err, IsNil)

	// execute a DDL again
	lastDDL := "CREATE DATABASE `test_relay_retry_db`"
	_, err = r.db.ExecContext(ctx2, lastDDL)
	c.Assert(err, IsNil)

	defer func() {
		query := "DROP DATABASE IF EXISTS `test_relay_retry_db`"
		_, err = r.db.ExecContext(ctx2, query)
		c.Assert(err, IsNil)
	}()

	time.Sleep(2 * time.Second) // waiting for events
	cancel()                    // stop processing
	wg.Wait()

	// should got the last DDL
	gotLastDDL := false
	onEventFunc := func(e *replication.BinlogEvent) error {
		switch ev := e.Event.(type) {
		case *replication.QueryEvent:
			if bytes.Contains(ev.Query, []byte(lastDDL)) {
				gotLastDDL = true
			}
		}
		return nil
	}
	parser2 := replication.NewBinlogParser()
	parser2.SetVerifyChecksum(true)

	// check whether have binlog file in relay directory
	// and check for events already done in `TestHandleEvent`
	uuid, err := utils.GetServerUUID(r.db, r.cfg.Flavor)
	c.Assert(err, IsNil)
	files, err := streamer.CollectAllBinlogFiles(filepath.Join(relayCfg.RelayDir, fmt.Sprintf("%s.000001", uuid)))
	c.Assert(err, IsNil)
	var binlogFileCount int
	for _, f := range files {
		if binlog.VerifyFilename(f) {
			binlogFileCount++

			if !gotLastDDL {
				err = parser2.ParseFile(filepath.Join(relayCfg.RelayDir, fmt.Sprintf("%s.000001", uuid), f), 0, onEventFunc)
				c.Assert(err, IsNil)
			}
		}
	}
	c.Assert(binlogFileCount, Greater, 0)
	c.Assert(gotLastDDL, IsTrue)
}

// getBinlogDumpConnID gets the `Binlog Dump` connection ID.
// now only return the first one.
func getBinlogDumpConnID(ctx context.Context, db *sql.DB) (uint32, error) {
	query := `SHOW PROCESSLIST`
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var (
		id      sql.NullInt64
		user    sql.NullString
		host    sql.NullString
		db2     sql.NullString
		command sql.NullString
		time2   sql.NullInt64
		state   sql.NullString
		info    sql.NullString
	)
	for rows.Next() {
		err = rows.Scan(&id, &user, &host, &db2, &command, &time2, &state, &info)
		if err != nil {
			return 0, err
		}
		if id.Valid && command.Valid && command.String == "Binlog Dump" {
			return uint32(id.Int64), rows.Err()
		}
	}
	return 0, errors.NotFoundf("Binlog Dump")
}
