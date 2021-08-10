// Copyright 2021 PingCAP, Inc.
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

package dbconn

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
	. "github.com/pingcap/check"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/utils"
)

var _ = Suite(&testDBSuite{})

type testDBSuite struct {
	db       *sql.DB
	syncer   *replication.BinlogSyncer
	streamer *replication.BinlogStreamer
	cfg      *config.SubTaskConfig
}

func (s *testDBSuite) SetUpSuite(c *C) {
	s.cfg = &config.SubTaskConfig{
		From:       config.GetDBConfigFromEnv(),
		To:         config.GetDBConfigFromEnv(),
		ServerID:   102,
		MetaSchema: "db_test",
		Name:       "db_ut",
		Mode:       config.ModeIncrement,
		Flavor:     "mysql",
	}
	s.cfg.From.Adjust()
	s.cfg.To.Adjust()

	dir := c.MkDir()
	s.cfg.RelayDir = dir

	var err error
	dbAddr := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8", s.cfg.From.User, s.cfg.From.Password, s.cfg.From.Host, s.cfg.From.Port)
	s.db, err = sql.Open("mysql", dbAddr)
	c.Assert(err, IsNil)

	s.resetBinlogSyncer(c)
	_, err = s.db.Exec("SET GLOBAL binlog_format = 'ROW';")
	c.Assert(err, IsNil)
}

func (s *testDBSuite) resetBinlogSyncer(c *C) {
	cfg := replication.BinlogSyncerConfig{
		ServerID:       s.cfg.ServerID,
		Flavor:         "mysql",
		Host:           s.cfg.From.Host,
		Port:           uint16(s.cfg.From.Port),
		User:           s.cfg.From.User,
		Password:       s.cfg.From.Password,
		UseDecimal:     false,
		VerifyChecksum: true,
	}
	cfg.TimestampStringLocation = time.UTC

	if s.syncer != nil {
		s.syncer.Close()
	}

	pos, _, err := utils.GetMasterStatus(context.Background(), s.db, "mysql")
	c.Assert(err, IsNil)

	s.syncer = replication.NewBinlogSyncer(cfg)
	s.streamer, err = s.syncer.StartSync(pos)
	c.Assert(err, IsNil)
}

func (s *testDBSuite) TestGetServerUUID(c *C) {
	u, err := utils.GetServerUUID(context.Background(), s.db, "mysql")
	c.Assert(err, IsNil)
	_, err = uuid.Parse(u)
	c.Assert(err, IsNil)
}

func (s *testDBSuite) TestGetServerID(c *C) {
	id, err := utils.GetServerID(context.Background(), s.db)
	c.Assert(err, IsNil)
	c.Assert(id, Greater, uint32(0))
}

func (s *testDBSuite) TestGetServerUnixTS(c *C) {
	id, err := utils.GetServerUnixTS(context.Background(), s.db)
	c.Assert(err, IsNil)
	c.Assert(id, Greater, int64(0))
}

func (s *testDBSuite) TestBinaryLogs(c *C) {
	ctx := context.Background()
	files, err := getBinaryLogs(ctx, s.db)
	c.Assert(err, IsNil)
	c.Assert(files, Not(HasLen), 0)

	fileNum := len(files)
	pos := mysql.Position{
		Name: files[fileNum-1].name,
		Pos:  0,
	}

	remainingSize, err := countBinaryLogsSize(ctx, pos, s.db)
	c.Assert(err, IsNil)
	c.Assert(remainingSize, Equals, files[fileNum-1].size)

	_, err = s.db.Exec("FLUSH BINARY LOGS")
	c.Assert(err, IsNil)
	files, err = getBinaryLogs(ctx, s.db)
	c.Assert(err, IsNil)
	c.Assert(files, HasLen, fileNum+1)

	pos = mysql.Position{
		Name: files[fileNum].name,
		Pos:  0,
	}

	remainingSize, err = countBinaryLogsSize(ctx, pos, s.db)
	c.Assert(err, IsNil)
	c.Assert(remainingSize, Equals, files[fileNum].size)
}
