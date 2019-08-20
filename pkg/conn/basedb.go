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

package conn

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/retry"
	"github.com/pingcap/dm/pkg/terror"
)

// DBProvider providers BaseDB instance
type DBProvider interface {
	Apply(config config.DBConfig) (*BaseDB, error)
}

type defaultDBProvider struct {
}

// DefaultDBProvider is global instance of DBProvider
var DefaultDBProvider DBProvider

func init() {
	DefaultDBProvider = &defaultDBProvider{}
}

func (d *defaultDBProvider) Apply(config config.DBConfig) (*BaseDB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&interpolateParams=true&maxAllowedPacket=%d",
		config.User, config.Password, config.Host, config.Port, *config.MaxAllowedPacket)

	maxIdleConn := 0
	rawCfg := config.RawDBCfg
	if rawCfg != nil {
		if rawCfg.ReadTimeout != "" {
			dsn += fmt.Sprintf("&readTimeout=%s", rawCfg.ReadTimeout)
		}
		if rawCfg.WriteTimeout != "" {
			dsn += fmt.Sprintf("&writeTimeout=%s", rawCfg.WriteTimeout)
		}
		maxIdleConn = rawCfg.MaxIdleConns
	}
	db, err := sql.Open("mysql", dsn)

	if maxIdleConn > 0 {
		db.SetMaxIdleConns(maxIdleConn)
	}

	if err != nil {
		return nil, terror.ErrDBDriverError.Delegate(err)
	}
	return &BaseDB{db, &retry.FiniteRetryStrategy{}}, nil
}

// BaseDB wraps *sql.DB
type BaseDB struct {
	DB *sql.DB

	Retry retry.Strategy
}

// GetBaseConn retrieves *BaseConn which has own retryStrategy
func (d *BaseDB) GetBaseConn(ctx context.Context) (*BaseConn, error) {
	conn, err := d.DB.Conn(ctx)
	if err != nil {
		return nil, terror.ErrDBDriverError.Delegate(err)
	}
	return newBaseConn(conn, d.Retry)
}

// Close release db resource
func (d *BaseDB) Close() error {
	if d == nil || d.DB == nil {
		return nil
	}
	return d.DB.Close()
}
