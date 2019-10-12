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
	"sync"

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

// Apply will build BaseDB with DBConfig
func (d *defaultDBProvider) Apply(config config.DBConfig) (*BaseDB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&interpolateParams=true&maxAllowedPacket=%d",
		config.User, config.Password, config.Host, config.Port, *config.MaxAllowedPacket)

	var maxIdleConns int
	rawCfg := config.RawDBCfg
	if rawCfg != nil {
		if rawCfg.ReadTimeout != "" {
			dsn += fmt.Sprintf("&readTimeout=%s", rawCfg.ReadTimeout)
		}
		if rawCfg.WriteTimeout != "" {
			dsn += fmt.Sprintf("&writeTimeout=%s", rawCfg.WriteTimeout)
		}
		maxIdleConns = rawCfg.MaxIdleConns
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	db.SetMaxIdleConns(maxIdleConns)

	return NewBaseDB(db), nil
}

// BaseDB wraps *sql.DB, control the BaseConn
type BaseDB struct {
	DB *sql.DB

	mu sync.Mutex // protects following fields
	// hold all db connections generated from this BaseDB
	conns map[*BaseConn]struct{}

	Retry retry.Strategy
}

// NewBaseDB returns *BaseDB object
func NewBaseDB(db *sql.DB) *BaseDB {
	conns := make(map[*BaseConn]struct{})
	return &BaseDB{DB: db, conns: conns, Retry: &retry.FiniteRetryStrategy{}}
}

// GetBaseConn retrieves *BaseConn which has own retryStrategy
func (d *BaseDB) GetBaseConn(ctx context.Context) (*BaseConn, error) {
	conn, err := d.DB.Conn(ctx)
	if err != nil {
		return nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	err = conn.PingContext(ctx)
	if err != nil {
		return nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	baseConn := NewBaseConn(conn, d.Retry)
	d.mu.Lock()
	defer d.mu.Unlock()
	d.conns[baseConn] = struct{}{}
	return baseConn, nil
}

// CloseBaseConn release BaseConn resource from BaseDB, and close BaseConn
func (d *BaseDB) CloseBaseConn(conn *BaseConn) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.conns, conn)
	return conn.close()
}

// Close release *BaseDB resource
func (d *BaseDB) Close() error {
	if d == nil || d.DB == nil {
		return nil
	}
	var err error
	d.mu.Lock()
	defer d.mu.Unlock()
	for conn := range d.conns {
		terr := conn.close()
		if err == nil {
			err = terr
		}
	}
	terr := d.DB.Close()
	if err == nil {
		return terr
	}
	return err
}
