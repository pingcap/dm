// Copyright 2018 PingCAP, Inc.
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

package syncer

import (
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-enterprise-tools/dm/pb"
	"github.com/siddontang/go/sync2"
	"golang.org/x/net/context"
)

var (
	ddlExecIdle    = "idle"
	ddlExecSending = "sending"
	ddlExecClosed  = "closed"
)

// DDLExecInfo used by syncer to execute or ignore sharding DDL
// it's specific to syncer, and can not be used by other process unit
type DDLExecInfo struct {
	sync.RWMutex
	status sync2.AtomicString
	ch     chan *pb.ExecDDLRequest // item.Exec: true for exec, false for ignore
	cancel chan struct{}           // chan used to cancel sending
	ddl    string                  // DDL which is blocking
}

// NewDDLExecInfo creates a new DDLExecInfo
func NewDDLExecInfo() *DDLExecInfo {
	i := &DDLExecInfo{
		ch:     make(chan *pb.ExecDDLRequest), // un-buffered
		cancel: make(chan struct{}),
		ddl:    "",
	}
	i.status.Set(ddlExecIdle)
	return i
}

// Renew renews the chan
func (i *DDLExecInfo) Renew() {
	i.Lock()
	defer i.Unlock()

	i.cancelAndWaitSending()

	if i.status.Get() != ddlExecClosed {
		close(i.ch)
	}

	i.ch = make(chan *pb.ExecDDLRequest)
	i.cancel = make(chan struct{})
	i.ddl = ""
	i.status.Set(ddlExecIdle)
}

// Close closes the chan
func (i *DDLExecInfo) Close() {
	i.Lock()
	defer i.Unlock()
	if i.status.CompareAndSwap(ddlExecClosed, ddlExecClosed) {
		return
	}

	i.cancelAndWaitSending()

	close(i.ch)
	i.ddl = ""
	i.status.Set(ddlExecClosed)
}

// NOTE: in caller, should do lock
func (i *DDLExecInfo) cancelAndWaitSending() {
	// close the un-closed cancel chan
	select {
	case <-i.cancel:
	default:
		close(i.cancel)
	}

	// wait Send to return
	timer := time.NewTicker(1 * time.Millisecond)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			if !i.status.CompareAndSwap(ddlExecSending, ddlExecSending) {
				return // return from select and for
			}
		}
	}
}

// Send sends an item to the chan
func (i *DDLExecInfo) Send(ctx context.Context, item *pb.ExecDDLRequest) error {
	i.RLock()
	if !i.status.CompareAndSwap(ddlExecIdle, ddlExecSending) {
		i.RUnlock()
		return errors.New("the chan has closed or already in sending")
	}
	i.RUnlock()
	defer i.status.Set(ddlExecIdle)

	select {
	case <-ctx.Done():
		return errors.New("canceled from external")
	case i.ch <- item:
		return nil
	case <-i.cancel:
		return errors.New("canceled by Close or Renew")
	}
}

// Chan returns a receive only chan
func (i *DDLExecInfo) Chan(ddl string) <-chan *pb.ExecDDLRequest {
	i.Lock()
	i.ddl = ddl
	i.Unlock()
	return i.ch
}

// BlockingDDL returns current blocking DDL
func (i *DDLExecInfo) BlockingDDL() string {
	i.RLock()
	defer i.RUnlock()
	return i.ddl
}

// ClearBlockingDDL clears current blocking DDL
func (i *DDLExecInfo) ClearBlockingDDL() {
	i.Lock()
	defer i.Unlock()
	i.ddl = ""
}
