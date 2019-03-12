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

package syncer

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/siddontang/go/sync2"

	"github.com/pingcap/dm/dm/pb"
)

var (
	ddlExecIdle    = "idle"
	ddlExecSending = "sending"
	ddlExecClosed  = "closed"
)

// DDLExecItem wraps request and response for a sharding DDL execution
type DDLExecItem struct {
	req  *pb.ExecDDLRequest
	resp chan error
}

// newDDLExecItem creates a new DDLExecItem
func newDDLExecItem(req *pb.ExecDDLRequest) *DDLExecItem {
	item := &DDLExecItem{
		req:  req,
		resp: make(chan error, 1), // one elem buffered
	}
	return item
}

// DDLExecInfo used by syncer to execute or ignore sharding DDL
// it's specific to syncer, and can not be used by other process unit
type DDLExecInfo struct {
	sync.RWMutex
	status sync2.AtomicString
	ch     chan *DDLExecItem // item.req.Exec: true for exec, false for ignore
	cancel chan struct{}     // chan used to cancel sending
	ddls   []string          // DDL which is blocking
}

// NewDDLExecInfo creates a new DDLExecInfo
func NewDDLExecInfo() *DDLExecInfo {
	i := &DDLExecInfo{
		ch:     make(chan *DDLExecItem), // un-buffered
		cancel: make(chan struct{}),
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

	i.ch = make(chan *DDLExecItem)
	i.cancel = make(chan struct{})
	i.ddls = nil
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
	i.ddls = nil
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

// Send sends an item (with request) to the chan
func (i *DDLExecInfo) Send(ctx context.Context, item *DDLExecItem) error {
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

// Chan returns a receive only DDLExecItem chan
func (i *DDLExecInfo) Chan(ddls []string) <-chan *DDLExecItem {
	i.Lock()
	i.ddls = ddls
	i.Unlock()
	return i.ch
}

// BlockingDDLs returns current blocking DDL
func (i *DDLExecInfo) BlockingDDLs() []string {
	i.RLock()
	defer i.RUnlock()
	return i.ddls
}

// ClearBlockingDDL clears current blocking DDL
func (i *DDLExecInfo) ClearBlockingDDL() {
	i.Lock()
	defer i.Unlock()
	i.ddls = nil
}
