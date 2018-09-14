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

package utils

import (
	"encoding/binary"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/replication"
)

// GenFakeRotateEvent generates a fake ROTATE_EVENT without checksum
// ref: https://github.com/mysql/mysql-server/blob/4f1d7cf5fcb11a3f84cff27e37100d7295e7d5ca/sql/rpl_binlog_sender.cc#L855
func GenFakeRotateEvent(nextLogName string, logPos uint64, serverID uint32) (*replication.BinlogEvent, error) {
	headerLen := replication.EventHeaderSize
	bodyLen := 8 + len(nextLogName)
	eventSize := headerLen + bodyLen
	rawData := make([]byte, eventSize)
	// header
	binary.LittleEndian.PutUint32(rawData, 0)                         // timestamp
	rawData[4] = byte(replication.ROTATE_EVENT)                       // event type
	binary.LittleEndian.PutUint32(rawData[4+1:], serverID)            // server ID
	binary.LittleEndian.PutUint32(rawData[4+1+4:], uint32(eventSize)) // event size
	binary.LittleEndian.PutUint32(rawData[4+1+4+4:], 0)               // log pos, always 0
	binary.LittleEndian.PutUint16(rawData[4+1+4+4+4:], 0x20)          // flags, LOG_EVENT_ARTIFICIAL_F
	// body
	binary.LittleEndian.PutUint64(rawData[headerLen:], logPos)
	copy(rawData[headerLen+8:], []byte(nextLogName))
	// decode header
	h := &replication.EventHeader{}
	err := h.Decode(rawData)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// decode body
	e := &replication.RotateEvent{}
	err = e.Decode(rawData[headerLen:])
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &replication.BinlogEvent{RawData: rawData, Header: h, Event: e}, nil
}
