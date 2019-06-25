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

package worker

import (
	"encoding/binary"
	"strconv"

	"github.com/pingcap/errors"
)

const (
	// The current internal version of DM-worker used when upgrading from an older version, and it's different from the release version.
	// +1 when an incompatible problem is introduced.
	currentWorkerVersion uint64 = 1
)

var (
	// The key used when saving the internal version of DM-worker
	internalVersionKey = []byte("!DM-worker!internalVersion")
)

// The internal version of DM-worker used when upgrading from an older version.
type internalVersion struct {
	no uint64 // version number
}

// fromUint64 restores the version from an uint64 value.
func (v *internalVersion) fromUint64(verNo uint64) {
	v.no = verNo
}

// toUint64 converts the version to an uint64 value.
func (v *internalVersion) toUint64() uint64 {
	return uint64(v.no)
}

// String implements Stringer.String.
func (v *internalVersion) String() string {
	return strconv.FormatUint(v.no, 10)
}

// MarshalBinary implements encoding.BinaryMarshal, it never returns none-nil error now.
func (v *internalVersion) MarshalBinary() ([]byte, error) {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, v.toUint64())
	return data, nil
}

// UnmarshalBinary implements encoding.BinaryMarshal.
func (v *internalVersion) UnmarshalBinary(data []byte) error {
	if len(data) != 8 {
		return errors.NotValidf("binary data % X", data)
	}
	v.fromUint64(binary.BigEndian.Uint64(data))
	return nil
}

// loadInternalVersion loads the internal version from the levelDB.
func loadInternalVersion(h Getter) (ver internalVersion, err error) {
	if whetherNil(h) {
		return ver, errors.Trace(ErrInValidHandler)
	}

	data, err := h.Get(internalVersionKey, nil)
	if err != nil {
		return ver, errors.Annotatef(err, "load internal version with key %s from levelDB", internalVersionKey)
	}
	err = ver.UnmarshalBinary(data)
	return ver, errors.Annotatef(err, "unmarshal internal version from data % X", data)
}

// saveInternalVersion saves the internal version into the levelDB.
func saveInternalVersion(h Putter, ver internalVersion) error {
	if whetherNil(h) {
		return errors.Trace(ErrInValidHandler)
	}

	data, err := ver.MarshalBinary()
	if err != nil {
		return errors.Annotatef(err, "marshal internal version %v to binary data", ver)
	}

	err = h.Put(internalVersionKey, data, nil)
	return errors.Annotatef(err, "save internal version %v into levelDB with key %s", ver, internalVersionKey)
}
