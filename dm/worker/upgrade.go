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
	"os"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/syndtr/goleveldb/leveldb"

	"github.com/pingcap/dm/pkg/log"
)

const (
	// The current internal version of DM-worker used when upgrading from an older version, and it's different from the release version.
	// +1 when an incompatible problem is introduced.
	currentWorkerVersion uint64 = 1
	// The default previous internal version of DM-worker if no valid internal version exists in DB before the upgrade.
	defaultPreviousWorkerVersion uint64 = 0
	// all internal versions exists in the history.
	workerVersion1 uint64 = 1
)

var (
	// The key used when saving the internal version of DM-worker
	internalVersionKey = []byte("!DM-worker!internalVersion")
)

// The internal version of DM-worker used when upgrading from an older version.
type internalVersion struct {
	no uint64 // version number
}

// newInternalVersion creates a new instance of internalVersion.
func newInternalVersion(verNo uint64) internalVersion {
	return internalVersion{
		no: verNo,
	}
}

// fromUint64 restores the version from an uint64 value.
func (v *internalVersion) fromUint64(verNo uint64) {
	v.no = verNo
}

// toUint64 converts the version to an uint64 value.
func (v *internalVersion) toUint64() uint64 {
	return uint64(v.no)
}

// compare compares the version with another version.
func (v *internalVersion) compare(other internalVersion) int {
	if v.no < other.no {
		return -1
	} else if v.no == other.no {
		return 0
	}
	return 1
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
		if err == leveldb.ErrNotFound {
			ver.fromUint64(defaultPreviousWorkerVersion)
			return ver, nil
		}
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

// tryUpgrade tries to upgrade from an older version.
func tryUpgrade(dbDir string) error {
	// 1. check the DB directory
	fs, err := os.Stat(dbDir)
	if err != nil {
		if os.IsNotExist(err) {
			log.Infof("[worker upgrade] no previous operation log exists, no need to upgrade")
			return nil // DB does not exist, no need to upgrade
		}
		return errors.Annotatef(err, "get stat for %s", dbDir)
	} else if !fs.IsDir() { // should be a directory
		return errors.NotValidf("directory %s for DB", dbDir)
	}

	// 2. open the kv DB
	db, err := openDB(dbDir, defaultKVConfig)
	if err != nil {
		return errors.Annotatef(err, "open DB for %s", dbDir)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			log.Errorf("[worker upgrade] close DB fail %v", err)
		}
	}()

	// 3. load previous internal version
	prevVer, err := loadInternalVersion(db)
	if err != nil {
		return errors.Annotatef(err, "load previous internal version from DB %s", dbDir)
	}

	// 4. check needing to upgrade
	currVer := newInternalVersion(currentWorkerVersion)
	if prevVer.compare(currVer) == 0 {
		log.Infof("[worker upgrade] the previous and current internal versions both are %v, no need to upgrade", prevVer)
		return nil
	} else if prevVer.compare(currVer) > 0 {
		log.Warnf("[worker upgrade] the previous internal version %v is newer than current %v, automatic downgrade is not supported now", prevVer, currVer)
		return nil
	}

	// 5. upgrade from previous version to +1, +2, ...
	if prevVer.compare(newInternalVersion(workerVersion1)) < 0 {
		err = upgradeToVer1()
		if err != nil {
			return errors.Annotate(err, "upgrade to internal version 1")
		}
	}

	// 6. save current internal version after upgrade done
	err = saveInternalVersion(db, currVer)
	return errors.Annotatef(err, "save current internal version %v into DB %s", currVer, dbDir)
}

// upgradeToVer1 upgrades from version 0 to version 1.
func upgradeToVer1() error {
	log.Info("[worker upgrade] upgrading to internal version 1")
	log.Info("[worker upgrade] upgraded to internal version 1")
	return nil
}
