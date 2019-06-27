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
	"encoding/json"
	"os"

	"github.com/pingcap/errors"
	"github.com/syndtr/goleveldb/leveldb"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/utils"
)

const (
	// The current internal version number of DM-worker used when upgrading from an older version, and it's different from the release version.
	// NOTE: +1 when an incompatible problem is introduced.
	currentWorkerInternalNo uint64 = 1
)

var (
	// The key used when saving the version of DM-worker
	dmWorkerVersionKey = []byte("!DM-worker!version")
	// The default previous version of DM-worker if no valid version exists in DB before the upgrade.
	defaultPreviousWorkerVersion = version{InternalNo: 0, ReleaseVersion: "None"}
	// all versions exists in the history.
	workerVersion1 = version{InternalNo: 1, ReleaseVersion: "v1.0.0-alpha"}
)

// The version of DM-worker used when upgrading from an older version.
type version struct {
	InternalNo     uint64 `json:"internal-no"`     // internal version number
	ReleaseVersion string `json:"release-version"` // release version, like `v1.0.0`
}

// newVersion creates a new instance of version.
func newVersion(internalNo uint64, releaseVersion string) version {
	return version{
		InternalNo:     internalNo,
		ReleaseVersion: releaseVersion,
	}
}

// newCurrentVersion creates a new instance of version match the current DM-worker.
func newCurrentVersion() version {
	return newVersion(currentWorkerInternalNo, utils.ReleaseVersion)
}

// compare compares the version with another version.
// NOTE: also compare `ReleaseVersion` when needed.
func (v *version) compare(other version) int {
	if v.InternalNo < other.InternalNo {
		return -1
	} else if v.InternalNo == other.InternalNo {
		return 0
	}
	return 1
}

// String implements Stringer.String.
func (v version) String() string {
	data, err := v.MarshalBinary()
	if err != nil {
		log.Errorf("[worker upgrade] marshal version (internal-no: %d, release-version: %s) to binary error %v",
			v.InternalNo, v.ReleaseVersion, err)
		return ""
	}
	return string(data)
}

// MarshalBinary implements encoding.BinaryMarshal.
func (v *version) MarshalBinary() ([]byte, error) {
	return json.Marshal(v)
}

// UnmarshalBinary implements encoding.BinaryMarshal.
func (v *version) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, v)
}

// loadVersion loads the version of DM-worker from the levelDB.
func loadVersion(h Getter) (ver version, err error) {
	if whetherNil(h) {
		return ver, errors.Trace(ErrInValidHandler)
	}

	data, err := h.Get(dmWorkerVersionKey, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return defaultPreviousWorkerVersion, nil
		}
		return ver, errors.Annotatef(err, "load version with key %s from levelDB", dmWorkerVersionKey)
	}
	err = ver.UnmarshalBinary(data)
	return ver, errors.Annotatef(err, "unmarshal version from data % X", data)
}

// saveVersion saves the version of DM-worker into the levelDB.
func saveVersion(h Putter, ver version) error {
	if whetherNil(h) {
		return errors.Trace(ErrInValidHandler)
	}

	data, err := ver.MarshalBinary()
	if err != nil {
		return errors.Annotatef(err, "marshal version %v to binary data", ver)
	}

	err = h.Put(dmWorkerVersionKey, data, nil)
	return errors.Annotatef(err, "save version %v into levelDB with key %s", ver, dmWorkerVersionKey)
}

// tryUpgrade tries to upgrade from an older version.
func tryUpgrade(dbDir string) error {
	// 1. check the DB directory
	notExist := false
	fs, err := os.Stat(dbDir)
	if err != nil {
		if os.IsNotExist(err) {
			notExist = true
		} else {
			return errors.Annotatef(err, "get stat for %s", dbDir)
		}
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

	if notExist {
		log.Infof("[worker upgrade] no previous operation log exists, no need to upgrade")
		// still need to save the current version version
		currVer := newCurrentVersion()
		err = saveVersion(db, currVer)
		return errors.Annotatef(err, "save current version %v into DB %s", currVer, dbDir)
	}

	// 3. load previous version
	prevVer, err := loadVersion(db)
	if err != nil {
		return errors.Annotatef(err, "load previous version from DB %s", dbDir)
	}
	log.Infof("[worker upgrade] the previous version is %v", prevVer)

	// 4. check needing to upgrade
	currVer := newCurrentVersion()
	if prevVer.compare(currVer) == 0 {
		log.Infof("[worker upgrade] the previous and current versions both are %v, no need to upgrade", prevVer)
		return nil
	} else if prevVer.compare(currVer) > 0 {
		log.Warnf("[worker upgrade] the previous version %v is newer than current %v, automatic downgrade is not supported now", prevVer, currVer)
		return nil
	}

	// 5. upgrade from previous version to +1, +2, ...
	if prevVer.compare(workerVersion1) < 0 {
		err = upgradeToVer1(db)
		if err != nil {
			return errors.Annotatef(err, "upgrade to version %v", workerVersion1)
		}
	}

	// 6. save current version after upgrade done
	err = saveVersion(db, currVer)
	return errors.Annotatef(err, "save current version %v into DB %s", currVer, dbDir)
}

// upgradeToVer1 upgrades from version 0 to version 1.
// before this version, we use `LittleEndian` to encode/decode operation log ID, but it's not correct when scanning operation log by log ID.
// so, if upgrading from previous version to this one, we need to:
//  1. remove all operation log in the levelDB
//  2. reset handled pointer
//  3. remove all task meta in the levelDB
// and let user to restart all necessary tasks.
func upgradeToVer1(db *leveldb.DB) error {
	log.Infof("[worker upgrade] upgrading to version %v", workerVersion1)
	txn, err := db.OpenTransaction()
	if err != nil {
		return errors.Annotatef(err, "open transaction for upgrading to version %v", workerVersion1)
	}

	defer func() {
		if err != nil {
			txn.Discard()
		}
	}()
	err = ClearOperationLog(txn)
	if err != nil {
		return errors.Annotatef(err, "upgrade to version %v", workerVersion1)
	}
	err = ClearHandledPointer(txn)
	if err != nil {
		return errors.Annotatef(err, "upgrade to version %v", workerVersion1)
	}
	err = ClearTaskMeta(txn)
	if err != nil {
		return errors.Annotatef(err, "upgrade to version %v", workerVersion1)
	}
	err2 := txn.Commit()
	if err2 != nil {
		return errors.Annotatef(err, "upgrade to version %v", workerVersion1)
	}

	log.Warnf("[worker upgrade] upgraded to version %v, please restart all necessary tasks manually", workerVersion1)
	return nil
}
