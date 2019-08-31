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

	"github.com/syndtr/goleveldb/leveldb"
	"go.uber.org/zap"

	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/helper"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
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
	// The current version of DM-worker.
	currentWorkerVersion = newVersion(currentWorkerInternalNo, utils.ReleaseVersion)
	// The default previous version of DM-worker if no valid version exists in DB before the upgrade.
	defaultPreviousWorkerVersion = newVersion(0, "None")
	// all versions exists in the history.
	workerVersion1 = newVersion(1, "v1.0.0-alpha")
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
		log.L().Error("fail to marshal version to binary",
			zap.Uint64("internal NO", v.InternalNo),
			zap.String("release version", v.ReleaseVersion), log.ShortError(err))
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
func loadVersion(tctx *tcontext.Context, h dbOperator) (ver version, err error) {
	if helper.IsNil(h) {
		return ver, terror.ErrWorkerLogInvalidHandler.Generate()
	}

	data, err := h.Get(dmWorkerVersionKey, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			tctx.L().Warn("no version found in levelDB, use default version", zap.Stringer("default version", defaultPreviousWorkerVersion))
			return defaultPreviousWorkerVersion, nil
		}
		return ver, terror.ErrWorkerGetVersionFromKV.Delegate(err, dmWorkerVersionKey)
	}
	err = ver.UnmarshalBinary(data)
	return ver, terror.ErrWorkerUnmarshalVerBinary.Delegate(err, data)
}

// saveVersion saves the version of DM-worker into the levelDB.
func saveVersion(tctx *tcontext.Context, h dbOperator, ver version) error {
	if helper.IsNil(h) {
		return terror.ErrWorkerLogInvalidHandler.Generate()
	}

	data, err := ver.MarshalBinary()
	if err != nil {
		return terror.ErrWorkerMarshalVerBinary.Delegate(err, ver)
	}

	err = h.Put(dmWorkerVersionKey, data, nil)
	return terror.ErrWorkerSaveVersionToKV.Delegate(err, ver, dmWorkerVersionKey)
}

// tryUpgrade tries to upgrade from an older version.
func tryUpgrade(dbDir string) error {
	tctx := tcontext.Background().WithLogger(log.L().WithFields(zap.String("component", "bootstrap upgrade")))

	// 1. check the DB directory
	notExist := false
	fs, err := os.Stat(dbDir)
	if err != nil {
		if os.IsNotExist(err) {
			notExist = true
		} else {
			return terror.ErrWorkerUpgradeCheckKVDir.AnnotateDelegate(err, "get stat for %s", dbDir)
		}
	} else if !fs.IsDir() { // should be a directory
		return terror.ErrWorkerUpgradeCheckKVDir.Generatef("directory %s for DB not valid", dbDir)
	}

	// 2. open the kv DB
	db, err := openDB(dbDir, defaultKVConfig)
	if err != nil {
		return terror.Annotatef(err, "open DB for %s", dbDir)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			tctx.L().Error("fail to close DB", log.ShortError(err))
		}
	}()

	if notExist {
		tctx.L().Info("no previous operation log exists, no need to upgrade")
		// still need to save the current version version
		currVer := currentWorkerVersion
		err = saveVersion(tctx, db, currVer)
		return terror.Annotatef(err, "save current version %s into DB %s", currVer, dbDir)
	}

	// 3. load previous version
	prevVer, err := loadVersion(tctx, db)
	if err != nil {
		return terror.Annotatef(err, "load previous version from DB %s", dbDir)
	}
	tctx.L().Info("", zap.Stringer("previous version", prevVer))

	// 4. check needing to upgrade
	currVer := currentWorkerVersion
	if prevVer.compare(currVer) == 0 {
		tctx.L().Info("previous and current versions are same, no need to upgrade", zap.Stringer("version", prevVer))
		return nil
	} else if prevVer.compare(currVer) > 0 {
		return terror.ErrWorkerVerAutoDowngrade.Generate(prevVer, currVer)
	}

	// 5. upgrade from previous version to +1, +2, ...
	if prevVer.compare(workerVersion1) < 0 {
		err = upgradeToVer1(tctx, db)
		if err != nil {
			return err
		}
	}

	// 6. save current version after upgrade done
	err = saveVersion(tctx, db, currVer)
	return terror.Annotatef(err, "save current version %s into DB %s", currVer, dbDir)
}

// upgradeToVer1 upgrades from version 0 to version 1.
// before this version, we use `LittleEndian` to encode/decode operation log ID, but it's not correct when scanning operation log by log ID.
// so, if upgrading from previous version to this one, we need to:
//  1. remove all operation log in the levelDB
//  2. reset handled pointer
//  3. remove all task meta in the levelDB
// and let user to restart all necessary tasks.
func upgradeToVer1(tctx *tcontext.Context, db *leveldb.DB) error {
	tctx.L().Info("upgrading to version", zap.Stringer("version", workerVersion1))
	err := ClearOperationLog(tctx, db)
	if err != nil {
		return terror.Annotatef(err, "upgrade to version %s", workerVersion1)
	}
	err = ClearHandledPointer(tctx, db)
	if err != nil {
		return terror.Annotatef(err, "upgrade to version %s", workerVersion1)
	}
	err = ClearTaskMeta(tctx, db)
	if err != nil {
		return terror.Annotatef(err, "upgrade to version %s", workerVersion1)
	}

	tctx.L().Warn("upgraded to version, please restart all necessary tasks manually", zap.Stringer("version", workerVersion1))
	return nil
}
