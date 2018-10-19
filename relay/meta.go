package relay

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"sync"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go/ioutil2"

	"github.com/pingcap/tidb-enterprise-tools/pkg/gtid"
	"github.com/pingcap/tidb-enterprise-tools/pkg/utils"
)

var (
	minCheckpoint = mysql.Position{Pos: 4}
)

// Meta represents binlog meta information for sync source
// when re-syncing, we should reload meta info to guarantee continuous transmission
// in order to support master-slave switching, Meta should support switching binlog meta info to newer master
// should support the case, where switching from A to B, then switching from B back to A
type Meta interface {
	// Load loads meta information for the recently active server
	Load() error

	// Save saves meta information
	Save(pos mysql.Position, gset gtid.Set) error

	// Flush flushes meta information
	Flush() error

	// Dirty checks whether meta in memory is dirty (need to Flush)
	Dirty() bool

	// AddDir adds sub relay directory for server UUID (without suffix)
	// the added sub relay directory's suffix is incremented
	// after sub relay directory added, the internal binlog pos should be reset
	// and binlog pos will be set again when new binlog events received
	// @serverUUID should be a server_uuid for MySQL or MariaDB
	// if set @newPos / @newGTID, old value will be replaced
	AddDir(serverUUID string, newPos *mysql.Position, newGTID gtid.Set) error

	// Pos returns current (UUID with suffix, Position) pair
	Pos() (string, mysql.Position)

	// GTID returns current (UUID with suffix, GTID) pair
	GTID() (string, gtid.Set)

	// UUID returns current UUID (with suffix)
	UUID() string

	// Dir returns current relay log (sub) directory
	Dir() string

	// String returns string representation of current meta info
	String() string
}

// LocalMeta implements Meta by save info in local
type LocalMeta struct {
	sync.RWMutex
	flavor        string
	baseDir       string
	uuidIndexPath string
	currentUUID   string // current UUID with suffix
	gset          gtid.Set
	emptyGSet     gtid.Set
	dirty         bool

	BinLogName string `toml:"binlog-name" json:"binlog-name"`
	BinLogPos  uint32 `toml:"binlog-pos" json:"binlog-pos"`
	BinlogGTID string `toml:"binlog-gtid" json:"binlog-gtid"`
}

// NewLocalMeta creates a new LocalMeta
func NewLocalMeta(flavor, baseDir string) Meta {
	lm := &LocalMeta{
		flavor:        flavor,
		baseDir:       baseDir,
		uuidIndexPath: path.Join(baseDir, utils.UUIDIndexFilename),
		currentUUID:   "",
		dirty:         false,
		BinLogName:    minCheckpoint.Name,
		BinLogPos:     minCheckpoint.Pos,
		BinlogGTID:    "",
	}
	lm.emptyGSet, _ = gtid.ParserGTID(flavor, "")
	return lm
}

// Load implements Meta.Load
func (lm *LocalMeta) Load() error {
	lm.Lock()
	defer lm.Unlock()

	uuids, err := utils.ParseUUIDIndex(lm.uuidIndexPath)
	if err != nil {
		return errors.Trace(err)
	}

	err = lm.verifyUUIDs(uuids)
	if err != nil {
		return errors.Trace(err)
	}

	if len(uuids) > 0 {
		// update to the latest
		err = lm.updateCurrentUUID(uuids[len(uuids)-1])
		if err != nil {
			return errors.Trace(err)
		}
	}

	err = lm.loadMetaData()
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// Save implements Meta.Save
func (lm *LocalMeta) Save(pos mysql.Position, gset gtid.Set) error {
	lm.Lock()
	defer lm.Unlock()

	if len(lm.currentUUID) == 0 {
		return errors.NotValidf("no current UUID set")
	}

	lm.BinLogName = pos.Name
	lm.BinLogPos = pos.Pos
	if gset == nil {
		lm.BinlogGTID = ""
	} else {
		lm.BinlogGTID = gset.String()
		lm.gset = gset
	}

	lm.dirty = true

	return nil
}

// Flush implements Meta.Flush
func (lm *LocalMeta) Flush() error {
	lm.RLock()
	defer lm.RUnlock()

	return lm.doFlush()
}

// doFlush does the real flushing
func (lm *LocalMeta) doFlush() error {
	if len(lm.currentUUID) == 0 {
		return errors.NotValidf("no current UUID set")
	}

	var buf bytes.Buffer
	enc := toml.NewEncoder(&buf)
	err := enc.Encode(lm)
	if err != nil {
		return errors.Trace(err)
	}

	filename := path.Join(lm.baseDir, lm.currentUUID, utils.MetaFilename)
	err = ioutil2.WriteFileAtomic(filename, buf.Bytes(), 0644)
	if err != nil {
		return errors.Trace(err)
	}

	lm.dirty = false

	return nil
}

// Dirty implements Meta.Dirty
func (lm *LocalMeta) Dirty() bool {
	lm.RLock()
	defer lm.RUnlock()

	return lm.dirty
}

// Dir implements Meta.Dir
func (lm *LocalMeta) Dir() string {
	lm.RLock()
	defer lm.RUnlock()

	return path.Join(lm.baseDir, lm.currentUUID)
}

// AddDir implements Meta.AddDir
func (lm *LocalMeta) AddDir(serverUUID string, newPos *mysql.Position, newGTID gtid.Set) error {
	lm.Lock()
	defer lm.Unlock()

	var newUUID string

	if len(lm.currentUUID) == 0 {
		// no UUID exists yet, simply add it
		newUUID = utils.AddSuffixForUUID(serverUUID, 1)
	} else {
		_, suffix, err := utils.ParseSuffixForUUID(lm.currentUUID)
		if err != nil {
			return errors.Trace(err)
		}
		// even newUUID == currentUUID, we still append it (for some cases, like `RESET MASTER`)
		newUUID = utils.AddSuffixForUUID(serverUUID, suffix+1)
	}

	// flush previous meta
	if lm.dirty {
		err := lm.doFlush()
		if err != nil {
			return errors.Trace(err)
		}
	}

	// make sub dir for UUID
	os.Mkdir(path.Join(lm.baseDir, newUUID), 0744)

	// append UUID to index file
	err := lm.appendIndexFile(newUUID)
	if err != nil {
		return errors.Trace(err)
	}

	// update current UUID
	lm.currentUUID = newUUID

	if newPos != nil {
		lm.BinLogName = newPos.Name
		lm.BinLogPos = newPos.Pos
	} else {
		// reset binlog pos, will be set again when new binlog events received from master
		// not reset GTID, it will be used to continue the syncing
		lm.BinLogName = minCheckpoint.Name
		lm.BinLogPos = minCheckpoint.Pos
	}

	if newGTID != nil {
		lm.gset = newGTID
		lm.BinlogGTID = newGTID.String()
	} // if newGTID == nil, keep GTID not changed

	// flush new meta to file
	lm.doFlush()

	return nil
}

// Pos implements Meta.Pos
func (lm *LocalMeta) Pos() (string, mysql.Position) {
	lm.RLock()
	defer lm.RUnlock()

	return lm.currentUUID, mysql.Position{Name: lm.BinLogName, Pos: lm.BinLogPos}
}

// GTID implements Meta.GTID
func (lm *LocalMeta) GTID() (string, gtid.Set) {
	lm.RLock()
	defer lm.RUnlock()

	return lm.currentUUID, lm.gset.Clone()
}

// UUID implements Meta.UUID
func (lm *LocalMeta) UUID() string {
	lm.RLock()
	defer lm.RUnlock()
	return lm.currentUUID
}

// String implements Meta.String
func (lm *LocalMeta) String() string {
	uuid, pos := lm.Pos()
	_, gs := lm.GTID()
	return fmt.Sprintf("master-uuid = %s, relay-binlog = %v, relay-binlog-gtid = %v", uuid, pos, gs)
}

// appendIndexFile appends uuid to index file
func (lm *LocalMeta) appendIndexFile(uuid string) error {
	fd, err := os.OpenFile(lm.uuidIndexPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return errors.Trace(err)
	}
	defer fd.Close()

	_, err = fd.WriteString(fmt.Sprintf("%s\n", uuid))
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (lm *LocalMeta) verifyUUIDs(uuids []string) error {
	previousSuffix := 0
	for _, uuid := range uuids {
		_, suffix, err := utils.ParseSuffixForUUID(uuid)
		if err != nil {
			return errors.Annotatef(err, "UUID %s", uuid)
		}
		if previousSuffix > 0 {
			if previousSuffix+1 != suffix {
				return errors.Errorf("UUID %s suffix %d should be 1 larger than previous suffix %d", uuid, suffix, previousSuffix)
			}
		}
		previousSuffix = suffix
	}

	return nil
}

// updateCurrentUUID updates current UUID
func (lm *LocalMeta) updateCurrentUUID(uuid string) error {
	_, suffix, err := utils.ParseSuffixForUUID(uuid)
	if err != nil {
		return errors.Trace(err)
	}

	if len(lm.currentUUID) > 0 {
		_, previousSuffix, err := utils.ParseSuffixForUUID(lm.currentUUID)
		if err != nil {
			return errors.Trace(err) // should not happen
		}
		if previousSuffix > suffix {
			return errors.Errorf("previous UUID %s has suffix larger than %s", lm.currentUUID, uuid)
		}
	}

	lm.currentUUID = uuid
	return nil
}

// loadMetaData loads meta information from meta data file
func (lm *LocalMeta) loadMetaData() error {
	lm.gset = lm.emptyGSet.Clone()

	if len(lm.currentUUID) == 0 {
		return nil
	}

	filename := path.Join(lm.baseDir, lm.currentUUID, utils.MetaFilename)

	fd, err := os.Open(filename)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return errors.Trace(err)
	}
	defer fd.Close()

	_, err = toml.DecodeReader(fd, lm)
	if err != nil {
		return errors.Trace(err)
	}

	gset, err := gtid.ParserGTID(lm.flavor, lm.BinlogGTID)
	if err != nil {
		return errors.Trace(err)
	}
	lm.gset = gset

	return nil
}
