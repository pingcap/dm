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

package streamer

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"

	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

var _ = Suite(&testFileSuite{})

type testFileSuite struct {
}

func (t *testFileSuite) TestCollectBinlogFiles(c *C) {
	var (
		valid = []string{
			"mysql-bin.000001",
			"mysql-bin.000002",
			"mysql-bin.000003",
			"mysql-bin.000004",
		}
		invalid = []string{
			"mysql-bin.invalid01",
			"mysql-bin.invalid02",
		}
		meta = []string{
			utils.MetaFilename,
			utils.MetaFilename + ".tmp",
		}
	)

	files, err := CollectAllBinlogFiles("")
	c.Assert(err, NotNil)
	c.Assert(files, IsNil)

	dir := c.MkDir()

	// create all valid binlog files
	for _, fn := range valid {
		err = ioutil.WriteFile(filepath.Join(dir, fn), nil, 0600)
		c.Assert(err, IsNil)
	}
	files, err = CollectAllBinlogFiles(dir)
	c.Assert(err, IsNil)
	c.Assert(files, DeepEquals, valid)

	// create some invalid binlog files
	for _, fn := range invalid {
		err = ioutil.WriteFile(filepath.Join(dir, fn), nil, 0600)
		c.Assert(err, IsNil)
	}
	files, err = CollectAllBinlogFiles(dir)
	c.Assert(err, IsNil)
	c.Assert(files, DeepEquals, valid)

	// create some invalid meta files
	for _, fn := range meta {
		err = ioutil.WriteFile(filepath.Join(dir, fn), nil, 0600)
		c.Assert(err, IsNil)
	}
	files, err = CollectAllBinlogFiles(dir)
	c.Assert(err, IsNil)
	c.Assert(files, DeepEquals, valid)

	// collect newer files, none
	files, err = CollectBinlogFilesCmp(dir, valid[len(valid)-1], FileCmpBigger)
	c.Assert(err, IsNil)
	c.Assert(files, DeepEquals, []string{})

	// collect newer files, some
	files, err = CollectBinlogFilesCmp(dir, valid[0], FileCmpBigger)
	c.Assert(err, IsNil)
	c.Assert(files, DeepEquals, valid[1:])

	// collect newer or equal files, all
	files, err = CollectBinlogFilesCmp(dir, valid[0], FileCmpBiggerEqual)
	c.Assert(err, IsNil)
	c.Assert(files, DeepEquals, valid)

	// collect newer or equal files, some
	files, err = CollectBinlogFilesCmp(dir, valid[1], FileCmpBiggerEqual)
	c.Assert(err, IsNil)
	c.Assert(files, DeepEquals, valid[1:])

	// collect older files, none
	files, err = CollectBinlogFilesCmp(dir, valid[0], FileCmpLess)
	c.Assert(err, IsNil)
	c.Assert(files, DeepEquals, []string{})

	// collect older files, some
	files, err = CollectBinlogFilesCmp(dir, valid[len(valid)-1], FileCmpLess)
	c.Assert(err, IsNil)
	c.Assert(files, DeepEquals, valid[:len(valid)-1])
}

func (t *testFileSuite) TestCollectBinlogFilesCmp(c *C) {
	var (
		dir         string
		baseFile    string
		cmp         = FileCmpEqual
		binlogFiles = []string{
			"mysql-bin.000001",
			"mysql-bin.000002",
			"mysql-bin.000003",
			"mysql-bin.000004",
		}
	)

	// empty dir
	files, err := CollectBinlogFilesCmp(dir, baseFile, cmp)
	c.Assert(terror.ErrEmptyRelayDir.Equal(err), IsTrue)
	c.Assert(files, IsNil)

	// empty base filename, not found
	dir = c.MkDir()
	files, err = CollectBinlogFilesCmp(dir, baseFile, cmp)
	c.Assert(errors.IsNotFound(err), IsTrue)
	c.Assert(files, IsNil)

	// base file not found
	baseFile = utils.MetaFilename
	files, err = CollectBinlogFilesCmp(dir, baseFile, cmp)
	c.Assert(errors.IsNotFound(err), IsTrue)
	c.Assert(files, IsNil)

	// create a meta file
	filename := filepath.Join(dir, utils.MetaFilename)
	err = ioutil.WriteFile(filename, nil, 0600)
	c.Assert(err, IsNil)

	// invalid base filename, is a meta filename
	files, err = CollectBinlogFilesCmp(dir, baseFile, cmp)
	c.Assert(err, ErrorMatches, ".*invalid binlog filename.*")
	c.Assert(files, IsNil)

	// create some binlog files
	for _, f := range binlogFiles {
		filename = filepath.Join(dir, f)
		err = ioutil.WriteFile(filename, nil, 0600)
		c.Assert(err, IsNil)
	}

	// > base file
	cmp = FileCmpBigger
	var i int
	for i, baseFile = range binlogFiles {
		files, err = CollectBinlogFilesCmp(dir, baseFile, cmp)
		c.Assert(err, IsNil)
		c.Assert(files, DeepEquals, binlogFiles[i+1:])
	}

	// >= base file
	cmp = FileCmpBiggerEqual
	for i, baseFile = range binlogFiles {
		files, err = CollectBinlogFilesCmp(dir, baseFile, cmp)
		c.Assert(err, IsNil)
		c.Assert(files, DeepEquals, binlogFiles[i:])
	}

	// < base file
	cmp = FileCmpLess
	for i, baseFile = range binlogFiles {
		files, err = CollectBinlogFilesCmp(dir, baseFile, cmp)
		c.Assert(err, IsNil)
		c.Assert(files, DeepEquals, binlogFiles[:i])
	}

	// add a basename mismatch binlog file
	filename = filepath.Join(dir, "bin-mysql.100000")
	err = ioutil.WriteFile(filename, nil, 0600)
	c.Assert(err, IsNil)

	// test again, should ignore it
	for i, baseFile = range binlogFiles {
		files, err = CollectBinlogFilesCmp(dir, baseFile, cmp)
		c.Assert(err, IsNil)
		c.Assert(files, DeepEquals, binlogFiles[:i])
	}

	// other cmp not supported yet
	cmps := []FileCmp{FileCmpLessEqual, FileCmpEqual}
	for _, cmp = range cmps {
		files, err = CollectBinlogFilesCmp(dir, baseFile, cmp)
		c.Assert(err, ErrorMatches, ".*not supported.*")
		c.Assert(files, IsNil)
	}
}

func (t *testFileSuite) TestGetFirstBinlogName(c *C) {
	var (
		baseDir = c.MkDir()
		uuid    = "b60868af-5a6f-11e9-9ea3-0242ac160006.000001"
		subDir  = filepath.Join(baseDir, uuid)
	)

	// sub directory not exist
	name, err := getFirstBinlogName(baseDir, uuid)
	c.Assert(err, ErrorMatches, ".*(no such file or directory|The system cannot find the file specified).*")
	c.Assert(name, Equals, "")

	// empty directory
	err = os.MkdirAll(subDir, 0700)
	c.Assert(err, IsNil)
	name, err = getFirstBinlogName(baseDir, uuid)
	c.Assert(err, ErrorMatches, ".*not found.*")
	c.Assert(name, Equals, "")

	// has file, but not a valid binlog file
	filename := "invalid.bin"
	err = ioutil.WriteFile(filepath.Join(subDir, filename), nil, 0600)
	c.Assert(err, IsNil)
	_, err = getFirstBinlogName(baseDir, uuid)
	c.Assert(err, ErrorMatches, ".*not valid.*")
	err = os.Remove(filepath.Join(subDir, filename))
	c.Assert(err, IsNil)

	// has a valid binlog file
	filename = "z-mysql-bin.000002" // z prefix, make it become not the _first_ if possible.
	err = ioutil.WriteFile(filepath.Join(subDir, filename), nil, 0600)
	c.Assert(err, IsNil)
	name, err = getFirstBinlogName(baseDir, uuid)
	c.Assert(err, IsNil)
	c.Assert(name, Equals, filename)

	// has one more earlier binlog file
	filename = "z-mysql-bin.000001"
	err = ioutil.WriteFile(filepath.Join(subDir, filename), nil, 0600)
	c.Assert(err, IsNil)
	name, err = getFirstBinlogName(baseDir, uuid)
	c.Assert(err, IsNil)
	c.Assert(name, Equals, filename)

	// has a meta file
	err = ioutil.WriteFile(filepath.Join(subDir, utils.MetaFilename), nil, 0600)
	c.Assert(err, IsNil)
	name, err = getFirstBinlogName(baseDir, uuid)
	c.Assert(err, IsNil)
	c.Assert(name, Equals, filename)
}

func (t *testFileSuite) TestFileSizeUpdated(c *C) {
	var (
		filename   = "mysql-bin.000001"
		filePath   = filepath.Join(c.MkDir(), filename)
		data       = []byte("meaningless file content")
		latestSize = int64(len(data))
	)

	// file not exists
	cmp, err := fileSizeUpdated(filePath, latestSize)
	c.Assert(err, ErrorMatches, ".*(no such file or directory|The system cannot find the file specified).*")
	c.Assert(cmp, Equals, 0)

	// create and write the file
	err = ioutil.WriteFile(filePath, data, 0600)
	c.Assert(err, IsNil)

	// equal
	cmp, err = fileSizeUpdated(filePath, latestSize)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)

	// less than
	cmp, err = fileSizeUpdated(filePath, latestSize+1)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)

	// greater than
	cmp, err = fileSizeUpdated(filePath, latestSize-1)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 1)
}

func (t *testUtilSuite) TestRelaySubDirUpdated(c *C) {
	var (
		relayFiles = []string{
			"mysql-bin.000001",
			"mysql-bin.000002",
			"mysql-bin.000003",
			"mysql-bin.000004",
		}
		relayPaths      = make([]string, len(relayFiles))
		data            = []byte("meaningless file content")
		size            = int64(len(data))
		watcherInterval = 100 * time.Millisecond
		updatePathCh    = make(chan string, 1)
		errCh           = make(chan error, 1)
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// a. relay log dir not exist
	relaySubDirUpdated(ctx, watcherInterval, "/not-exists-directory", "/not-exists-filepath", "not-exists-file", 0, updatePathCh, errCh)
	c.Assert(len(errCh), Equals, 1)
	c.Assert(len(updatePathCh), Equals, 0)
	err := <-errCh
	c.Assert(err, ErrorMatches, ".*(no such file or directory|The system cannot find the file specified).*")

	// create relay log dir
	subDir := c.MkDir()
	// join the file path
	for i, rf := range relayFiles {
		relayPaths[i] = filepath.Join(subDir, rf)
	}

	// b. relay file not found
	relaySubDirUpdated(ctx, watcherInterval, subDir, relayPaths[0], relayFiles[0], 0, updatePathCh, errCh)
	c.Assert(len(errCh), Equals, 1)
	c.Assert(len(updatePathCh), Equals, 0)
	err = <-errCh
	c.Assert(err, ErrorMatches, ".*not found.*")

	// create the first relay file
	err = ioutil.WriteFile(relayPaths[0], nil, 0600)
	c.Assert(err, IsNil)

	// c. latest file path not exist
	relaySubDirUpdated(ctx, watcherInterval, subDir, "/no-exists-filepath", relayFiles[0], 0, updatePathCh, errCh)
	c.Assert(len(errCh), Equals, 1)
	c.Assert(len(updatePathCh), Equals, 0)
	err = <-errCh
	c.Assert(err, ErrorMatches, ".*(no such file or directory|The system cannot find the file specified).*")

	// 1. file increased when adding watching
	err = ioutil.WriteFile(relayPaths[0], data, 0600)
	c.Assert(err, IsNil)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		relaySubDirUpdated(ctx, watcherInterval, subDir, relayPaths[0], relayFiles[0], 0, updatePathCh, errCh)
		c.Assert(len(errCh), Equals, 0)
		c.Assert(len(updatePathCh), Equals, 1)
		up := <-updatePathCh
		c.Assert(up, Equals, relayPaths[0])
	}()
	wg.Wait()

	// 2. file decreased when adding watching
	err = ioutil.WriteFile(relayPaths[0], nil, 0600)
	c.Assert(err, IsNil)
	wg.Add(1)
	go func() {
		defer wg.Done()
		relaySubDirUpdated(ctx, watcherInterval, subDir, relayPaths[0], relayFiles[0], size, updatePathCh, errCh)
		c.Assert(len(errCh), Equals, 1)
		c.Assert(len(updatePathCh), Equals, 0)
		err = <-errCh
		c.Assert(err, ErrorMatches, ".*file size of relay log.*become smaller.*")
	}()
	wg.Wait()

	// 3. new file created when adding watching
	err = ioutil.WriteFile(relayPaths[1], nil, 0600)
	c.Assert(err, IsNil)
	wg.Add(1)
	go func() {
		defer wg.Done()
		relaySubDirUpdated(ctx, watcherInterval, subDir, relayPaths[0], relayFiles[0], 0, updatePathCh, errCh)
		c.Assert(len(errCh), Equals, 0)
		c.Assert(len(updatePathCh), Equals, 1)
		up := <-updatePathCh
		c.Assert(up, Equals, relayPaths[1])
	}()
	wg.Wait()

	// 4. file updated
	wg.Add(1)
	go func() {
		defer wg.Done()
		relaySubDirUpdated(ctx, watcherInterval, subDir, relayPaths[1], relayFiles[1], 0, updatePathCh, errCh)
		c.Assert(len(errCh), Equals, 0)
		c.Assert(len(updatePathCh), Equals, 1)
		up := <-updatePathCh
		c.Assert(up, Equals, relayPaths[1])
	}()

	// wait watcher started, update the file
	time.Sleep(2 * watcherInterval)
	err = ioutil.WriteFile(relayPaths[1], data, 0600)
	c.Assert(err, IsNil)
	wg.Wait()

	// 5. file created
	wg.Add(1)
	go func() {
		defer wg.Done()
		relaySubDirUpdated(ctx, watcherInterval, subDir, relayPaths[1], relayFiles[1], size, updatePathCh, errCh)
		c.Assert(len(errCh), Equals, 0)
		c.Assert(len(updatePathCh), Equals, 1)
		up := <-updatePathCh
		c.Assert(up, Equals, relayPaths[2])
	}()

	// wait watcher started, create a new file
	time.Sleep(2 * watcherInterval)
	err = ioutil.WriteFile(relayPaths[2], data, 0600)
	c.Assert(err, IsNil)
	wg.Wait()

	// 6.
	// directory created will be ignored
	// invalid binlog name will be ignored
	// rename file will be ignore
	wg.Add(1)
	go func() {
		defer wg.Done()
		relaySubDirUpdated(ctx, watcherInterval, subDir, relayPaths[2], relayFiles[2], size, updatePathCh, errCh)
		c.Assert(len(errCh), Equals, 0)
		c.Assert(len(updatePathCh), Equals, 1)
		up := <-updatePathCh
		c.Assert(up, Equals, relayPaths[2])
	}()

	// wait watcher started, create new directory
	time.Sleep(2 * watcherInterval)
	err = os.MkdirAll(filepath.Join(subDir, "new-directory"), 0700)
	c.Assert(err, IsNil)

	// wait again, create an invalid binlog file
	time.Sleep(2 * watcherInterval)
	err = ioutil.WriteFile(filepath.Join(subDir, "invalid-binlog-filename"), data, 0600)
	c.Assert(err, IsNil)

	// wait again, rename an older filename, seems watch can not handle `RENAME` operation well
	//time.Sleep(2 * watcherInterval)
	//err = os.Rename(relayPaths[0], relayPaths[3])
	//c.Assert(err, IsNil)

	// wait again, update the file
	time.Sleep(2 * watcherInterval)
	err = ioutil.WriteFile(relayPaths[2], data, 0600)
	c.Assert(err, IsNil)
	wg.Wait()
}

func (t *testFileSuite) TestNeedSwitchSubDir(c *C) {
	var (
		relayDir = c.MkDir()
		UUIDs    = []string{
			"53ea0ed1-9bf8-11e6-8bea-64006a897c73.000001",
			"53ea0ed1-9bf8-11e6-8bea-64006a897c72.000002",
			"53ea0ed1-9bf8-11e6-8bea-64006a897c71.000003",
		}
		currentUUID    = UUIDs[len(UUIDs)-1] // no next UUID
		latestFilePath string
		latestFileSize int64
		data           = []byte("binlog file data")
		switchCh       = make(chan SwitchPath, 1)
		errCh          = make(chan error, 1)
	)

	ctx := context.Background()

	// invalid UUID in UUIDs, error
	UUIDs = append(UUIDs, "invalid.uuid")
	t.writeUUIDs(c, relayDir, UUIDs)
	needSwitchSubDir(ctx, relayDir, currentUUID, latestFilePath, latestFileSize, switchCh, errCh)
	c.Assert(len(errCh), Equals, 1)
	c.Assert(len(switchCh), Equals, 0)
	err := <-errCh
	c.Assert(err, ErrorMatches, ".*not valid.*")

	UUIDs = UUIDs[:len(UUIDs)-1] // remove the invalid UUID
	t.writeUUIDs(c, relayDir, UUIDs)

	// no next UUID, no need switch
	newCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	needSwitchSubDir(newCtx, relayDir, currentUUID, latestFilePath, latestFileSize, switchCh, errCh)
	c.Assert(len(errCh), Equals, 0)
	c.Assert(len(switchCh), Equals, 0)

	// no next sub directory
	currentUUID = UUIDs[0]
	needSwitchSubDir(ctx, relayDir, currentUUID, latestFilePath, latestFileSize, switchCh, errCh)
	c.Assert(len(errCh), Equals, 1)
	c.Assert(len(switchCh), Equals, 0)
	err = <-errCh
	c.Assert(err, ErrorMatches, fmt.Sprintf(".*%s.*(no such file or directory|The system cannot find the file specified).*", UUIDs[1]))

	// create next sub directory, block
	err = os.Mkdir(filepath.Join(relayDir, UUIDs[1]), 0700)
	c.Assert(err, IsNil)
	newCtx2, cancel2 := context.WithTimeout(ctx, 5*time.Second)
	defer cancel2()
	needSwitchSubDir(newCtx2, relayDir, currentUUID, latestFilePath, latestFileSize, switchCh, errCh)
	c.Assert(len(errCh), Equals, 0)
	c.Assert(len(switchCh), Equals, 0)

	// create a relay log file in the next sub directory
	nextBinlogPath := filepath.Join(relayDir, UUIDs[1], "mysql-bin.000001")
	err = os.MkdirAll(filepath.Dir(nextBinlogPath), 0700)
	c.Assert(err, IsNil)
	err = ioutil.WriteFile(nextBinlogPath, nil, 0600)
	c.Assert(err, IsNil)

	// switch to the next
	latestFileSize = int64(len(data))
	needSwitchSubDir(ctx, relayDir, currentUUID, latestFilePath, latestFileSize, switchCh, errCh)
	c.Assert(len(errCh), Equals, 0)
	c.Assert(len(switchCh), Equals, 1)
	res := <-switchCh
	c.Assert(res.nextUUID, Equals, UUIDs[1])
	c.Assert(res.nextBinlogName, Equals, filepath.Base(nextBinlogPath))
}

func (t *testFileSuite) writeUUIDs(c *C, relayDir string, UUIDs []string) []byte {
	indexPath := path.Join(relayDir, utils.UUIDIndexFilename)
	var buf bytes.Buffer
	for _, uuid := range UUIDs {
		_, err := buf.WriteString(uuid)
		c.Assert(err, IsNil)
		_, err = buf.WriteString("\n")
		c.Assert(err, IsNil)
	}

	// write the index file
	err := ioutil.WriteFile(indexPath, buf.Bytes(), 0600)
	c.Assert(err, IsNil)
	return buf.Bytes()
}
