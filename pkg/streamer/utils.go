package streamer

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"

	"github.com/pingcap/tidb-enterprise-tools/pkg/utils"
	"github.com/pingcap/tidb-enterprise-tools/pkg/watcher"
)

// getFirstBinlogName gets the first binlog file in relay sub directory
func getFirstBinlogName(baseDir, uuid string) (string, error) {
	subDir := path.Join(baseDir, uuid)
	files, err := readDir(subDir)
	if err != nil {
		return "", errors.Annotatef(err, "get binlog file for dir %s", subDir)
	}

	for _, f := range files {
		if f == utils.MetaFilename {
			log.Debugf("[streamer] skip meta file %s", f)
			continue
		}

		_, err := parseBinlogFile(f)
		if err != nil {
			return "", errors.NotValidf("binlog file %s", f)
		}
		return f, nil
	}

	return "", errors.NotFoundf("binlog files in dir %s", subDir)
}

// relaySubDirUpdated checks whether the relay sub directory updated
// including file changed, created, removed, etc.
func relaySubDirUpdated(ctx context.Context, watcherInterval time.Duration, dir string, latestFilePath, latestFile string, latestFileSize int64) (string, error) {
	// create polling watcher
	watcher2 := watcher.NewWatcher()

	// Add before Start
	// no need to Remove, it will be closed and release when return
	err := watcher2.Add(dir)
	if err != nil {
		return "", errors.Annotatef(err, "add watch for relay log dir %s", dir)
	}

	err = watcher2.Start(watcherInterval)
	if err != nil {
		return "", errors.Trace(err)
	}
	defer watcher2.Close()

	type watchResult struct {
		updatePath string
		err        error
	}

	result := make(chan watchResult, 1) // buffered chan to ensure not block the sender even return in the halfway
	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		for {
			select {
			case <-newCtx.Done():
				result <- watchResult{
					updatePath: "",
					err:        newCtx.Err(),
				}
				return
			case err2, ok := <-watcher2.Errors:
				if !ok {
					result <- watchResult{
						updatePath: "",
						err:        errors.Errorf("watcher's errors chan for relay log dir %s closed", dir),
					}
				} else {
					result <- watchResult{
						updatePath: "",
						err:        errors.Annotatef(err2, "relay log dir %s", dir),
					}
				}
				return
			case event, ok := <-watcher2.Events:
				if !ok {
					result <- watchResult{
						updatePath: "",
						err:        errors.Errorf("watcher's events chan for relay log dir %s closed", dir),
					}
					return
				}
				log.Debugf("[streamer] watcher receive event %+v", event)
				if event.IsDirEvent() {
					log.Debugf("[streamer] skip watcher event %+v for directory", event)
					continue
				} else if !event.HasOps(watcher.Modify, watcher.Create) {
					log.Debugf("[streamer] skip uninterested event op %s for file %s", event.Op, event.Path)
					continue
				}
				baseName := path.Base(event.Path)
				_, err2 := GetBinlogFileIndex(baseName)
				if err2 != nil {
					log.Debugf("skip watcher event %+v for invalid relay log file", event)
					continue // not valid binlog created, updated
				}
				result <- watchResult{
					updatePath: event.Path,
					err:        nil,
				}
				return
			}
		}
	}()

	// try collect newer relay log file to check whether newer exists before watching
	// NOTE: I have refine `collectBinlogFiles` in zxc/purge-relay, update this later
	newerFiles, err := collectBinlogFiles(dir, latestFile)
	if err != nil {
		return "", errors.Annotatef(err, "collect newer files from %s in dir %s", latestFile, dir)
	}

	// check the latest relay log file whether updated when adding watching and collecting newer
	cmp, err := fileSizeUpdated(latestFilePath, latestFileSize)
	if err != nil {
		return "", errors.Trace(err)
	} else if cmp < 0 {
		return "", errors.Errorf("file size of relay log %s become smaller", latestFilePath)
	} else if cmp > 0 {
		// the latest relay log file already updated, need to parse from it again (not need to re-collect relay log files)
		return latestFilePath, nil
	} else {
		// check whether newer relay log file exists
		if len(newerFiles) > 1 {
			nextFilePath := filepath.Join(dir, newerFiles[1])
			log.Infof("[streamer] newer relay log file %s already generated, start parse from it", nextFilePath)
			return nextFilePath, nil
		}
	}

	res := <-result
	return res.updatePath, res.err
}

// fileSizeUpdated checks whether the file's size has updated
// return
//   0: not updated
//   1: update to larger
//  -1: update to smaller, should not happen
func fileSizeUpdated(path string, latestSize int64) (int, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, errors.Annotatef(err, "get stat for relay log %s", path)
	}
	currSize := fi.Size()
	if currSize == latestSize {
		return 0, nil
	} else if currSize > latestSize {
		log.Debugf("[streamer] size of relay log file %s has changed from %d to %d", path, latestSize, currSize)
		return 1, nil
	} else {
		panic(fmt.Sprintf("size of relay log %s has changed from %d to %d", path, latestSize, currSize))
	}
}

// constructPosName construct binlog file name with UUID suffix
func constructBinlogName(originalName *binlogFile, uuidSuffix string) string {
	return fmt.Sprintf("%s%s%s%s%s", originalName.baseName, posUUIDSuffixSeparator, uuidSuffix, baseSeqSeparator, originalName.seq)
}
