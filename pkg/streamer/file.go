package streamer

import (
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-enterprise-tools/pkg/utils"
)

var (
	// ErrInvalidBinlogFilename means error about invalid binlog file name.
	ErrInvalidBinlogFilename = errors.New("invalid binlog file name")
	// ErrEmptyRelayDir means error about empty relay dir.
	ErrEmptyRelayDir = errors.New("empty relay dir")
)

func collectBinlogFiles(dirpath string, firstFile string) ([]string, error) {
	if dirpath == "" {
		return nil, ErrEmptyRelayDir
	}
	files, err := readDir(dirpath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if fp := filepath.Join(dirpath, firstFile); !utils.IsFileExists(fp) {
		return nil, errors.Errorf("%s not exists", fp)
	}

	targetFiles := make([]string, 0, len(files))

	ff, err := parseBinlogFile(firstFile)
	if err != nil {
		return nil, errors.Annotatef(err, "filename %s", firstFile)
	}
	if !allAreDigits(ff.seq) {
		return nil, errors.Errorf("firstfile %s is invalid ", firstFile)
	}

	for _, f := range files {
		// check prefix
		if !strings.HasPrefix(f, ff.baseName) {
			log.Warnf("collecting binlog file, ignore invalid file %s", f)
			continue
		}
		parsed, err := parseBinlogFile(f)
		if err != nil {
			log.Warnf("collecting binlog file, ignore invalid file %s, err %v", f, err)
			continue
		}
		// check suffix
		if !allAreDigits(parsed.seq) {
			log.Warnf("collecting binlog file, ignore invalid file %s", f)
			continue
		}
		if !parsed.BiggerOrEqualThan(ff) {
			log.Debugf("ignore older binlog file %s", f)
			continue
		}

		targetFiles = append(targetFiles, f)
	}

	return targetFiles, nil
}

// GetBinlogFileIndex return a float64 value of index.
func GetBinlogFileIndex(filename string) (float64, error) {
	f, err := parseBinlogFile(filename)
	if err != nil {
		return 0, errors.Errorf("parse binlog file name %s err %v", filename, err)
	}

	idx, err := strconv.ParseFloat(f.seq, 64)
	if err != nil {
		return 0, errors.Errorf("parse binlog index %s to float64, err %v", filename, err)
	}
	return idx, nil
}

func parseBinlogFile(filename string) (*binlogFile, error) {
	// chendahui: I found there will always be only one dot in the mysql binlog name.
	parts := strings.Split(filename, ".")
	if len(parts) != 2 {
		log.Warnf("filename %s not valid", filename)
		return nil, ErrInvalidBinlogFilename
	}

	return &binlogFile{
		baseName: parts[0],
		seq:      parts[1],
	}, nil
}

type binlogFile struct {
	baseName string
	seq      string
}

func (f *binlogFile) BiggerThan(other *binlogFile) bool {
	return f.baseName == other.baseName && f.seq > other.seq
}

func (f *binlogFile) Equal(other *binlogFile) bool {
	return f.baseName == other.baseName && f.seq == other.seq
}

func (f *binlogFile) BiggerOrEqualThan(other *binlogFile) bool {
	return f.baseName == other.baseName && f.seq >= other.seq
}

// [0-9] in string -> [48,57] in ascii
func allAreDigits(s string) bool {
	for _, r := range s {
		if r >= 48 && r <= 57 {
			continue
		}
		return false
	}
	return true
}

// readDir reads and returns all file(sorted asc) and dir names from directory f
func readDir(dirpath string) ([]string, error) {
	dir, err := os.Open(dirpath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer dir.Close()

	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, errors.Annotatef(err, "dir %s", dirpath)
	}

	sort.Strings(names)

	return names, nil
}
