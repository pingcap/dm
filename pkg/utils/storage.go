package utils

import (
	"reflect"

	"github.com/juju/errors"
	"golang.org/x/sys/unix"
)

// StorageSize represents the storage's capacity and available size
// Learn from tidb-binlog source code.
type StorageSize struct {
	Capacity  uint64
	Available uint64
}

// GetStorageSize gets storage's capacity and available size
func GetStorageSize(dir string) (size StorageSize, err error) {
	var stat unix.Statfs_t

	err = unix.Statfs(dir, &stat)
	if err != nil {
		return size, errors.Trace(err)
	}

	// When container is run in MacOS, `bsize` obtained by `statfs` syscall is not the fundamental block size,
	// but the `iosize` (optimal transfer block size) instead, it's usually 1024 times larger than the `bsize`.
	// for example `4096 * 1024`. To get the correct block size, we should use `frsize`. But `frsize` isn't
	// guaranteed to be supported everywhere, so we need to check whether it's supported before use it.
	// For more details, please refer to: https://github.com/docker/for-mac/issues/2136
	bSize := uint64(stat.Bsize)
	field := reflect.ValueOf(&stat).Elem().FieldByName("Frsize")
	if field.IsValid() {
		if field.Kind() == reflect.Uint64 {
			bSize = field.Uint()
		} else {
			bSize = uint64(field.Int())
		}
	}

	// Available blocks * size per block = available space in bytes
	size.Available = stat.Bavail * bSize
	size.Capacity = stat.Blocks * bSize

	return
}
