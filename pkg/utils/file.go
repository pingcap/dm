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

package utils

import (
	"io"
	"io/ioutil"
	"os"
	"path"

	"github.com/pingcap/dm/pkg/terror"
)

// IsFileExists checks if file exists.
func IsFileExists(name string) bool {
	f, err := os.Stat(name)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}

	if f.IsDir() {
		return false
	}

	return true
}

// IsDirExists checks if dir exists.
func IsDirExists(name string) bool {
	f, err := os.Stat(name)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}

	if !f.IsDir() {
		return false
	}

	return true
}

// GetFileSize return the size of the file.
// NOTE: do not support to get the size of the directory now.
func GetFileSize(file string) (int64, error) {
	if !IsFileExists(file) {
		return 0, terror.ErrGetFileSize.Generate(file)
	}

	stat, err := os.Stat(file)
	if err != nil {
		return 0, terror.ErrGetFileSize.Delegate(err, file)
	}
	return stat.Size(), nil
}

// WriteFileAtomic writes file to temp and atomically move when everything else succeeds.
func WriteFileAtomic(filename string, data []byte, perm os.FileMode) error {
	dir, name := path.Dir(filename), path.Base(filename)
	f, err := ioutil.TempFile(dir, name)
	if err != nil {
		return err
	}
	n, err := f.Write(data)
	f.Close()
	if err == nil && n < len(data) {
		err = io.ErrShortWrite
	} else {
		err = os.Chmod(f.Name(), perm)
	}
	if err != nil {
		os.Remove(f.Name())
		return err
	}
	return os.Rename(f.Name(), filename)
}
