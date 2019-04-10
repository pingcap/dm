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

package reader

import (
	"context"

	"github.com/pingcap/errors"
)

// isRetryableError checks whether the error is retryable.
func isRetryableError(err error) bool {
	err = errors.Cause(err)
	switch err {
	case context.DeadlineExceeded:
		return true
	}
	return false
}

// isIgnorableError checks whether the error is ignorable.
func isIgnorableError(err error) bool {
	err = errors.Cause(err)
	switch err {
	case context.Canceled:
		return true
	}
	return false
}
