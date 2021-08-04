// Copyright 2021 PingCAP, Inc.
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

package scheduler

import (
	"sync"

	"github.com/pingcap/errors"
)

// latches provides a simple way to block concurrent accessing to one resource, if caller tries to acquire latch before
// accessing resources.
type latches struct {
	mu    sync.Mutex
	inUse map[string]struct{}
	// TODO: use map[string]semaphore to implement a blocking acquire
}

// releaseFunc wraps on releasing a latch.
// It is safe to call multiple times. Also compiler can warn you of not used releaseFunc variables.
type releaseFunc func()

func newLatches() *latches {
	return &latches{
		inUse: map[string]struct{}{},
	}
}

func (l *latches) tryAcquire(name string) (releaseFunc, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.inUse[name]; ok {
		return nil, errors.Errorf("%s is in use by other client", name)
	}

	l.inUse[name] = struct{}{}
	var once sync.Once
	return func() {
		once.Do(func() {
			l.release(name)
		})
	}, nil
}

// release should not be called directly, it's recommended to wrap it with releaseFunc to avoid release a latch that not
// belongs to caller.
func (l *latches) release(name string) {
	l.mu.Lock()
	delete(l.inUse, name)
	l.mu.Unlock()
}
