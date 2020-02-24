// Copyright 2020 PingCAP, Inc.
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

package shardddl

import (
	"sync"

	"github.com/pingcap/parser/model"
)

type Lock struct {
	mu sync.RWMutex

	ID    string   // lock's ID
	Task  string   // lock's corresponding task name
	Owner string   // Owner's source ID (not DM-worker's name)
	DDLs  []string // list of DDLs accepted for synchronization
	done  map[string]bool

	impl LockImpl
}

type LockImpl interface {
	AddSources(sources []string)

	TrySync(caller string, ddls []string, newTableInfo *model.TableInfo) (newDDLs []string, err error)
	// UnsyncCount returns number of sources not yet synchronized. Returns 0 if all sources are synchronized.
	UnsyncCount() int
	// ForceSynced forces all sources to become synchronized.
	ForceSynced()
	// Ready returns the sources tracked by this lock, and whether they are synchronized.
	Ready() map[string]bool
}

func NewLock(ID, task, owner string, sources []string, impl LockImpl) *Lock {
	lock := &Lock{
		ID:    ID,
		Task:  task,
		Owner: owner,
		done:  make(map[string]bool, len(sources)),
		impl:  impl,
	}
	for _, source := range sources {
		lock.done[source] = false
	}
	impl.AddSources(sources)
	return lock
}

func (l *Lock) TrySync(caller string, ddls []string, newTableInfo *model.TableInfo, sources []string) (bool, int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// mark new sources as not-done
	for _, s := range sources {
		if _, ok := l.done[s]; !ok {
			l.done[s] = false
		}
	}
	l.impl.AddSources(sources)

	newDDLs, err := l.impl.TrySync(caller, ddls, newTableInfo)
	if err == nil {
		l.DDLs = newDDLs
	}
	remain := l.impl.UnsyncCount()
	return remain <= 0, remain, err
}

// ForceSynced forces to mark the lock as synced.
func (l *Lock) ForceSynced() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.impl.ForceSynced()
}

// IsSynced returns whether the lock has synced.
func (l *Lock) IsSynced() (bool, int) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	remain := l.impl.UnsyncCount()
	return remain <= 0, remain
}

// MarkDone marks the operation of the source as done.
// NOTE: we do not support revert the `done` after marked now.
func (l *Lock) MarkDone(source string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.done[source]; !ok {
		return // do not add it if not exists.
	}
	l.done[source] = true
}

// IsDone returns whether the operation has done for its owner.
func (l *Lock) IsDone(source string) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.done[source]
}

// IsResolved returns whether the lock has resolved (all operations have done).
func (l *Lock) IsResolved() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	for _, done := range l.done {
		if !done {
			return false
		}
	}
	return true
}

// Ready returns the sources sync status or whether they are ready.
func (l *Lock) Ready() map[string]bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.impl.Ready()
}
