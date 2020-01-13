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

package master

/*
 * sharding DDL sync description
 *
 * assumption:
 * 1. every table in the database is sharding, and must wait for other dm-workers to sync before continue
 * 2. sub task process unit hangs up before sharding DDL resolved
 *    so, every process unit only waiting for one DDL to resolve at one time
 *
 * DDL (lock) info communication:
 *   dm-worker: gRPC server
 *   dm-master: gRPC client
 *   bidirectional streaming RPC between dm-worker and dm-master
 *   do ping-pong on stream
 *     Send / Recv on dm-worker
 *     Recv / Send on dm-master
 *
 * one DDL lock can do re-try-sync by same dm-worker multi times, reentrant
 *
 * normal work flow
 * 1. sub task process unit encounters DDL when syncing
 * 2. process unit saves DDL info and hangs self up
 * 3. dm-worker fetches saved DDL info, and tries to sending it to dm-master
 *    if sends fail, saves DDL info back, and returns to wait dm-master retry on stream
 *    when dm-master retrying on stream, continue from STEP-3
 * 4. dm-master receives DDL info from stream
 *    if receives fail, closes the stream and retries requests on new stream when timeout
 *    when retrying on new stream, dm-worker re-sends DDL info in STEP-3
 * 5. dm-master tries sync DDL lock, and sends results back to dm-worker
 *    if sends fail, closes the stream and retries requests on new stream when timeout
 *    when retrying on new stream, dm-worker re-sends DDL info in STEP-3
 * 6. dm-worker receives DDL lock info
 *    if receives fail, saves DDL info back, and returns to wait dm-master retry on new stream
 *    when dm-master retrying on new stream, continue from STEP-3
 * 7. dm-worker records DDL lock info
 *    if records fail, log error
 *    * this may affect the following DDL lock resolve, user need to use dmctl to handle
 * 8. dm-master sends DDL lock resolve request to dm-worker when lock synced
 * 9. dm-worker executes DDL or ignores (skips) DDL
 *    if executes fail, user need to use dmctl to handle
 * 10. process unit continues execution
 *
 * cases that need dmctl to force to unlock / resolve DDL lock
 * 1. some dm-workers offline, so they can not sync the DDL lock
 *    use dmctl to force dm-workers to try-resolve DDL lock (execute / skip the DDL)
 * 2. dm-workers for the same task are syncing on different DDL locks, like
 *    dm-worker-A is syncing on ddl-lock-A, dm-worker-B is syncing on ddl-lock-B
 *    use dmctl to force dm-workers to try-resolve ddl-lock-A or / and ddl-lock-B
 * 3. some dm-workers occurred error when recording DDL lock info
 *    when resolving DDL lock, the dm-worker can not match the DDL lock info
 *    use dmctl to force the dm-worker to execute / skip the DDL which current is blocking
 * 4. some dm-workers occurred error when executing DDL
 *    use dmctl to force the dm-worker to execute / skip the DDL which current is blocking
 * 5. DDL lock info lost after dm-master restarted
 *    use dmctl to force the dm-worker to execute / skip the DDL which current is blocking
 *
 * dmctl operations to handle abnormal cases
 * 1. force dm-master to resolve un-synced DDL lock
 *    dm-workers will try execute / skip the DDL
 *    supporting use a different dm-worker to replace the owner to execute the DDL
 * 2. force dm-worker to execute / skip the DDL which current is blocking
 */

import (
	"fmt"
	"sync"
)

// genDDLLockID generates a DDL lock ID
// NOTE: refine to include DDL type or other info?
func genDDLLockID(task, schema, table string) string {
	return fmt.Sprintf("%s-`%s`.`%s`", task, schema, table)
}

// LockKeeper used to keep and handle DDL lock
type LockKeeper struct {
	sync.RWMutex
	locks map[string]*Lock // lockID -> lock
}

// NewLockKeeper creates a new LockKeeper
func NewLockKeeper() *LockKeeper {
	l := &LockKeeper{
		locks: make(map[string]*Lock),
	}
	return l
}

// TrySync tries to sync the lock
func (lk *LockKeeper) TrySync(task, schema, table, worker string, stmts []string, workers []string) (string, bool, int, error) {
	lockID := genDDLLockID(task, schema, table)
	var (
		l  *Lock
		ok bool
	)

	lk.Lock()
	defer lk.Unlock()

	if l, ok = lk.locks[lockID]; !ok {
		lk.locks[lockID] = NewLock(lockID, task, worker, stmts, workers)
		l = lk.locks[lockID]
	}

	synced, remain, err := l.TrySync(worker, workers, stmts)
	return lockID, synced, remain, err
}

// RemoveLock removes a lock
func (lk *LockKeeper) RemoveLock(lockID string) bool {
	lk.Lock()
	defer lk.Unlock()

	_, ok := lk.locks[lockID]
	delete(lk.locks, lockID)
	return ok
}

// FindLock finds a lock
func (lk *LockKeeper) FindLock(lockID string) *Lock {
	lk.RLock()
	defer lk.RUnlock()

	return lk.locks[lockID]
}

// Locks returns a copy of all locks
func (lk *LockKeeper) Locks() map[string]*Lock {
	lk.RLock()
	defer lk.RUnlock()
	locks := make(map[string]*Lock, len(lk.locks))
	for k, v := range lk.locks {
		locks[k] = v
	}
	return locks
}
