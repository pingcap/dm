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

package election

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/siddontang/go/sync2"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
)

// Election implements the leader election based on etcd.
type Election struct {
	// the Election instance does not own the client instance,
	// so do not close it in the methods of Election.
	cli        *clientv3.Client
	sessionTTL int
	key        string
	id         string
	ech        chan error
	leaderCh   chan bool
	isLeader   sync2.AtomicBool

	closed sync2.AtomicInt32
	cancel context.CancelFunc
	bgWg   sync.WaitGroup

	l log.Logger
}

// NewElection creates a new etcd leader Election instance and starts the campaign loop.
func NewElection(ctx context.Context, cli *clientv3.Client, sessionTTL int, key, id string) *Election {
	ctx2, cancel2 := context.WithCancel(ctx)
	e := &Election{
		cli:        cli,
		sessionTTL: sessionTTL,
		key:        key,
		id:         id,
		leaderCh:   make(chan bool, 1),
		ech:        make(chan error, 1), // size 1 is enough
		cancel:     cancel2,
		l:          log.With(zap.String("component", "election")),
	}
	e.bgWg.Add(1)
	go func() {
		defer e.bgWg.Done()
		e.campaignLoop(ctx2)
	}()
	return e
}

// IsLeader returns whether this member is the leader.
func (e *Election) IsLeader() bool {
	return e.isLeader.Get()
}

// ID returns the current member's ID.
func (e *Election) ID() string {
	return e.id
}

// LeaderInfo returns the current leader's key and ID.
// it's similar with https://github.com/etcd-io/etcd/blob/v3.4.3/clientv3/concurrency/election.go#L147.
func (e *Election) LeaderInfo(ctx context.Context) (string, string, error) {
	resp, err := e.cli.Get(ctx, e.key, clientv3.WithFirstCreate()...)
	if err != nil {
		return "", "", terror.ErrElectionGetLeaderIDFail.Delegate(err)
	} else if len(resp.Kvs) == 0 {
		// no leader currently elected
		return "", "", terror.ErrElectionGetLeaderIDFail.Delegate(concurrency.ErrElectionNoLeader)
	}
	return string(resp.Kvs[0].Key), string(resp.Kvs[0].Value), nil
}

// LeaderNotify returns a channel that can fetch notification when the member become the leader or retire from the leader.
// `true` means become the leader; `false` means retire from the leader.
func (e *Election) LeaderNotify() <-chan bool {
	return e.leaderCh
}

// ErrorNotify returns a channel that can fetch errors occurred for campaign.
func (e *Election) ErrorNotify() <-chan error {
	return e.ech
}

// Close closes the election instance and release the resources.
func (e *Election) Close() {
	e.l.Info("election is closing")
	if !e.closed.CompareAndSwap(0, 1) {
		e.l.Info("election was already closed")
		return
	}

	e.cancel()
	e.bgWg.Wait()
	e.l.Info("election is closed")
}

func (e *Election) campaignLoop(ctx context.Context) {
	var (
		session *concurrency.Session
		err     error
	)

	closeSession := func(se *concurrency.Session) {
		err2 := se.Close() // only log this error
		if err2 != nil {
			e.l.Error("fail to close etcd session", zap.Int64("lease", int64(se.Lease())), zap.Error(err2))
		}
	}

	defer func() {
		if session != nil {
			closeSession(session) // close the latest session.
		}
		if err != nil && errors.Cause(err) != ctx.Err() { // only send non-ctx.Err() error
			e.l.Error("", zap.Error(err))
			e.ech <- err
		}
	}()

	// create the initial session.
	session, err = newSession(e.cli, e.sessionTTL)
	if err != nil {
		err = terror.ErrElectionCampaignFail.Delegate(err, "create the initial session")
		return
	}

	for {
		// check context canceled/timeout
		select {
		case <-session.Done():
			e.l.Info("etcd session is done, will try to create a new one", zap.Int64("old lease", int64(session.Lease())))
			closeSession(session)
			session, err = newSession(e.cli, e.sessionTTL)
			if err != nil {
				err = terror.ErrElectionCampaignFail.Delegate(err, "create a new session")
				return
			}
		case <-ctx.Done():
			e.l.Info("break campaign loop, context is done", zap.Error(ctx.Err()))
			return
		default:
		}

		// try to campaign
		elec := concurrency.NewElection(session, e.key)
		err = elec.Campaign(ctx, e.id)
		if err != nil {
			// err may be ctx.Err(), but this can be handled in `case <-ctx.Done()`
			e.l.Warn("fail to campaign", zap.Error(err))
			continue
		}

		// compare with the current leader
		leaderKey, leaderID, err := getLeaderInfo(ctx, elec)
		if err != nil {
			// err may be ctx.Err(), but this can be handled in `case <-ctx.Done()`
			e.l.Warn("fail to get leader ID", zap.Error(err))
			continue
		}
		if leaderID != e.id {
			e.l.Info("current member is not the leader", zap.String("current member", e.id), zap.String("leader", leaderID))
			continue
		}

		e.toBeLeader() // become the leader now
		e.watchLeader(ctx, session, leaderKey)
		e.retireLeader() // need to re-campaign
	}
}

func (e *Election) toBeLeader() {
	e.isLeader.Set(true)
	select {
	case e.leaderCh <- true:
	default:
	}
	e.l.Info("become the leader", zap.String("member", e.id))
}

func (e *Election) retireLeader() {
	e.isLeader.Set(false)
	select {
	case e.leaderCh <- false:
	default:
	}
	e.l.Info("retire from the leader", zap.String("member", e.id))
}

func (e *Election) watchLeader(ctx context.Context, session *concurrency.Session, key string) {
	e.l.Debug("watch leader key", zap.String("key", key))
	wch := e.cli.Watch(ctx, key)
	for {
		select {
		case resp, ok := <-wch:
			if !ok {
				e.l.Info("watch channel is closed")
				return
			}
			if resp.Canceled {
				e.l.Info("watch canceled")
				return
			}

			for _, ev := range resp.Events {
				if ev.Type == mvccpb.DELETE {
					e.l.Info("fail to watch, the leader is deleted", zap.ByteString("key", ev.Kv.Key))
					return
				}
			}
		case <-session.Done():
			return
		case <-ctx.Done():
			return
		}
	}
}

func newSession(cli *clientv3.Client, ttl int) (*concurrency.Session, error) {
	// add more options if needed.
	return concurrency.NewSession(cli, concurrency.WithTTL(ttl))
}

// getLeaderInfo get the current leader's information (if exists).
func getLeaderInfo(ctx context.Context, elec *concurrency.Election) (key, ID string, err error) {
	resp, err := elec.Leader(ctx)
	if err != nil {
		return
	}
	return string(resp.Kvs[0].Key), string(resp.Kvs[0].Value), nil
}
