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
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/siddontang/go/sync2"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
)

const (
	// newSessionDefaultRetryCnt is the default retry times when creating new session.
	newSessionDefaultRetryCnt = 3
	// newSessionRetryUnlimited is the unlimited retry times when creating new session.
	newSessionRetryUnlimited = math.MaxInt64
	// newSessionRetryInterval is the interval time when retrying to create a new session.
	newSessionRetryInterval = 200 * time.Millisecond
)

// Election implements the leader election based on etcd.
type Election struct {
	// the Election instance does not own the client instance,
	// so do not close it in the methods of Election.
	cli        *clientv3.Client
	sessionTTL int
	key        string
	meta       NodeMeta
	ech        chan error
	leaderCh   chan bool
	isLeader   sync2.AtomicBool
	leaderMeta atomic.Value

	closed sync2.AtomicInt32
	cancel context.CancelFunc
	bgWg   sync.WaitGroup

	l log.Logger
}

// NodeMeta represent node name/address that stored in etcd master key
type NodeMeta struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

func (m *NodeMeta) String() string {
	return fmt.Sprintf("NodeMeta[id: %s, address: %s]", m.ID, m.Address)
}

// NewElection creates a new etcd leader Election instance and starts the campaign loop.
func NewElection(
	ctx context.Context,
	cli *clientv3.Client,
	sessionTTL int,
	key, id, address string,
) (*Election, error) {
	ctx2, cancel2 := context.WithCancel(ctx)
	e := &Election{
		cli:        cli,
		sessionTTL: sessionTTL,
		key:        key,
		meta:       NodeMeta{id, address},
		leaderCh:   make(chan bool, 1),
		ech:        make(chan error, 1), // size 1 is enough
		cancel:     cancel2,
		l:          log.With(zap.String("component", "election")),
	}

	// try create a session before enter the campaign loop.
	// so we can detect potential error earlier.
	session, err := e.newSession(ctx, newSessionDefaultRetryCnt)
	if err != nil {
		cancel2()
		return nil, terror.ErrElectionCampaignFail.Delegate(err, "create the initial session")
	}

	for {
		_, meta, err := e.LeaderInfo(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if meta == nil {
			log.L().Warn("seems there is no leader now, try campaign leader.")
			err1 := e.tryCampaignLeader(ctx, session.Lease())
			if err1 != nil {
				return nil, errors.Trace(err1)
			}
		} else {
			if meta.ID == e.meta.ID {
				e.toBeLeader()
			} else {
				err = e.updateLeader(meta)
				if err != nil {
					return nil, err
				}
			}
			break
		}
	}

	e.bgWg.Add(1)
	go func() {
		defer e.bgWg.Done()
		e.campaignLoop(ctx2, session)
	}()
	return e, nil
}

// IsLeader returns whether this member is the leader.
func (e *Election) IsLeader() bool {
	meta := e.leaderMeta.Load().(*NodeMeta)
	return meta != nil && meta.ID == e.meta.ID
}

// ID returns the current member's ID.
func (e *Election) ID() string {
	return e.meta.ID
}

// LeaderInfo returns the current leader's key and ID.
// it's similar with https://github.com/etcd-io/etcd/blob/v3.4.3/clientv3/concurrency/election.go#L147.
func (e *Election) LeaderInfo(ctx context.Context) (string, *NodeMeta, error) {
	resp, err := e.cli.Get(ctx, e.key, clientv3.WithFirstCreate()...)
	if err != nil {
		return "", nil, terror.ErrElectionGetLeaderIDFail.Delegate(err)
	} else if len(resp.Kvs) == 0 {
		// no leader currently elected
		return "", nil, nil
	}
	var meta NodeMeta
	err = json.Unmarshal(resp.Kvs[0].Value, &meta)
	if err != nil {
		return "", nil, terror.ErrElectionGetLeaderIDFail.Delegate(err)
	}
	return string(resp.Kvs[0].Key), &meta, nil
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

func (e *Election) campaignLoop(ctx context.Context, session *concurrency.Session) {
	closeSession := func(se *concurrency.Session) {
		err2 := se.Close() // only log this error
		if err2 != nil {
			e.l.Error("fail to close etcd session", zap.Int64("lease", int64(se.Lease())), zap.Error(err2))
		}
	}

	var err error
	defer func() {
		if session != nil {
			closeSession(session) // close the latest session.
		}
		if err != nil && errors.Cause(err) != ctx.Err() { // only send non-ctx.Err() error
			e.ech <- err
		}
	}()

	for {
		// check context canceled/timeout
		select {
		case <-session.Done():
			e.l.Info("etcd session is done, will try to create a new one", zap.Int64("old lease", int64(session.Lease())))
			closeSession(session)
			session, err = e.newSession(ctx, newSessionRetryUnlimited) // retry until context is done
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
		if !e.IsLeader() {
			elec := concurrency.NewElection(session, e.key)
			val, _ := json.Marshal(e.meta)
			err = elec.Campaign(ctx, string(val))
			if err != nil {
				// err may be ctx.Err(), but this can be handled in `case <-ctx.Done()`
				e.l.Warn("fail to campaign", zap.Error(err))
				continue
			}

			// compare with the current leader
			_, leaderMeta, err := getLeaderInfo(ctx, elec)
			if err != nil {
				// err may be ctx.Err(), but this can be handled in `case <-ctx.Done()`
				e.l.Warn("fail to get leader ID", zap.Error(err))
				continue
			}
			// no leader available, leader key maybe delete, continue campaign
			if leaderMeta == nil {
				continue
			}

			if leaderMeta.ID != e.meta.ID {
				e.l.Info("current member is not the leader", zap.String("current member", e.meta.ID), zap.String("leader", leaderMeta.ID))
				// leader change, must update lead info and gRPC client to leader
				if *leaderMeta != e.leaderMeta.Load() {
					for i := 0; i < 3; i++ {
						err = e.updateLeader(leaderMeta)
						if err == nil {
							break
						}
						log.L().Error("update leader failed", zap.Stringer("leader", leaderMeta))
						time.Sleep(200 * time.Millisecond)
					}
				}

				continue
			}

			e.toBeLeader() // become the leader now
		}

		e.watchLeader(ctx, session, e.key)
		_, leaderMeta, err := e.LeaderInfo(ctx)
		if err != nil {
			log.L().Warn("seems no leader is available, ")
		} else {
			if err := e.retireLeader(leaderMeta); err != nil {
				e.ech <- err
			}
		}
	}
}

func (e *Election) tryCampaignLeader(ctx context.Context, leaseID clientv3.LeaseID) error {
	value, _ := json.Marshal(&e.meta)
	_, err := e.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(e.key), "=", 0)).
		Then(clientv3.OpPut(e.key, string(value), clientv3.WithLease(leaseID))).
		Commit()
	return errors.WithStack(err)
}

func (e *Election) toBeLeader() {
	e.leaderMeta.Store(e.meta)
	select {
	case e.leaderCh <- true:
	default:
	}
}

func (e *Election) retireLeader(leaderMeta *NodeMeta) error {
	err := e.updateLeader(leaderMeta)
	select {
	case e.leaderCh <- false:
	default:
	}
	return err
}

func (e *Election) updateLeader(leaderMeta *NodeMeta) error {
	e.leaderMeta.Store(leaderMeta)
	if leaderMeta != nil {
		return common.InitClient([]string{leaderMeta.Address}, false)
	}
	common.ResetMasterClient()
	return nil
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
				// user may use some etcd client (like etcdctl) to delete the leader key and trigger a new campaign.
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

func (e *Election) newSession(ctx context.Context, retryCnt int) (*concurrency.Session, error) {
	var (
		err     error
		session *concurrency.Session
	)

forLoop:
	for i := 0; i < retryCnt; i++ {
		if i > 0 {
			select {
			case e.ech <- terror.ErrElectionCampaignFail.Delegate(err, "create a new session"):
			default:
			}

			select {
			case <-time.After(newSessionRetryInterval):
			case <-ctx.Done():
				break forLoop
			}
		}

		// add more options if needed.
		// NOTE: I think use the client's context is better than something like `concurrency.WithContext(ctx)`,
		// so we can close the session when the client is still valid.
		session, err = concurrency.NewSession(e.cli, concurrency.WithTTL(e.sessionTTL))
		if err == nil || errors.Cause(err) == e.cli.Ctx().Err() {
			break forLoop
		}
	}
	return session, err
}

// getLeaderInfo get the current leader's information (if exists).
func getLeaderInfo(ctx context.Context, elec *concurrency.Election) (key string, meta *NodeMeta, err error) {
	resp, err := elec.Leader(ctx)
	if err != nil {
		return
	}
	leaderMeta := &NodeMeta{}
	err = json.Unmarshal(resp.Kvs[0].Value, leaderMeta)
	if err != nil {
		return
	}
	return string(resp.Kvs[0].Key), leaderMeta, nil
}
