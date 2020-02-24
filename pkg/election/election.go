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
	"time"

	"github.com/pingcap/errors"
	"github.com/siddontang/go/sync2"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"

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

	// IsLeader means current compaigner become leader
	IsLeader = "isLeader"
	// RetireFromLeader means current compaigner is old leader, and retired
	RetireFromLeader = "retireFromLeader"
	// IsNotLeader means current compaigner is not old leader and current leader
	IsNotLeader = "isNotLeader"
)

// CampaignerInfo is the campaigner's information
type CampaignerInfo struct {
	ID string `json:"id"`
	// addr is the campaigner's advertise address
	Addr string `json:"addr"`
}

func (c *CampaignerInfo) String() string {
	infoBytes, err := json.Marshal(c)
	if err != nil {
		// this should never happened
		return fmt.Sprintf("id: %s, addr: %s", c.ID, c.Addr)
	}

	return string(infoBytes)
}

func getCompaingerInfo(infoBytes []byte) (*CampaignerInfo, error) {
	info := &CampaignerInfo{}
	err := json.Unmarshal(infoBytes, info)
	if err != nil {
		return nil, err
	}
	return info, nil
}

// Election implements the leader election based on etcd.
type Election struct {
	// the Election instance does not own the client instance,
	// so do not close it in the methods of Election.
	cli        *clientv3.Client
	sessionTTL int
	key        string

	info    *CampaignerInfo
	infoStr string

	ech      chan error
	leaderCh chan string
	isLeader sync2.AtomicBool

	closed sync2.AtomicInt32
	cancel context.CancelFunc
	bgWg   sync.WaitGroup

	l log.Logger
}

// NewElection creates a new etcd leader Election instance and starts the campaign loop.
func NewElection(ctx context.Context, cli *clientv3.Client, sessionTTL int, key, id, addr string) (*Election, error) {
	ctx2, cancel2 := context.WithCancel(ctx)
	e := &Election{
		cli:        cli,
		sessionTTL: sessionTTL,
		key:        key,
		info: &CampaignerInfo{
			ID:   id,
			Addr: addr,
		},
		leaderCh: make(chan string, 1),
		ech:      make(chan error, 1), // size 1 is enough
		cancel:   cancel2,
		l:        log.With(zap.String("component", "election")),
	}
	e.infoStr = e.info.String()

	// try create a session before enter the campaign loop.
	// so we can detect potential error earlier.
	session, err := e.newSession(ctx, newSessionDefaultRetryCnt)
	if err != nil {
		cancel2()
		return nil, terror.ErrElectionCampaignFail.Delegate(err, "create the initial session")
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
	return e.isLeader.Get()
}

// ID returns the current member's ID.
func (e *Election) ID() string {
	return e.info.ID
}

// LeaderInfo returns the current leader's key, ID and address.
// it's similar with https://github.com/etcd-io/etcd/blob/v3.4.3/clientv3/concurrency/election.go#L147.
func (e *Election) LeaderInfo(ctx context.Context) (string, string, string, error) {
	resp, err := e.cli.Get(ctx, e.key, clientv3.WithFirstCreate()...)
	if err != nil {
		return "", "", "", terror.ErrElectionGetLeaderIDFail.Delegate(err)
	} else if len(resp.Kvs) == 0 {
		// no leader currently elected
		return "", "", "", terror.ErrElectionGetLeaderIDFail.Delegate(concurrency.ErrElectionNoLeader)
	}

	leaderInfo, err := getCompaingerInfo(resp.Kvs[0].Value)
	if err != nil {
		return "", "", "", terror.ErrElectionGetLeaderIDFail.Delegate(err)
	}

	return string(resp.Kvs[0].Key), leaderInfo.ID, leaderInfo.Addr, nil
}

// LeaderNotify returns a channel that can fetch notification when the member become the leader or retire from the leader, or get a new leader.
// `true` means become the leader; `false` means retire from the leader or get a new leader.
func (e *Election) LeaderNotify() <-chan string {
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

	var (
		oldLeaderID string
		compaignWg  sync.WaitGroup
	)
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
			e.l.Info("break campaign loop, context is done", zap.String("id", e.info.ID), zap.Error(ctx.Err()))
			return
		default:
		}

		// try to campaign
		elec := concurrency.NewElection(session, e.key)
		ctx2, cancel2 := context.WithCancel(ctx)

		compaignWg.Add(1)
		go func() {
			defer compaignWg.Done()

			e.l.Debug("begin to compaign", zap.String("ID", e.info.ID))
			err2 := elec.Campaign(ctx2, e.infoStr)
			if err2 != nil {
				// err may be ctx.Err(), but this can be handled in `case <-ctx.Done()`
				e.l.Info("fail to campaign", zap.String("ID", e.info.ID), zap.Error(err2))
			}
		}()

		var leaderKey, leaderID string
		eleObserveCh := elec.Observe(ctx2)

	observeElection:
		for {
			select {
			case <-ctx.Done():
				break observeElection
			case <-session.Done():
				break observeElection
			case resp, ok := <-eleObserveCh:
				if !ok {
					break observeElection
				}

				e.l.Info("get response from election observe", zap.String("key", string(resp.Kvs[0].Key)), zap.String("value", string(resp.Kvs[0].Value)))
				leaderKey = string(resp.Kvs[0].Key)
				leaderInfo, err := getCompaingerInfo(resp.Kvs[0].Value)
				if err != nil {
					// this should never happened
					e.l.Error("fail to get leader information", zap.String("value", string(resp.Kvs[0].Value)), zap.Error(err))
					continue
				}
				leaderID = leaderInfo.ID

				if oldLeaderID == leaderID {
					continue
				} else {
					oldLeaderID = leaderID
					break observeElection
				}
			}
		}

		if len(leaderID) == 0 {
			cancel2()
			compaignWg.Wait()
			continue
		}

		if leaderID != e.info.ID {
			e.l.Info("current member is not the leader", zap.String("current member", e.info.ID), zap.String("leader", leaderID))
			e.isNotLeader()
			cancel2()
			compaignWg.Wait()
			continue
		}

		e.toBeLeader() // become the leader now
		e.watchLeader(ctx, session, leaderKey)
		e.retireLeader() // need to re-campaign

		cancel2()
		compaignWg.Wait()
	}
}

func (e *Election) toBeLeader() {
	e.isLeader.Set(true)
	select {
	case e.leaderCh <- IsLeader:
	default:
	}
}

func (e *Election) retireLeader() {
	e.isLeader.Set(false)
	select {
	case e.leaderCh <- RetireFromLeader:
	default:
	}
}

func (e *Election) isNotLeader() {
	e.isLeader.Set(false)
	select {
	case e.leaderCh <- IsNotLeader:
	default:
	}
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
func getLeaderInfo(ctx context.Context, elec *concurrency.Election) (key, ID, addr string, err error) {
	resp, err := elec.Leader(ctx)
	if err != nil {
		return "", "", "", err
	}
	leaderInfo, err := getCompaingerInfo(resp.Kvs[0].Value)
	if err != nil {
		return "", "", "", err
	}

	return string(resp.Kvs[0].Key), leaderInfo.ID, leaderInfo.Addr, nil
}
