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
	"sort"
	"sync"
	"time"

	. "github.com/pingcap/check"
)

type testLatch struct{}

var _ = Suite(&testLatch{})

func (t *testLatch) TestOneAcquireSuccess(c *C) {
	var (
		l = newLatches()
		fire = make(chan struct{})
		success = make(chan string, 2)
		fail = make(chan struct{}, 8)
		group1 = "group1"
		group2 = "group2"
		wg sync.WaitGroup
	)

	for i := 0; i < 10; i++ {
		var group string
		if i < 5 {
			group = group1
		} else {
			group = group2
		}

		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			<- fire
			_, err := l.acquire(name)
			if err != nil {
				fail <- struct{}{}
			} else {
				success <- name
			}
		}(group)
	}

	for i := 0; i < 10; i++ {
		fire <- struct{}{}
	}
	wg.Wait()

	c.Assert(fail, HasLen, 8)

	var succNames []string
	succNames = append(succNames, <-success)
	succNames = append(succNames, <-success)
	sort.Strings(succNames)
	c.Assert(succNames, DeepEquals, []string{group1, group2})
}

func (t *testLatch) TestAcquireAfterRelease(c *C) {
	var (
		l = newLatches()
		fire = make(chan struct{})
		success int
		group = "group1"
		wg sync.WaitGroup
	)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<- fire

			for {
				release, err := l.acquire(group)
				if err != nil {
					time.Sleep(10*time.Millisecond)
				} else {
					time.Sleep(20*time.Millisecond)
					success++
					release()
					return
				}
			}
		}()
	}

	for i := 0; i < 5; i++ {
		fire <- struct{}{}
	}

	wg.Wait()
	c.Assert(success, Equals, 5)
}

func (t *testLatch) TestMultiRelease(c *C) {
	var (
		l = newLatches()
		names = []string{"name1", "name2", "name3"}
		fire = make(chan struct{}, len(names))
		wg sync.WaitGroup
	)

	for repeat := 0; repeat < 3; repeat ++ {
		for i := range names {
			wg.Add(1)
			go func(name string) {
				defer wg.Done()
				<- fire
				release, err := l.acquire(name)
				c.Assert(err, IsNil)
				release()
				// will not panic or cause other error
				release()
			}(names[i])
		}

		for _ = range names {
			fire <- struct{}{}
		}
		wg.Wait()
	}
}

func (t *testLatch) TestWontReleaseOther(c *C) {
	var (
		l = newLatches()
		group = "group1"
	)

	release1, err := l.acquire(group)
	c.Assert(err, IsNil)
	release1()

	// because release1 is called, another acquire should succeed
	release2, err := l.acquire(group)
	c.Assert(err, IsNil)

	// release1 should not release the latch of release2, we test this by try acquire
	release1()
	_, err = l.acquire(group)
	c.Assert(err, NotNil)

	release2()
	_, err = l.acquire(group)
	c.Assert(err, IsNil)
}