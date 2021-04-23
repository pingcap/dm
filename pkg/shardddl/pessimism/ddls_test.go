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

package pessimism

import (
	. "github.com/pingcap/check"
)

func (t *testForEtcd) TestDDLsEtcd(c *C) {
	defer clearTestInfoOperation(c)

	var (
		ID1   = "test1-`foo`.`bar`"
		ID2   = "test2-`foo`.`bar`"
		DDLs1 = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		DDLs2 = []string{"ALTER TABLE bar ADD COLUMN c2 INT"}
	)

	// put the same keys twice.
	rev1, err := PutLatestDoneDDLs(etcdTestCli, ID1, DDLs1)
	c.Assert(err, IsNil)
	rev2, err := PutLatestDoneDDLs(etcdTestCli, ID1, DDLs1)
	c.Assert(err, IsNil)
	c.Assert(rev2, Greater, rev1)

	// put another DDLs
	rev3, err := PutLatestDoneDDLs(etcdTestCli, ID1, DDLs2)
	c.Assert(err, IsNil)
	c.Assert(rev3, Greater, rev2)

	// put for another lock
	rev4, err := PutLatestDoneDDLs(etcdTestCli, ID2, DDLs1)
	c.Assert(err, IsNil)
	c.Assert(rev4, Greater, rev3)

	// get all ddls.
	latestDoneDDLsMap, rev5, err := GetAllLatestDoneDDLs(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(rev5, Equals, rev4)
	c.Assert(latestDoneDDLsMap, HasLen, 2)
	c.Assert(latestDoneDDLsMap, HasKey, ID1)
	c.Assert(latestDoneDDLsMap, HasKey, ID2)
	c.Assert(latestDoneDDLsMap[ID1], DeepEquals, DDLs2)
	c.Assert(latestDoneDDLsMap[ID2], DeepEquals, DDLs1)
}
