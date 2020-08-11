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

package upgrade

import "go.etcd.io/etcd/clientv3"

var (
	// upgrades records all functions used to upgrade from one version to the later version.
	upgrades = []func(cli *clientv3.Client) error{
		upgradeToVer1,
	}
)

// TryUpgrade tries to upgrade the cluster from an older version to a new version.
// This methods should have no side effects even calling multiple times.
func TryUpgrade(cli *clientv3.Client) error {
	// 1. get previous version from etcd.
	preVer, _, err := GetVersion(cli)
	if err != nil {
		return err
	}

	// 2. check if any previous version exists.
	if preVer.NotSet() {
		// no initialization operations exist for Ver1 now,
		// add any operations (may includes `upgrades`) if needed later.
		// put the current version into etcd.
		_, err = PutVersion(cli, CurrentVersion)
		return err
	}

	// 3. compare the previous version with the current version.
	if cmp := preVer.Compare(CurrentVersion); cmp == 0 {
		// previous == current version, no need to upgrade.
		return nil
	} else if cmp > 0 {
		// previous >= current version, this often means a older version of DM-master become the leader after started,
		// do nothing for this now.
		return nil
	}

	// 4. do upgrade operations.
	for _, upgrade := range upgrades {
		err = upgrade(cli)
		if err != nil {
			return err
		}
	}

	// 5. put the current version into etcd.
	_, err = PutVersion(cli, CurrentVersion)
	return err
}

// upgradeToVer1 does upgrade operations from Ver0 to Ver1.
// in fact, this do nothing now, and just for demonstration.
func upgradeToVer1(cli *clientv3.Client) error {
	return nil
}
