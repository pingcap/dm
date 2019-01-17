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

package purger

// Config is the configuration for Purger
type Config struct {
	Interval    int64 `toml:"interval" json:"interval"`         // check whether need to purge at this @Interval (seconds)
	Expires     int64 `toml:"expires" json:"expires"`           // if file's modified time is older than @Expires (hours), then it can be purged
	RemainSpace int64 `toml:"remain-space" json:"remain-space"` // if remain space in @RelayBaseDir less than @RemainSpace (GB), then it can be purged
}
