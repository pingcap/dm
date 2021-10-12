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

package utils

import (
	"strings"
	"time"

	"github.com/pingcap/tidb/types"

	"github.com/pingcap/dm/pkg/terror"
)

// ParseTimeZone parse the time zone location by name or offset
//
// NOTE: we don't support the `SYSTEM` time_zone.
func ParseTimeZone(s string) (*time.Location, error) {
	if strings.EqualFold(s, "SYSTEM") {
		return nil, terror.ErrConfigInvalidTimezone.New("'SYSTEM' time_zone is not supported")
	}

	loc, err := time.LoadLocation(s)
	if err == nil {
		return loc, nil
	}

	// The value can be given as a string indicating an offset from UTC, such as '+10:00' or '-6:00'.
	// The time zone's value should in [-12:59,+14:00].
	if strings.HasPrefix(s, "+") || strings.HasPrefix(s, "-") {
		d, err := types.ParseDuration(nil, s[1:], 0)
		if err == nil {
			if s[0] == '-' {
				if d.Duration > 12*time.Hour+59*time.Minute {
					return nil, terror.ErrConfigInvalidTimezone.Generate(s)
				}
			} else {
				if d.Duration > 14*time.Hour {
					return nil, terror.ErrConfigInvalidTimezone.Generate(s)
				}
			}

			ofst := int(d.Duration / time.Second)
			if s[0] == '-' {
				ofst = -ofst
			}
			return time.FixedZone("", ofst), nil
		}
	}

	return nil, terror.ErrConfigInvalidTimezone.Generate(s)
}
