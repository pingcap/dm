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

package worker

import (
	"sync"
)

var (
	conditionHub *ConditionHub
	once         sync.Once
)

// ConditionHub holds a DM-worker and it is used for wait condition detection
type ConditionHub struct {
	w *Worker
}

// InitConditionHub inits the singleton instance of ConditionHub
func InitConditionHub(w *Worker) {
	once.Do(func() {
		conditionHub = &ConditionHub{
			w: w,
		}
	})
}

// GetConditionHub returns singleton instance of ConditionHub
func GetConditionHub() *ConditionHub {
	return conditionHub
}
