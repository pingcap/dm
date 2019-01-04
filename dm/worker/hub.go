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
