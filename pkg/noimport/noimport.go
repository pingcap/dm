package noimport

import "time"

// InitTime records the time of invocation of init().
var InitTime time.Time

func init() {
	InitTime = time.Now()
}
