package noimport

import "time"

var InitTime time.Time

func init() {
	InitTime = time.Now()
}