package master

import (
	"testing"

	. "github.com/pingcap/check"
)

func TestMaster(t *testing.T) {
	TestingT(t)
}

type testMaster struct {
}

var _ = Suite(&testMaster{})
