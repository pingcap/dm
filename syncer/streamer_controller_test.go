package syncer

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
)

func (s *testSyncerSuite) TestIsConnectionRefusedError(c *C) {
	isConnRefusedErr := isConnectionRefusedError(nil)
	c.Assert(isConnRefusedErr, Equals, false)

	isConnRefusedErr = isConnectionRefusedError(errors.New("timeout"))
	c.Assert(isConnRefusedErr, Equals, false)

	isConnRefusedErr = isConnectionRefusedError(errors.New("connect: connection refused"))
	c.Assert(isConnRefusedErr, Equals, true)

}
