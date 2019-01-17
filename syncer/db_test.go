package syncer

import (
	. "github.com/pingcap/check"
	gouuid "github.com/satori/go.uuid"

	"github.com/pingcap/dm/pkg/utils"
)

func (s *testSyncerSuite) TestGetServerUUID(c *C) {
	uuid, err := utils.GetServerUUID(s.db, "mysql")
	c.Assert(err, IsNil)
	_, err = gouuid.FromString(uuid)
	c.Assert(err, IsNil)
}

func (s *testSyncerSuite) TestGetServerID(c *C) {
	id, err := utils.GetServerID(s.db)
	c.Assert(err, IsNil)
	c.Assert(id, Greater, int64(0))
}
