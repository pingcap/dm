package syncer

import (
	. "github.com/pingcap/check"
)

func (s *testSyncerSuite) TestCausality(c *C) {
	ca := newCausality()
	caseData := []string{"test_1", "test_2", "test_3"}
	excepted := map[string]string{
		"test_1": "test_1",
		"test_2": "test_1",
		"test_3": "test_1",
	}
	c.Assert(ca.add(caseData), IsNil)
	c.Assert(ca.relations, DeepEquals, excepted)
	c.Assert(ca.add([]string{"test_4"}), IsNil)
	excepted["test_4"] = "test_4"
	c.Assert(ca.relations, DeepEquals, excepted)
	conflictData := []string{"test_4", "test_3"}
	c.Assert(ca.detectConflict(conflictData), IsTrue)
	c.Assert(ca.add(conflictData), NotNil)
	ca.reset()
	c.Assert(ca.relations, HasLen, 0)
}
