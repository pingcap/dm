package worker

import (
	. "github.com/pingcap/check"
)

func (t *testWorker) TestConfig(c *C) {
	cfg := &Config{}

	err := cfg.configFromFile("./dm-worker.toml")
	c.Assert(err, IsNil)
	c.Assert(cfg.SourceID, Equals, "mysql-replica-01")

	clone1 := cfg.Clone()
	c.Assert(cfg, DeepEquals, clone1)
	clone1.From.Password = "1234"

	clone2, err := cfg.DecryptPassword()
	c.Assert(err, IsNil)
	c.Assert(clone2, DeepEquals, clone1)
}
