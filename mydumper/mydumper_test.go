package mydumper

import (
	"strings"
	"testing"

	. "github.com/pingcap/check"

	"github.com/pingcap/dm/dm/config"
)

var _ = Suite(&testMydumperSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testMydumperSuite struct {
	cfg *config.SubTaskConfig
}

func (m *testMydumperSuite) SetUpSuite(c *C) {
	m.cfg = &config.SubTaskConfig{
		From: config.DBConfig{
			Host:     "127.0.0.1",
			User:     "root",
			Password: "123",
			Port:     3306,
		},
		MydumperConfig: config.MydumperConfig{
			MydumperPath:  "./bin/mydumper",
			Threads:       4,
			SkipTzUTC:     true,
			ChunkFilesize: 64,
		},
		LoaderConfig: config.LoaderConfig{
			Dir: "./dumped_data",
		},
	}
}

func (m *testMydumperSuite) TestArgs(c *C) {
	expected := strings.Fields("--host 127.0.0.1 --port 3306 --user root --password 123 " +
		"--outputdir ./dumped_data --threads 4 --chunk-filesize 64 --skip-tz-utc " +
		"--regex ^(?!(mysql|information_schema|performance_schema))")
	m.cfg.MydumperConfig.ExtraArgs = "--regex '^(?!(mysql|information_schema|performance_schema))'"
	mydumper := NewMydumper(m.cfg)
	args := mydumper.constructArgs()
	c.Assert(args, DeepEquals, expected)
}
