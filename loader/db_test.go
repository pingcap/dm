package loader

import (
	"bufio"
	"os"
	"path"
	"strconv"

	. "github.com/pingcap/check"

	"github.com/pingcap/dm/dm/config"
	tcontext "github.com/pingcap/dm/pkg/context"
)

var _ = Suite(&testdDbSuite{})

type testdDbSuite struct {
	cfg *config.SubTaskConfig
}

type msgs struct {
	testMsg  string
	testData string
	testKey  string
}

func (t *testdDbSuite) SetUpSuite(c *C) {
	host := os.Getenv("MYSQL_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port, _ := strconv.Atoi(os.Getenv("MYSQL_PORT"))
	if port == 0 {
		port = 3306
	}
	user := os.Getenv("MYSQL_USER")
	if user == "" {
		user = "root"
	}
	pswd := os.Getenv("MYSQL_PSWD")
	t.cfg = &config.SubTaskConfig{
		To: config.DBConfig{
			Host:     host,
			User:     user,
			Password: pswd,
			Port:     port,
		},
		MetaSchema: "test",
	}
	t.cfg.To.Adjust()
}

func (t *testdDbSuite) TestProcessErrSQL(c *C) {
	tctx := tcontext.Background()

	dir := c.MkDir()
	t.cfg.ErrDataFile = path.Join(dir, "dupDataFile.txt")
	t.cfg.To.Adjust()

	conn, err := createConn(t.cfg)
	c.Assert(err, IsNil)
	defer closeConn(conn)

	db := conn.baseConn.GetDb()
	txn, err := db.Begin()
	c.Assert(err, IsNil)

	_, err = txn.Exec("USE test;")
	c.Assert(err, IsNil)

	sql := "DROP TABLE `testTableLoaderDb`;"
	_, err = txn.Exec(sql)

	sql = "CREATE TABLE `testTableLoaderDb` (`a` varchar(255) DEFAULT NULL,`b` varchar(255) NOT NULL,`c` varchar(255) DEFAULT NULL,`d` int(11) DEFAULT NULL,PRIMARY KEY (`b`)) ENGINE=InnoDB DEFAULT CHARSET=latin1;"

	_, err = txn.Exec(sql)
	c.Assert(err, IsNil)

	for i := 0; i < 2; i++ {
		// when i = 0, need to determine the existence of duplicate data

		sql = "INSERT IGNORE INTO `testTableLoaderDb` VALUES\n('111','aaa','!!!',1),\n('222','bbb','###',2),\n('333','ccc','$$$',3);"

		_, err = txn.Exec(sql)
		c.Assert(err, IsNil)
	}

	processErrSQL(tctx, conn, txn, sql)

	strs := []string{
		"*********************",
		"",
		"Column: b",
		"Data: aaa",
		"Pos: 2",
		"INSERT INTO `testTableLoaderDb` VALUES('111','aaa','!!!',1);",
		"*********************",
		"",
		"Column: b",
		"Data: bbb",
		"Pos: 2",
		"INSERT INTO `testTableLoaderDb` VALUES('222','bbb','###',2);",
		"*********************",
		"",
		"Column: b",
		"Data: ccc",
		"Pos: 2",
		"INSERT INTO `testTableLoaderDb` VALUES('333','ccc','$$$',3);",
	}
	dupDataFile := path.Join(dir, "dupDataFile.txt")
	fi, err := os.Open(dupDataFile)
	c.Assert(err, IsNil)
	br := bufio.NewReader(fi)
	for i := 0; i < len(strs); i++ {
		res, _, err := br.ReadLine()
		if i == 1 || i == 7 || i == 13 {
			continue
		}
		c.Assert(err, IsNil)
		c.Assert(string(res), Equals, strs[i])
	}

	sql = "DROP TABLE `testTableLoaderDb`;"
	_, err = txn.Exec(sql)
	c.Assert(err, IsNil)

	err = txn.Commit()
	c.Assert(err, IsNil)
}

func (t *testdDbSuite) TestGetDupEntry(c *C) {
	var (
		testMsgs = []msgs{
			{"Duplicate entry '*^%^&&' for key 'PRIMARY'", "*^%^&&", "PRIMARY"},
			{"Duplicate entry ',trx',''/13,.x' for key 'a'", ",trx',''/13,.x", "a"},
			{"Duplicate entry '*&*!@#$!' for key 'bbbb'", "*&*!@#$!", "bbbb"},
			{"Duplicate entry '<>?:\"{}|' for key '^%$#dsa'", "<>?:\"{}|", "^%$#dsa"},
			{"Duplicate entry '-=,./';[]' for key '~gf`h'gf*&^'", "-=,./';[]", "~gf`h'gf*&^"},
			{"Duplicate entry '`~!@#?%&*()+' for key '<\">?{}\\|、'", "`~!@#?%&*()+", "<\">?{}\\|、"},
		}
		data string
		key  string
		err  error
	)

	for i := 0; i < len(testMsgs); i++ {
		data, key, err = getDupEntry(testMsgs[i].testMsg)
		c.Assert(data, Equals, testMsgs[i].testData)
		c.Assert(key, Equals, testMsgs[i].testKey)
		c.Assert(err, IsNil)
	}

	// test error message display is not correct
	msg := "Duplicate entry 'aaa' key 'PRIMARY'"

	data, key, err = getDupEntry(msg)
	c.Assert(data, Equals, "")
	c.Assert(key, Equals, "")
	c.Assert(err, NotNil)
}

func (t *testdDbSuite) TestFindDataPair(c *C) {
	target := "INSERT IGNORE INTO `testTableLoaderDb` VALUES\n('111','aaa','!!!',1),\n('222','bbb','###',2),\n('333','ccc','$$$',3);"
	dupData := "aaa"
	pos := 2

	data, err := findDataPair(target, dupData, pos)
	c.Assert(data, Equals, "('111','aaa','!!!',1)")
	c.Assert(err, IsNil)

	pos = 3
	data, err = findDataPair(target, dupData, pos)
	c.Assert(data, Equals, "")
	c.Assert(err, NotNil)
}

func (t *testdDbSuite) TestDupEntryLog(c *C) {
	dir := c.MkDir()
	dupDataFile := path.Join(dir, "dupDataFile.txt")
	dupDataPair := "('111','aaa','!!!',1)"
	errTable := "testTableLoaderDb"
	errData := "aaa"
	errColumn := "b"
	errPos := "2"
	strs := []string{
		"*********************",
		"",
		"Column: " + errColumn,
		"Data: " + errData,
		"Pos: " + errPos,
		"INSERT INTO `" + errTable + "` VALUES" + dupDataPair + ";",
	}
	_, err := os.Stat(dupDataFile) // Detects if the file exists
	if !os.IsNotExist(err) {
		err = os.Remove(dupDataFile)
		c.Assert(err, IsNil)
	}

	err = dupEntryLog(dupDataFile, dupDataPair, errTable, errData, errColumn, errPos)
	c.Assert(err, IsNil)

	fi, err := os.Open(dupDataFile)
	c.Assert(err, IsNil)
	br := bufio.NewReader(fi)
	for i := 0; i < len(strs); i++ {
		res, _, err := br.ReadLine()
		if i == 1 {
			continue
		}
		c.Assert(err, IsNil)
		c.Assert(string(res), Equals, strs[i])
	}
}
