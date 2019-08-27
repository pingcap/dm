package utils

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"

	_ "github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
)

var _ = Suite(&testdDbSuite{})

type testdDbSuite struct {
	host string
	port int
	user string
	pswd string
	db   *sql.DB
}

func (t *testdDbSuite) SetUpSuite(c *C) {
	t.host = os.Getenv("MYSQL_HOST")
	if t.host == "" {
		t.host = "127.0.0.1"
	}
	t.port, _ = strconv.Atoi(os.Getenv("MYSQL_PORT"))
	if t.port == 0 {
		t.port = 3306
	}
	t.user = os.Getenv("MYSQL_USER")
	if t.user == "" {
		t.user = "root"
	}
	t.pswd = os.Getenv("MYSQL_PSWD")
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4", t.user, t.pswd, t.host, t.port)
	db, err := sql.Open("mysql", dsn)
	c.Assert(err, IsNil)
	t.db = db
}

func (t *testdDbSuite) TestShowWarnings(c *C) {
	txn, err := t.db.Begin()
	c.Assert(err, IsNil)

	query := "DROP DATABASE `test_pkg_utils_db`"
	_, err = txn.Exec(query)

	query = "CREATE DATABASE `test_pkg_utils_db`"
	_, err = txn.Exec(query)
	c.Assert(err, IsNil)

	query = "USE test_pkg_utils_db;"
	_, err = txn.Exec(query)
	c.Assert(err, IsNil)

	query = "CREATE TABLE `testTablePkgUtilsDb` (`a` varchar(255) DEFAULT NULL,`b` varchar(255) NOT NULL,`c` varchar(255) DEFAULT NULL,`d` int(11) DEFAULT NULL,PRIMARY KEY (`b`)) ENGINE=InnoDB DEFAULT CHARSET=latin1;"

	_, err = txn.Exec(query)
	c.Assert(err, IsNil)

	for i := 0; i < 2; i++ {
		// when i = 0, need to determine the existence of duplicate data

		query = "INSERT IGNORE INTO `testTablePkgUtilsDb` VALUES\n('111','aaa','!!!',1),\n('222','bbb','###',2),\n('333','ccc','$$$',3);"
		_, err = txn.Exec(query)
		c.Assert(err, IsNil)
	}

	warns, err := ShowWarnings(txn)
	c.Assert(err, IsNil)

	strs := []string{
		"Duplicate entry 'aaa' for key 'PRIMARY'",
		"Duplicate entry 'bbb' for key 'PRIMARY'",
		"Duplicate entry 'ccc' for key 'PRIMARY'",
	}
	for i := 0; i < len(warns); i++ {
		c.Assert(warns[i].Level, Equals, "Warning")
		c.Assert(warns[i].Code, Equals, uint16(1062))
		c.Assert(warns[i].Message, Equals, strs[i])
	}

	query = "DROP DATABASE `test_pkg_utils_db`"
	_, err = txn.Exec(query)
	c.Assert(err, IsNil)
}

func (t *testdDbSuite) TestGetDbInfoFromInfoSchema(c *C) {
	target := "SELECT column_name FROM INFORMATION_SCHEMA.`KEY_COLUMN_USAGE` WHERE table_name='a' AND constraint_name='b';"
	targetValue := "column_name"
	targetStructure := "KEY_COLUMN_USAGE"
	var queryCondition = []Conditions{
		{"table_name", "a"},
		{"constraint_name", "b"},
	}
	query := GetDbInfoFromInfoSchema(targetValue, targetStructure, queryCondition)
	c.Assert(query, Equals, target)
}
