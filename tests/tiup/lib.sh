#!/bin/bash

set -eu

export TEST_DIR=/tmp/dm_test
export TEST_NAME="upgrade-via-tiup"

WORK_DIR=$TEST_DIR/$TEST_NAME
mkdir -p $WORK_DIR

TASK_NAME="upgrade_via_tiup"

DB1=sharding1
DB2=sharding2
DB3=opt_sharding1
DB4=opt_sharding2
TBL1=t1
TBL2=t2
TBL3=t3

function exec_sql() {
    echo $3 | mysql -h $1 -P $2
}

function install_sync_diff() {
    curl http://download.pingcap.org/tidb-enterprise-tools-latest-linux-amd64.tar.gz | tar xz
    mkdir -p bin
    mv tidb-enterprise-tools-latest-linux-amd64/bin/sync_diff_inspector bin/
}

function exec_full_stage() {
    # drop previous data
    exec_sql mysql1 3306 "DROP DATABASE IF EXISTS $DB1;" 
    exec_sql mysql2 3306 "DROP DATABASE IF EXISTS $DB2;" 
    exec_sql mysql1 3306 "DROP DATABASE IF EXISTS $DB3;" 
    exec_sql mysql2 3306 "DROP DATABASE IF EXISTS $DB4;" 
    exec_sql tidb 4000 "DROP DATABASE IF EXISTS db_target;"
    exec_sql tidb 4000 "DROP DATABASE IF EXISTS opt_db_target;"
    exec_sql tidb 4000 "DROP DATABASE IF EXISTS dm_meta;"

    # # prepare full data
    exec_sql mysql1 3306 "CREATE DATABASE $DB1;"
    exec_sql mysql2 3306 "CREATE DATABASE $DB2;"
    exec_sql mysql1 3306 "CREATE TABLE $DB1.$TBL1 (c1 INT PRIMARY KEY, c2 TEXT);"
    exec_sql mysql1 3306 "CREATE TABLE $DB1.$TBL2 (c1 INT PRIMARY KEY, c2 TEXT);"
    exec_sql mysql2 3306 "CREATE TABLE $DB2.$TBL2 (c1 INT PRIMARY KEY, c2 TEXT);"
    exec_sql mysql2 3306 "CREATE TABLE $DB2.$TBL3 (c1 INT PRIMARY KEY, c2 TEXT);"

    exec_sql mysql1 3306 "INSERT INTO $DB1.$TBL1 (c1, c2) VALUES (1, '1');"
    exec_sql mysql1 3306 "INSERT INTO $DB1.$TBL2 (c1, c2) VALUES (2, '2');"
    exec_sql mysql2 3306 "INSERT INTO $DB2.$TBL2 (c1, c2) VALUES (11, '11');"
    exec_sql mysql2 3306 "INSERT INTO $DB2.$TBL3 (c1, c2) VALUES (12, '12');"

    # prepare optimsitic full data
    exec_sql mysql1 3306 "CREATE DATABASE $DB3;"
    exec_sql mysql2 3306 "CREATE DATABASE $DB4;"
    exec_sql mysql1 3306 "CREATE TABLE $DB3.$TBL1 (c1 INT PRIMARY KEY, c2 TEXT);"
    exec_sql mysql1 3306 "CREATE TABLE $DB3.$TBL2 (c1 INT PRIMARY KEY, c2 TEXT);"
    exec_sql mysql2 3306 "CREATE TABLE $DB4.$TBL2 (c1 INT PRIMARY KEY, c2 TEXT);"
    exec_sql mysql2 3306 "CREATE TABLE $DB4.$TBL3 (c1 INT PRIMARY KEY, c2 TEXT);"

    exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL1 (c1, c2) VALUES (1, '1');"
    exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL2 (c1, c2) VALUES (2, '2');"
    exec_sql mysql2 3306 "INSERT INTO $DB4.$TBL2 (c1, c2) VALUES (11, '11');"
    exec_sql mysql2 3306 "INSERT INTO $DB4.$TBL3 (c1, c2) VALUES (12, '12');"
}

function exec_incremental_stage1() {
    # prepare incremental data
    exec_sql mysql1 3306 "INSERT INTO $DB1.$TBL1 (c1, c2) VALUES (101, '101');"
    exec_sql mysql1 3306 "INSERT INTO $DB1.$TBL2 (c1, c2) VALUES (102, '102');"
    exec_sql mysql2 3306 "INSERT INTO $DB2.$TBL2 (c1, c2) VALUES (111, '111');"
    exec_sql mysql2 3306 "INSERT INTO $DB2.$TBL3 (c1, c2) VALUES (112, '112');"

    # prepare optimistic incremental data
    exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL1 (c1, c2) VALUES (101, '101');"
    exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL2 (c1, c2) VALUES (102, '102');"
    exec_sql mysql2 3306 "INSERT INTO $DB4.$TBL2 (c1, c2) VALUES (111, '111');"
    exec_sql mysql2 3306 "INSERT INTO $DB4.$TBL3 (c1, c2) VALUES (112, '112');"

    # optimistic shard ddls
    exec_sql mysql1 3306 "ALTER TABLE $DB3.$TBL1 ADD COLUMN c3 INT;"
    exec_sql mysql1 3306 "ALTER TABLE $DB3.$TBL2 ADD COLUMN c4 INT;"
    exec_sql mysql2 3306 "ALTER TABLE $DB4.$TBL2 ADD COLUMN c3 INT;"
    exec_sql mysql2 3306 "ALTER TABLE $DB4.$TBL3 ADD COLUMN c4 INT;"

    # prepare optimistic incremental data
    exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL1 (c1, c2, c3) VALUES (103, '103', 103);"
    exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL2 (c1, c2, c4) VALUES (104, '104', 104);"
    exec_sql mysql2 3306 "INSERT INTO $DB4.$TBL2 (c1, c2, c3) VALUES (113, '113', 113);"
    exec_sql mysql2 3306 "INSERT INTO $DB4.$TBL3 (c1, c2, c4) VALUES (114, '113', 113);"
}

function exec_incremental_stage2() {
    # prepare incremental data
    exec_sql mysql1 3306 "INSERT INTO $DB1.$TBL1 (c1, c2) VALUES (201, '201');"
    exec_sql mysql1 3306 "INSERT INTO $DB1.$TBL2 (c1, c2) VALUES (202, '202');"
    exec_sql mysql2 3306 "INSERT INTO $DB2.$TBL2 (c1, c2) VALUES (211, '211');"
    exec_sql mysql2 3306 "INSERT INTO $DB2.$TBL3 (c1, c2) VALUES (212, '212');"

    # prepare optimistic incremental data
    exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL1 (c1, c2, c3) VALUES (201, '201', 201);"
    exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL2 (c1, c2, c4) VALUES (202, '202', 202);"
    exec_sql mysql2 3306 "INSERT INTO $DB4.$TBL2 (c1, c2, c3) VALUES (211, '211', 211);"
    exec_sql mysql2 3306 "INSERT INTO $DB4.$TBL3 (c1, c2, c4) VALUES (212, '212', 212);"

    # optimistic shard ddls
    exec_sql mysql1 3306 "ALTER TABLE $DB3.$TBL1 ADD COLUMN c4 INT;"
    exec_sql mysql1 3306 "ALTER TABLE $DB3.$TBL2 ADD COLUMN c3 INT AFTER c2;"
    exec_sql mysql2 3306 "ALTER TABLE $DB4.$TBL2 ADD COLUMN c4 INT;"
    exec_sql mysql2 3306 "ALTER TABLE $DB4.$TBL3 ADD COLUMN c3 INT AFTER c2;"

    # prepare optimistic incremental data
    exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL1 (c1, c2, c3, c4) VALUES (203, '203', 203, 203);"
    exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL2 (c1, c2, c3, c4) VALUES (204, '204', 204, 204);"
    exec_sql mysql2 3306 "INSERT INTO $DB4.$TBL2 (c1, c2, c3, c4) VALUES (213, '213', 213, 203);"
    exec_sql mysql2 3306 "INSERT INTO $DB4.$TBL3 (c1, c2, c3, c4) VALUES (214, '213', 213, 204);"
}
