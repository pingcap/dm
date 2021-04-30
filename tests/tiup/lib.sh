#!/bin/bash

set -eu

export TEST_DIR=/tmp/dm_test
export TEST_NAME="upgrade-via-tiup"

WORK_DIR=$TEST_DIR/$TEST_NAME
mkdir -p $WORK_DIR

TASK_NAME="upgrade_via_tiup"

DB1=sharding1
DB2=sharding2
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
    exec_sql tidb 4000 "DROP DATABASE IF EXISTS db_target;"
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
}

function exec_incremental_stage1() {
    # prepare incremental data
    exec_sql mysql1 3306 "INSERT INTO $DB1.$TBL1 (c1, c2) VALUES (101, '101');"
    exec_sql mysql1 3306 "INSERT INTO $DB1.$TBL2 (c1, c2) VALUES (102, '102');"
    exec_sql mysql2 3306 "INSERT INTO $DB2.$TBL2 (c1, c2) VALUES (111, '111');"
    exec_sql mysql2 3306 "INSERT INTO $DB2.$TBL3 (c1, c2) VALUES (112, '112');"
}

function exec_incremental_stage2() {
    # prepare incremental data
    exec_sql mysql1 3306 "INSERT INTO $DB1.$TBL1 (c1, c2) VALUES (201, '201');"
    exec_sql mysql1 3306 "INSERT INTO $DB1.$TBL2 (c1, c2) VALUES (202, '202');"
    exec_sql mysql2 3306 "INSERT INTO $DB2.$TBL2 (c1, c2) VALUES (211, '211');"
    exec_sql mysql2 3306 "INSERT INTO $DB2.$TBL3 (c1, c2) VALUES (212, '212');"
}

function run_dmctl_with_retry() {
    dmctl_log="dmctl.log"
    for ((k=0; k<10; k++)); do
        tiup dmctl:$1 --master-addr=master1:8261 $2 > $dmctl_log 2>&1
        all_matched=true
        for ((i=3; i<$#; i+=2)); do
            j=$((i+1))
            value=${!i}
            expected=${!j}
            got=$(sed "s/$value/$value\n/g" $dmctl_log | grep -c "$value")
            if [ "$got" != "$expected" ]; then
                all_matched=false
                break
            fi
        done

        if $all_matched; then
            return
        fi

        sleep 2
    done
    exit 1
}
