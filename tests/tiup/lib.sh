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
	exec_sql mariadb2 3306 "DROP DATABASE IF EXISTS $DB2;"
	exec_sql mysql1 3306 "DROP DATABASE IF EXISTS $DB3;"
	exec_sql mariadb2 3306 "DROP DATABASE IF EXISTS $DB4;"
	exec_sql tidb 4000 "DROP DATABASE IF EXISTS db_target;"
	exec_sql tidb 4000 "DROP DATABASE IF EXISTS opt_db_target;"
	exec_sql tidb 4000 "DROP DATABASE IF EXISTS dm_meta;"

	# # prepare full data
	exec_sql mysql1 3306 "CREATE DATABASE $DB1;"
	exec_sql mariadb2 3306 "CREATE DATABASE $DB2;"
	exec_sql mysql1 3306 "CREATE TABLE $DB1.$TBL1 (c1 INT PRIMARY KEY, c2 TEXT);"
	exec_sql mysql1 3306 "CREATE TABLE $DB1.$TBL2 (c1 INT PRIMARY KEY, c2 TEXT);"
	exec_sql mariadb2 3306 "CREATE TABLE $DB2.$TBL2 (c1 INT PRIMARY KEY, c2 TEXT);"
	exec_sql mariadb2 3306 "CREATE TABLE $DB2.$TBL3 (c1 INT PRIMARY KEY, c2 TEXT);"

	exec_sql mysql1 3306 "INSERT INTO $DB1.$TBL1 (c1, c2) VALUES (1, '1');"
	exec_sql mysql1 3306 "INSERT INTO $DB1.$TBL2 (c1, c2) VALUES (2, '2');"
	exec_sql mariadb2 3306 "INSERT INTO $DB2.$TBL2 (c1, c2) VALUES (11, '11');"
	exec_sql mariadb2 3306 "INSERT INTO $DB2.$TBL3 (c1, c2) VALUES (12, '12');"

	# prepare optimsitic full data
	exec_sql mysql1 3306 "CREATE DATABASE $DB3;"
	exec_sql mariadb2 3306 "CREATE DATABASE $DB4;"
	exec_sql mysql1 3306 "CREATE TABLE $DB3.$TBL1 (c1 INT PRIMARY KEY, c2 TEXT);"
	exec_sql mysql1 3306 "CREATE TABLE $DB3.$TBL2 (c1 INT PRIMARY KEY, c2 TEXT);"
	exec_sql mariadb2 3306 "CREATE TABLE $DB4.$TBL2 (c1 INT PRIMARY KEY, c2 TEXT);"
	exec_sql mariadb2 3306 "CREATE TABLE $DB4.$TBL3 (c1 INT PRIMARY KEY, c2 TEXT);"

	exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL1 (c1, c2) VALUES (1, '1');"
	exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL2 (c1, c2) VALUES (2, '2');"
	exec_sql mariadb2 3306 "INSERT INTO $DB4.$TBL2 (c1, c2) VALUES (11, '11');"
	exec_sql mariadb2 3306 "INSERT INTO $DB4.$TBL3 (c1, c2) VALUES (12, '12');"
}

function exec_incremental_stage1() {
	# prepare incremental data
	exec_sql mysql1 3306 "INSERT INTO $DB1.$TBL1 (c1, c2) VALUES (101, '101');"
	exec_sql mysql1 3306 "INSERT INTO $DB1.$TBL2 (c1, c2) VALUES (102, '102');"
	exec_sql mariadb2 3306 "INSERT INTO $DB2.$TBL2 (c1, c2) VALUES (111, '111');"
	exec_sql mariadb2 3306 "INSERT INTO $DB2.$TBL3 (c1, c2) VALUES (112, '112');"

	# prepare optimistic incremental data
	exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL1 (c1, c2) VALUES (101, '101');"
	exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL2 (c1, c2) VALUES (102, '102');"
	exec_sql mariadb2 3306 "INSERT INTO $DB4.$TBL2 (c1, c2) VALUES (111, '111');"
	exec_sql mariadb2 3306 "INSERT INTO $DB4.$TBL3 (c1, c2) VALUES (112, '112');"

	# optimistic shard ddls
	exec_sql mysql1 3306 "ALTER TABLE $DB3.$TBL1 ADD COLUMN c3 INT;"
	exec_sql mysql1 3306 "ALTER TABLE $DB3.$TBL2 ADD COLUMN c4 INT;"
	exec_sql mariadb2 3306 "ALTER TABLE $DB4.$TBL2 ADD COLUMN c3 INT;"
	exec_sql mariadb2 3306 "ALTER TABLE $DB4.$TBL3 ADD COLUMN c4 INT;"

	# prepare optimistic incremental data
	exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL1 (c1, c2, c3) VALUES (103, '103', 103);"
	exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL2 (c1, c2, c4) VALUES (104, '104', 104);"
	exec_sql mariadb2 3306 "INSERT INTO $DB4.$TBL2 (c1, c2, c3) VALUES (113, '113', 113);"
	exec_sql mariadb2 3306 "INSERT INTO $DB4.$TBL3 (c1, c2, c4) VALUES (114, '113', 113);"
}

function exec_incremental_stage2() {
	# prepare incremental data
	exec_sql mysql1 3306 "INSERT INTO $DB1.$TBL1 (c1, c2) VALUES (201, '201');"
	exec_sql mysql1 3306 "INSERT INTO $DB1.$TBL2 (c1, c2) VALUES (202, '202');"
	exec_sql mariadb2 3306 "INSERT INTO $DB2.$TBL2 (c1, c2) VALUES (211, '211');"
	exec_sql mariadb2 3306 "INSERT INTO $DB2.$TBL3 (c1, c2) VALUES (212, '212');"

	# prepare optimistic incremental data
	exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL1 (c1, c2, c3) VALUES (201, '201', 201);"
	exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL2 (c1, c2, c4) VALUES (202, '202', 202);"
	exec_sql mariadb2 3306 "INSERT INTO $DB4.$TBL2 (c1, c2, c3) VALUES (211, '211', 211);"
	exec_sql mariadb2 3306 "INSERT INTO $DB4.$TBL3 (c1, c2, c4) VALUES (212, '212', 212);"

	# optimistic shard ddls
	exec_sql mysql1 3306 "ALTER TABLE $DB3.$TBL1 ADD COLUMN c4 INT;"
	exec_sql mysql1 3306 "ALTER TABLE $DB3.$TBL2 ADD COLUMN c3 INT AFTER c2;"
	exec_sql mariadb2 3306 "ALTER TABLE $DB4.$TBL2 ADD COLUMN c4 INT;"
	exec_sql mariadb2 3306 "ALTER TABLE $DB4.$TBL3 ADD COLUMN c3 INT AFTER c2;"

	# prepare optimistic incremental data
	exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL1 (c1, c2, c3, c4) VALUES (203, '203', 203, 203);"
	exec_sql mysql1 3306 "INSERT INTO $DB3.$TBL2 (c1, c2, c3, c4) VALUES (204, '204', 204, 204);"
	exec_sql mariadb2 3306 "INSERT INTO $DB4.$TBL2 (c1, c2, c3, c4) VALUES (213, '213', 213, 213);"
	exec_sql mariadb2 3306 "INSERT INTO $DB4.$TBL3 (c1, c2, c3, c4) VALUES (214, '214', 214, 214);"
}

function patch_nightly_with_tiup_mirror() {
	# clone packages for upgrade.
	# FIXME: use nightly version of grafana and prometheus after https://github.com/pingcap/tiup/issues/1334 fixed.
	tiup mirror clone tidb-dm-nightly-linux-amd64 --os=linux --arch=amd64 \
		--alertmanager=v0.17.0 --grafana=v5.0.1 --prometheus=v5.0.1 \
		--tiup=v$(tiup --version | grep 'tiup' | awk -F ' ' '{print $1}') --dm=v$(tiup --version | grep 'tiup' | awk -F ' ' '{print $1}')

	# change tiup mirror
	tidb-dm-nightly-linux-amd64/local_install.sh

	# publish nightly version
	# binary files have already been built and packaged.
	tiup mirror genkey
	tiup mirror grant gmhdbjd --name gmhdbjd
	tiup mirror publish dm-master nightly /tmp/dm-master-nightly-linux-amd64.tar.gz dm-master/dm-master --arch amd64 --os linux --desc="dm-master component of Data Migration Platform"
	tiup mirror publish dm-worker nightly /tmp/dm-worker-nightly-linux-amd64.tar.gz dm-worker/dm-worker --arch amd64 --os linux --desc="dm-worker component of Data Migration Platform"
	tiup mirror publish dmctl nightly /tmp/dmctl-nightly-linux-amd64.tar.gz dmctl/dmctl --arch amd64 --os linux --desc="dmctl component of Data Migration Platform"

	tiup list
}

function run_dmctl_with_retry() {
	dmctl_log="dmctl.log"
	for ((k = 0; k < 10; k++)); do
		tiup dmctl:$1 --master-addr=master1:8261 $2 >$dmctl_log 2>&1
		all_matched=true
		for ((i = 3; i < $#; i += 2)); do
			j=$((i + 1))
			value=${!i}
			expected=${!j}
			got=$(sed "s/$value/$value\n/g" $dmctl_log | grep -c "$value" || true)
			if [ "$got" != "$expected" ]; then
				all_matched=false
				break
			fi
		done

		if $all_matched; then
			return 0
		fi

		sleep 2
	done

	cat $dmctl_log
	exit 1
}
