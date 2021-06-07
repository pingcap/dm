#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
source $cur/../_utils/shardddl_lib.sh

function DM_001_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
	run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Duplicate column name 'new_col1'" 1
}

function DM_001() {
	run_case 001 "single-source-no-sharding" "init_table 111 112" "clean_table" ""
}

function DM_002_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
	run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_002() {
	run_case 002 "single-source-pessimistic" "init_table 111 112" "clean_table" ""
}

function DM_003_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 3 "fail"
	run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_003() {
	run_case 003 "single-source-pessimistic" "init_table 111 112" "clean_table" "pessimistic"
}

function DM_004_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
	run_sql_tidb_with_retry "select count(1) from ${shardddl}.${tb};" "count(1): 1"
	run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values (2,2)"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_004() {
	run_case 004 "single-source-optimistic" "init_table 111 112" "clean_table" "optimistic"
}

function DM_005_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
	run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values (2,2)"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_005() {
	run_case 005 "single-source-pessimistic" "init_table 111 112" "clean_table" ""
}

function DM_RENAME_TABLE_CASE() {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column a int;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column a int;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column a int;"

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6);"

	run_sql_source1 "rename table ${shardddl1}.${tb1} to ${shardddl1}.${tb3};"
	run_sql_source2 "rename table ${shardddl1}.${tb1} to ${shardddl1}.${tb3};"
	run_sql_source2 "rename table ${shardddl1}.${tb2} to ${shardddl1}.${tb4};"

	run_sql_source1 "insert into ${shardddl1}.${tb3} values(7,7)"
	run_sql_source2 "insert into ${shardddl1}.${tb3} values(8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb4} values(9,9);"

	run_sql_source1 "alter table ${shardddl1}.${tb3} add column b int;"
	run_sql_source2 "alter table ${shardddl1}.${tb3} add column b int;"
	run_sql_source2 "alter table ${shardddl1}.${tb4} add column b int;"

	run_sql_source1 "insert into ${shardddl1}.${tb3} values(10,10,10)"
	run_sql_source2 "insert into ${shardddl1}.${tb3} values(11,11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb4} values(12,12,12);"

	if [[ "$1" = "pessimistic" ]]; then
		check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	else
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"\`RENAME TABLE\` statement not supported in $1 mode" 2
	fi
}

function DM_RENAME_TABLE() {
	run_case RENAME_TABLE "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
	run_case RENAME_TABLE "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_RENAME_COLUMN_OPTIMISTIC_CASE() {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'ccc');"

	run_sql_source1 "alter table ${shardddl1}.${tb1} change a c int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,'ddd');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,'eee');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,'fff');"

	run_sql_source2 "alter table ${shardddl1}.${tb1} change a c int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,'ggg');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,'hhh');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,'iii');"

	run_sql_source2 "alter table ${shardddl1}.${tb2} change a c int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,'jjj');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,'kkk');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,'lll');"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"because schema conflict detected" 2

	# first, execute sql in downstream TiDB
	run_sql_tidb "alter table ${shardddl}.${tb} change a c int;"

	# second, skip the unsupported ddl
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"handle-error test skip" \
		"\"result\": true" 3

	# dmls fail
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Paused" 2
	#"Error 1054: Unknown column 'a' in 'field list'" 2 // may more than 2 dml error

	# third, set schema to be same with upstream
	# TODO: support set schema automatically base on upstream schema
	echo 'CREATE TABLE `tb1` ( `c` int NOT NULL, `b` varchar(10) DEFAULT NULL, PRIMARY KEY (`c`)) ENGINE=InnoDB DEFAULT CHARSET=latin1' >${WORK_DIR}/schema1.sql
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-schema set -s mysql-replica-01 test -d ${shardddl1} -t ${tb1} ${WORK_DIR}/schema1.sql --flush --sync" \
		"\"result\": true" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-schema set -s mysql-replica-02 test -d ${shardddl1} -t ${tb1} ${WORK_DIR}/schema1.sql --flush --sync" \
		"\"result\": true" 2

	# fourth, resume-task. don't check "result: true" here, because worker may run quickly and meet the error from tb2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-task test"

	# WARN: if it's sequence_sharding, the other tables will not be fixed
	# source2.table2's dml fails
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Error 1054: Unknown column 'a' in 'field list'" 1

	# WARN: set schema of source2.table2
	# Actually it should be tb2(a,b), dml is {a: 9, b: 'iii'}
	# Now we set it to tb2(c,b), dml become {c: 9, b: 'iii'}
	# This may only work for a "rename ddl"
	echo 'CREATE TABLE `tb2` ( `c` int NOT NULL, `b` varchar(10) DEFAULT NULL, PRIMARY KEY (`c`)) ENGINE=InnoDB DEFAULT CHARSET=latin1' >${WORK_DIR}/schema2.sql
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-schema set -s mysql-replica-02 test -d ${shardddl1} -t ${tb2} ${WORK_DIR}/schema2.sql --flush --sync" \
		"\"result\": true" 2

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-task test -s mysql-replica-02"

	# source2.table2's ddl fails
	# Unknown column 'a' in 'tb2'
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unknown column 'a' in 'tb2'" 1

	# skip source2.table2's ddl
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"handle-error test skip -s mysql-replica-02" \
		"\"result\": true" 2

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 3

	# now, it works as normal
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column d int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(13,'mmm',13);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(14,'nnn');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(15,'ooo');"

	run_sql_source2 "alter table ${shardddl1}.${tb1} add column d int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(16,'ppp',16);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(17,'qqq',17);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(18,'rrr');"

	run_sql_source2 "alter table ${shardddl1}.${tb2} add column d int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(19,'sss',19);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(20,'ttt',20);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(21,'uuu',21);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 3
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"show-ddl-locks" \
		"no DDL lock exists" 1
}

# workaround of rename column in optimistic mode currently until we support it
# maybe also work for some other unsupported ddls in optimistic mode
function DM_RENAME_COLUMN_OPTIMISTIC() {
	run_case RENAME_COLUMN_OPTIMISTIC "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10)) DEFAULT CHARSET=latin1;\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10)) DEFAULT CHARSET=latin1;\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10)) DEFAULT CHARSET=latin1;\"" \
		"clean_table" "optimistic"
}

function DM_RECOVER_LOCK_CASE() {
	# tb1(a,b) tb2(a,b)
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(2,2);"

	# tb1(a,b,c); tb2(a,b)
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column c varchar(10);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(3,3,'aaa');"
	check_log_contain_with_retry "putted a shard DDL.*tb1.*ALTER TABLE .* ADD COLUMN" $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log

	# tb1(a,b,c); tb2(a)
	run_sql_source2 "alter table ${shardddl1}.${tb2} drop column b;"
	check_log_contain_with_retry "putted a shard DDL.*tb2.*ALTER TABLE .* DROP COLUMN" $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log

	restart_master

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop column b;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,'bbb');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(5);"
	check_log_contain_with_retry "putted a shard DDL.*tb1.*ALTER TABLE .* DROP COLUMN" $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log

	# tb1(a,c); tb2(a,b)
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column b int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(6,'ccc');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(7,7);"
	check_log_contain_with_retry "putted a shard DDL.*tb2.*ALTER TABLE .* ADD COLUMN" $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log

	# recover lock, tb1's info: (a,b,c)->(a,c); tb2's info: (a)->(a,b)
	# joined(a,b,c); tb1(a,b,c); tb2(a)
	# TrySync tb1: joined(a,b,c); tb1(a,c); tb2(a)
	# TrySync tb2: joined(a,c); tb1(a,c); tb2(a,b)
	restart_master

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(8,'eee');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column b int;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column c varchar(10) after a;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,'fff',10);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(11,'ggg',11);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"show-ddl-locks" \
		"no DDL lock exists" 1
}

function DM_RECOVER_LOCK() {
	run_case RECOVER_LOCK "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int) DEFAULT CHARSET=latin1;\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int) DEFAULT CHARSET=latin1;\"" \
		"clean_table" "optimistic"
}

function DM_RemoveLock_CASE() {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'ccc');"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column c double;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column c double;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column c double;"
	check_log_contain_with_retry "wait new ddl info putted into etcd" $WORK_DIR/master/log/dm-master.log
	check_metric_not_contains $MASTER_PORT "dm_master_shard_ddl_error" 3
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop column b;"

	if [[ "$1" = "pessimistic" ]]; then
		check_log_contain_with_retry "found new DDL info" $WORK_DIR/master/log/dm-master.log
	else
		check_log_contain_with_retry "fail to delete shard DDL infos and lock operations" $WORK_DIR/master/log/dm-master.log
	fi

	run_sql_source1 "alter table ${shardddl1}.${tb1} change a a bigint default 10;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} drop column b;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} change a a bigint default 10;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} drop column b;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} change a a bigint default 10;"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_RemoveLock() {
	ps aux | grep dm-master | awk '{print $2}' | xargs kill || true
	check_port_offline $MASTER_PORT1 20
	export GO_FAILPOINTS="github.com/pingcap/dm/dm/master/shardddl/SleepWhenRemoveLock=return(30)"
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member -w" \
		"bound" 2

	run_case RemoveLock "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10));\"" \
		"clean_table" "pessimistic"
	run_case RemoveLock "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10));\"" \
		"clean_table" "optimistic"

	export GO_FAILPOINTS=""
	ps aux | grep dm-master | awk '{print $2}' | xargs kill || true
	check_port_offline $MASTER_PORT1 20
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member -w" \
		"bound" 2
}

function restart_master() {
	echo "restart dm-master"
	kill_dm_master
	check_port_offline $MASTER_PORT 20
	sleep 2

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
}

function DM_RestartMaster_CASE() {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column c double;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column c text;"

	if [[ "$1" = "pessimistic" ]]; then
		# count of 2: `blockingDDLs` and `unresolvedGroups`
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			'ALTER TABLE `shardddl`.`tb` ADD COLUMN `c` DOUBLE' 2 \
			'ALTER TABLE `shardddl`.`tb` ADD COLUMN `c` TEXT' 2
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"show-ddl-locks" \
			'ALTER TABLE `shardddl`.`tb` ADD COLUMN `c`' 1
	else
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			'because schema conflict detected' 1
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"show-ddl-locks" \
			'mysql-replica-01-`shardddl1`.`tb1`' 1 \
			'mysql-replica-02-`shardddl1`.`tb1`' 1
	fi

	restart_master

	if [[ "$1" = "pessimistic" ]]; then
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			'ALTER TABLE `shardddl`.`tb` ADD COLUMN `c` DOUBLE' 2 \
			'ALTER TABLE `shardddl`.`tb` ADD COLUMN `c` TEXT' 2
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"show-ddl-locks" \
			'ALTER TABLE `shardddl`.`tb` ADD COLUMN `c`' 1
	else
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			'because schema conflict detected' 1
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"show-ddl-locks" \
			'mysql-replica-01-`shardddl1`.`tb1`' 1 \
			'mysql-replica-02-`shardddl1`.`tb1`' 1
	fi
}

function DM_RestartMaster() {
	run_case RestartMaster "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"" \
		"clean_table" "pessimistic"

	run_case RestartMaster "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"" \
		"clean_table" "optimistic"
}

function restart_master_on_pos() {
	if [ "$1" = "$2" ]; then
		restart_master
	fi
}

function DM_DropAddColumn_CASE() {
	reset=$2

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2,2);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop column c;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(3,3);"

	restart_master_on_pos $reset "1"

	run_sql_source2 "alter table ${shardddl1}.${tb1} drop column c;"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(4,4);"

	restart_master_on_pos $reset "2"

	# make sure column c is fully dropped in the downstream
	check_log_contain_with_retry 'finish to handle ddls in optimistic shard mode' $WORK_DIR/worker1/log/dm-worker.log
	check_log_contain_with_retry 'finish to handle ddls in optimistic shard mode' $WORK_DIR/worker2/log/dm-worker.log

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"show-ddl-locks" \
		"no DDL lock exists" 1

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop column b;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(5);"

	restart_master_on_pos $reset "3"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column c int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(6,6);"

	restart_master_on_pos $reset "4"

	# make sure task to step in "Sync" stage
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Running\"" 2 \
		"\"unit\": \"Sync\"" 2
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column b int after a;"

	restart_master_on_pos $reset "5"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"because schema conflict detected" 1 \
		"add column b that wasn't fully dropped in downstream" 1

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 3 'fail'
	# no ddl error but have un-synced ddl
	check_metric_not_contains $MASTER_PORT "dm_master_shard_ddl_error" 3
	# 9223372036854775807 is 2**63 -1
	check_metric $MASTER_PORT 'dm_master_ddl_state_number{task="test",type="Un-synced"}' 3 0 9223372036854775807
	# try to fix data
	echo 'CREATE TABLE `tb1` ( `a` int(11) NOT NULL, `b` int(11) DEFAULT NULL, `c` int(11) DEFAULT NULL, PRIMARY KEY (`a`) /*T![clustered_index] NONCLUSTERED */) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin' >${WORK_DIR}/schema.sql
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-schema set test ${WORK_DIR}/schema.sql -s mysql-replica-01 -d ${shardddl1} -t ${tb1}" \
		"\"result\": true" 2

	# skip this error
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"handle-error test skip" \
		"\"result\": true" 2 \
		"\"source 'mysql-replica-02' has no error\"" 1

	run_sql_source1 "update ${shardddl1}.${tb1} set b=1 where a=1;"
	run_sql_source1 "update ${shardddl1}.${tb1} set b=3 where a=3;"
	run_sql_source1 "update ${shardddl1}.${tb1} set b=4 where a=4;"
	run_sql_source1 "update ${shardddl1}.${tb1} set b=6 where a=6;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column c int"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(7,7,7);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_DropAddColumn() {
	for i in $(seq 0 5); do
		echo "run DM_DropAddColumn case #${i}"
		run_case DropAddColumn "double-source-optimistic" \
			"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int, c int);\"; \
            run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int, c int);\"" \
			"clean_table" "optimistic" "$i"
	done
}

function run() {
	init_cluster
	init_database
	start=1
	end=5
	for i in $(seq -f "%03g" ${start} ${end}); do
		DM_${i}
		sleep 1
	done
	DM_RENAME_TABLE
	DM_RENAME_COLUMN_OPTIMISTIC
	DM_RECOVER_LOCK
	DM_RemoveLock
	DM_RestartMaster
	DM_DropAddColumn
}

cleanup_data $shardddl
cleanup_data $shardddl1
cleanup_data $shardddl2
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
