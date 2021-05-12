#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
source $cur/../_utils/shardddl_lib.sh

function DM_071_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} default character set utf8mb4 collate utf8mb4_bin;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'ccc');"

	run_sql_source2 "alter table ${shardddl1}.${tb1} default character set utf8mb4 collate utf8mb4_bin;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,'ddd');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,'eee');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,'fff');"

	run_sql_source2 "alter table ${shardddl1}.${tb2} default character set utf8mb4 collate utf8mb4_bin;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,'ggg');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,'hhh');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,'iii');"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_071() {
	run_case 071 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10)) default character set utf8 collate utf8_bin;\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10)) default character set utf8 collate utf8_bin;\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10)) default character set utf8 collate utf8_bin;\"" \
		"clean_table" "pessimistic"

	# schema comparer doesn't support to compare/diff charset now
	# run_case 071 "double-source-optimistic" \
	# "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10)) default character set utf8 collate utf8_bin;\"; \
	#  run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10)) default character set utf8 collate utf8_bin;\"; \
	#  run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10)) default character set utf8 collate utf8_bin;\"" \
	# "clean_table" "optimistic"
}

function DM_073_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} convert to character set utf8mb4 collate utf8mb4_bin;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'ccc');"

	run_sql_source2 "alter table ${shardddl1}.${tb1} convert to character set utf8mb4 collate utf8mb4_bin;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,'ddd');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,'eee');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,'fff');"

	run_sql_source2 "alter table ${shardddl1}.${tb2} convert to character set utf8mb4 collate utf8mb4_bin;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,'ggg');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,'hhh');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,'iii');"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_073() {
	run_case 073 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10)) default character set utf8 collate utf8_bin;\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10)) default character set utf8 collate utf8_bin;\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10)) default character set utf8 collate utf8_bin;\"" \
		"clean_table" "pessimistic"

	# schema comparer doesn't support to compare/diff charset now
	# run_case 073 "double-source-optimistic" \
	# "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10)) default character set utf8 collate utf8_bin;\"; \
	#  run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10)) default character set utf8 collate utf8_bin;\"; \
	#  run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10)) default character set utf8 collate utf8_bin;\"" \
	# "clean_table" "optimistic"
}

function DM_076_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add primary key(id);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_076() {
	run_case 076 "single-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int unique, id int);\"" \
		"clean_table" "pessimistic"
	run_case 076 "single-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int unique, id int);\"" \
		"clean_table" "optimistic"
}

function DM_077_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop primary key;"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unsupported drop primary key" 1
}

function DM_077() {
	run_sql_tidb "create table ${shardddl}.${tb} (id int, primary key(id) clustered);"
	run_case 077 "single-source-pessimistic" "run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key);\"" "clean_table" ""
	run_sql_tidb "create table ${shardddl}.${tb} (id int, primary key(id) clustered);"
	run_case 077 "single-source-optimistic" "run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key);\"" "clean_table" ""
}

function DM_078_CASE() {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values (1, 1, 'wer'), (2, 2, NULL);"
	run_sql_source1 "alter table ${shardddl1}.${tb1} add primary key(a);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values (3, 3, 'wer'), (4, 4, NULL);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_078() {
	# start a TiDB alter-pk
	pkill -hup tidb-server 2>/dev/null || true
	wait_process_exit tidb-server
	run_tidb_server 4000 $TIDB_PASSWORD $cur/conf/tidb-alter-pk-config.toml

	run_case 078 "single-source-pessimistic" "run_sql_source1 \"create table ${shardddl1}.${tb1} (id int unique, a int, b varchar(10));\"" "clean_table" ""
	run_case 078 "single-source-optimistic" "run_sql_source1 \"create table ${shardddl1}.${tb1} (id int unique, a int, b varchar(10));\"" "clean_table" ""

	# don't revert tidb until DM_079
}

function DM_079_CASE() {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values (1, 'wer'), (2, NULL);"
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop primary key;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values (0, 'wer'), (0, NULL);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_079() {
	run_case 079 "single-source-pessimistic" "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"" "clean_table" ""
	run_case 079 "single-source-optimistic" "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"" "clean_table" ""

	# revert tidb
	pkill -hup tidb-server 2>/dev/null || true
	wait_process_exit tidb-server
	run_tidb_server 4000 $TIDB_PASSWORD
}

function DM_080_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add unique key idx_a(a);"
	run_sql_source1 "alter table ${shardddl1}.${tb1} add unique key idx_b(b);"
	run_sql_source1 "alter table ${shardddl1}.${tb1} add unique key idx_ab(a,b);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,'aaa');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,1,'aaa');"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add unique key idx_a(a);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add unique key idx_b(b);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add unique key idx_ab(a,b);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(3,2,'bbb');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(4,2,'bbb');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(5,2,'bbb');"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add unique key idx_a(a);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add unique key idx_b(b);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add unique key idx_ab(a,b);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(6,3,'ccc');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(7,3,'ccc');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(8,3,'ccc');"
	run_sql_tidb_with_retry "select count(1) from ${shardddl}.${tb};" "count(1): 3"
}

function DM_080() {
	run_case 080 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a int, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a int, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a int, b varchar(10));\"" \
		"clean_table" "pessimistic"

	# currently not support optimistic
	#run_case 080 "double-source-optimistic" \
	#"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a int, b varchar(10));\"; \
	# run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a int, b varchar(10));\"; \
	# run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a int, b varchar(10));\"" \
	#"clean_table" "optimistic"
}

function DM_081_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add unique key idx_a(a);"
	run_sql_source1 "alter table ${shardddl1}.${tb1} add unique key idx_b(b);"
	run_sql_source1 "alter table ${shardddl1}.${tb1} add unique key idx_ab(a,b);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add unique key idx_a(a);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add unique key idx_b(b);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add unique key idx_ab(a,b);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add unique key idx_a(a);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add unique key idx_b(b);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add unique key idx_ab(a,b);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop index idx_a;"
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop index idx_b;"
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop index idx_ab;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,'aaa');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,1,'aaa');"
	run_sql_source2 "alter table ${shardddl1}.${tb1} drop index idx_a;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} drop index idx_b;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} drop index idx_ab;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(3,2,'bbb');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(4,2,'bbb');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(5,2,'bbb');"
	run_sql_source2 "alter table ${shardddl1}.${tb2} drop index idx_a;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} drop index idx_b;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} drop index idx_ab;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(6,3,'ccc');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(7,3,'ccc');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(8,3,'ccc');"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_081() {
	run_case 081 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a int, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a int, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a int, b varchar(10));\"" \
		"clean_table" "pessimistic"

	# currently not support optimistic
	# run_case 081 "double-source-optimistic" \
	#"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a int, b varchar(10));\"; \
	# run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a int, b varchar(10));\"; \
	# run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a int, b varchar(10));\"" \
	#"clean_table" "optimistic"
}

function DM_082_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} rename index a to c;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'ccc');"
	run_sql_source2 "alter table ${shardddl1}.${tb1} rename index a to c;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,'ddd');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,'eee');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,'fff');"
	run_sql_source2 "alter table ${shardddl1}.${tb2} rename index a to c;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,'ggg');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,'hhh');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,'iii');"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_082() {
	run_case 082 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int unique key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int unique key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int unique key, b varchar(10));\"" \
		"clean_table" "pessimistic"

	# currently not support optimistic
	# run_case 082 "double-source-optimistic" \
	#"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int unique key, b varchar(10));\"; \
	# run_sql_source2 \"create table ${shardddl1}.${tb1} (a int unique key, b varchar(10));\"; \
	# run_sql_source2 \"create table ${shardddl1}.${tb2} (a int unique key, b varchar(10));\"" \
	#"clean_table" "optimistic"
}

function DM_085_CASE() {
	run_sql_source2 "alter table ${shardddl1}.${tb1} alter index a visible;"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(2,'bbb');"
	run_sql_source2 "insert into ${shardddl1}.${tb3} values(3,'ccc');"
	run_sql_source2 "alter table ${shardddl1}.${tb2} alter index a visible;"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(4,'ddd');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(5,'eee');"
	run_sql_source2 "insert into ${shardddl1}.${tb3} values(6,'fff');"
	run_sql_source2 "alter table ${shardddl1}.${tb3} alter index a visible;"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(7,'ggg');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(8,'hhh');"
	run_sql_source2 "insert into ${shardddl1}.${tb3} values(9,'iii');"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_085() {
	# `ALTER INDEX` not supported in MySQL 5.7, but we setup the second MySQL 8.0 in CI now.
	run_case 085 "single-source-pessimistic-2" \
		"run_sql_source2 \"create table ${shardddl1}.${tb1} (a int unique key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int unique key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb3} (a int unique key, b varchar(10));\"" \
		"clean_table" "pessimistic"

	run_case 085 "single-source-optimistic-2" \
		"run_sql_source2 \"create table ${shardddl1}.${tb1} (a int unique key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int unique key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb3} (a int unique key, b varchar(10));\"" \
		"clean_table" "optimistic"
}

function DM_086_CASE() {
	run_sql_source2 "alter table ${shardddl1}.${tb1} alter index a visible;"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(2,'bbb');"
	run_sql_source2 "insert into ${shardddl1}.${tb3} values(3,'ccc');"
	run_sql_source2 "alter table ${shardddl1}.${tb2} alter index a invisible;"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(4,'ddd');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(5,'eee');"
	run_sql_source2 "insert into ${shardddl1}.${tb3} values(6,'fff');"
	run_sql_source2 "alter table ${shardddl1}.${tb3} alter index a visible;"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(7,'ggg');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(8,'hhh');"
	run_sql_source2 "insert into ${shardddl1}.${tb3} values(9,'iii');"

	if [[ "$1" = "pessimistic" ]]; then
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"detect inconsistent DDL sequence from source" 1
	else
		# schema comparer for optimistic shard DDL can't diff visible/invisible now.
		# may this needs to be failed?
		check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	fi
}

function DM_086() {
	# `ALTER INDEX` not supported in MySQL 5.7, but we setup the second MySQL 8.0 in CI now.
	run_case 086 "single-source-pessimistic-2" \
		"run_sql_source2 \"create table ${shardddl1}.${tb1} (a int unique key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int unique key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb3} (a int unique key, b varchar(10));\"" \
		"clean_table" "pessimistic"

	run_case 086 "single-source-optimistic-2" \
		"run_sql_source2 \"create table ${shardddl1}.${tb1} (a int unique key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int unique key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb3} (a int unique key, b varchar(10));\"" \
		"clean_table" "optimistic"
}

function DM_094_CASE() {
	# here we run ddl to make sure we flush first check point in syncer
	# otherwise the worker may dump again when restart
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column col int;"
	run_sql_source1 "alter table ${shardddl1}.${tb2} add column col int;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column col int;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column col int;"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values(4,4);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 int;"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task test" \
		"\"result\": true" 3

	run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column new_col1 int;"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $cur/conf/double-source-$1.yaml" \
		"\"result\": true" 3

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_094() {
	run_case 094 "double-source-pessimistic" "init_table 111 112 211 212" "clean_table" "pessimistic"
	run_case 094 "double-source-optimistic" "init_table 111 112 211 212" "clean_table" "optimistic"
}

function DM_095_CASE() {
	# here we run ddl to make sure we flush first check point in syncer
	# otherwise the worker may dump again when restart
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column col int;"
	run_sql_source1 "alter table ${shardddl1}.${tb2} add column col int;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column col int;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column col int;"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values(4,4);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 int;"

	# do not check pause result because we may pause when auto resume in optimistic
	# then pause-task "result": true may not 3
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"pause-task test"

	run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column new_col1 int;"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-task test" \
		"\"result\": true" 3

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_095() {
	run_case 095 "double-source-pessimistic" "init_table 111 112 211 212" "clean_table" "pessimistic"
	run_case 095 "double-source-optimistic" "init_table 111 112 211 212" "clean_table" "optimistic"
}

function DM_096_CASE() {
	# here we run ddl to make sure we flush first check point in syncer
	# otherwise the worker may dump again when restart
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column col int;"
	run_sql_source1 "alter table ${shardddl1}.${tb2} add column col int;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column col int;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column col int;"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values(4,4);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 int;"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"pause-task test"

	run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column new_col1 int;"

	# TODO: uncomment after we support update-task
	# run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
	#     "update-task $cur/conf/double-source-$1.yaml" \
	#     "\"result\": true" 1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-task test" \
		"\"result\": true" 3

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_096() {
	run_case 096 "double-source-pessimistic" "init_table 111 112 211 212" "clean_table" "pessimistic"
	run_case 096 "double-source-optimistic" "init_table 111 112 211 212" "clean_table" "optimistic"
}

function DM_097_CASE() {
	# here we run ddl to make sure we flush first check point in syncer
	# otherwise the worker may dump again when restart
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column col int;"
	run_sql_source1 "alter table ${shardddl1}.${tb2} add column col int;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column col int;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column col int;"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values(4,4);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 int;"

	ps aux | grep dm-master | awk '{print $2}' | xargs kill || true
	check_port_offline $MASTER_PORT1 20

	run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column new_col1 int;"

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_097() {
	run_case 097 "double-source-pessimistic" "init_table 111 112 211 212" "clean_table" "pessimistic"
	run_case 097 "double-source-optimistic" "init_table 111 112 211 212" "clean_table" "optimistic"
}

function DM_098_CASE() {
	# here we run ddl to make sure we flush first check point in syncer
	# otherwise the worker may dump again when restart
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column col int;"
	run_sql_source1 "alter table ${shardddl1}.${tb2} add column col int;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column col int;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column col int;"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values(4,4);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"

	ps aux | grep dm-worker1 | awk '{print $2}' | xargs kill || true
	check_port_offline $WORKER1_PORT 20

	run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
	run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"

	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	run_sql_source2 "alter table ${shardddl1}.${tb2} add column new_col1 int;"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_098() {
	run_case 098 "double-source-pessimistic" "init_table 111 112 211 212" "clean_table" "pessimistic"
}

function DM_099_CASE() {
	# here we run ddl to make sure we flush first check point in syncer
	# otherwise the worker may dump again when restart
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column col int;"
	run_sql_source1 "alter table ${shardddl1}.${tb2} add column col int;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column col int;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column col int;"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values(4,4);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 int;"

	ps aux | grep dm-worker2 | awk '{print $2}' | xargs kill || true
	check_port_offline $WORKER2_PORT 20

	run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"

	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	run_sql_source2 "alter table ${shardddl1}.${tb2} add column new_col1 int;"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_099() {
	run_case 099 "double-source-pessimistic" "init_table 111 112 211 212" "clean_table" "pessimistic"
}

function DM_100_CASE() {
	# here we run ddl to make sure we flush first check point in syncer
	# otherwise the worker may dump again when restart
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column col int;"
	run_sql_source1 "alter table ${shardddl1}.${tb2} add column col int;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column col int;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column col int;"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values(4,4);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"

	ps aux | grep dm-worker1 | awk '{print $2}' | xargs kill || true
	check_port_offline $WORKER1_PORT 20

	run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
	run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"

	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	run_sql_source2 "alter table ${shardddl1}.${tb2} add column new_col1 int;"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_100() {
	run_case 100 "double-source-optimistic" "init_table 111 112 211 212" "clean_table" "optimistic"
}

function DM_101_CASE() {
	# here we run ddl to make sure we flush first check point in syncer
	# otherwise the worker may dump again when restart
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column col int;"
	run_sql_source1 "alter table ${shardddl1}.${tb2} add column col int;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column col int;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column col int;"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values(4,4);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 int;"

	ps aux | grep dm-worker2 | awk '{print $2}' | xargs kill || true
	check_port_offline $WORKER2_PORT 20

	run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"

	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	run_sql_source2 "alter table ${shardddl1}.${tb2} add column new_col1 int;"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_101() {
	run_case 101 "double-source-optimistic" "init_table 111 112 211 212" "clean_table" "optimistic"
}

function DM_102_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int default 0;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 int default -1;"

	sleep 1
	# wait DM receive source2's DDL
	found=false
	for ((k = 0; k < 10; k++)); do
		content=$($PWD/bin/dmctl.test DEVEL --master-addr=127.0.0.1:$MASTER_PORT query-status test)
		master2=$(echo $content | sed 's/"masterBinlog":/"masterBinlog":\n/g' | awk -F')' 'FNR==3{print $1}')
		syncer2=$(echo $content | sed 's/"syncerBinlog":/"syncerBinlog":\n/g' | awk -F')' 'FNR==3{print $1}')
		if [ "$master2" != "$syncer2" ]; then
			found=true
			break
		fi
	done
	if [[ $found == false ]]; then
		echo "didn't receive mismatched DDL"
		exit 2
	fi

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"show-ddl-locks" \
		"\"ID\": \"test-\`shardddl\`.\`tb\`\"" 1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"unlock-ddl-lock test-\`shardddl\`.\`tb\`" \
		"\"result\": true" 1

	run_sql_source2 "insert into ${shardddl1}.${tb1} values (2,2);"

	run_sql_tidb_with_retry "select count(1) from ${shardddl}.${tb};" "count(1): 2"
}

function DM_102() {
	run_case 102 "double-source-pessimistic" "init_table 111 211" "clean_table" ""
}

function DM_103_CASE() {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'ccc');"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column c double;"
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop column b;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column c double;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column c double;"
	run_sql_source1 "alter table ${shardddl1}.${tb1} change a a bigint default 10;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} drop column b;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} change a a bigint default 10;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} drop column b;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} change a a bigint default 10;"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_103() {
	run_case 103 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10));\"" \
		"clean_table" "pessimistic"
	run_case 103 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10));\"" \
		"clean_table" "optimistic"
}

function DM_104_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add new_col1 int not null default 10;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values(1);"
	run_sql_source1 "alter table ${shardddl1}.${tb1} add new_col2 int not null default 20;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values(2);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(3);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(4);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add new_col2 int not null default 20;"
	run_sql_source2 "insert into ${shardddl1}.${tb1} (id) values(5);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add new_col1 int not null default 10;"
	run_sql_source2 "insert into ${shardddl1}.${tb1} (id) values(6);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add new_col1 int not null default 10;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add new_col2 int not null default 20;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values(7);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values(8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} (id) values(9);"
	run_sql_tidb_with_retry "select count(1) from ${shardddl}.${tb};" "count(1): 9"
}

function DM_104() {
	# currently not support pessimistic
	# run_case 104 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
	run_case 104 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function add_drop_index_test() {
	action=$1
	col1=$2
	col2=$3
	run_sql_source1 "alter table ${shardddl1}.${tb1} ${action} index new_idx1${col1};"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,1);"
	run_sql_source1 "alter table ${shardddl1}.${tb1} ${action} index new_idx2${col2};"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(2,2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(3,3,3);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(4,4,4);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} ${action} index new_idx2${col2};"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,5);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} ${action} index new_idx1${col1};"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(6,6,6);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} ${action} index new_idx1${col1};"
	run_sql_source2 "alter table ${shardddl1}.${tb2} ${action} index new_idx2${col2};"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,7);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9,9);"
	run_sql_tidb_with_retry "select count(1) from ${shardddl}.${tb};" "count(1): 9"
}

function DM_105_CASE() {
	add_drop_index_test "add" "(b)" "(c)"
	run_sql_tidb_with_retry "select count(b) from ${shardddl}.${tb};" "count(b): 9"
	run_sql_tidb_with_retry "select count(c) from ${shardddl}.${tb};" "count(c): 9"
}

function DM_105() {
	# currently not support pessimistic
	# run_case 105 "double-source-pessimistic" \
	# "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int, c int);\"; \
	# run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int, c int);\"; \
	# run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int, c int);\"" \
	# "clean_table" "pessimistic"
	run_case 105 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int, c int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int, c int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int, c int);\"" \
		"clean_table" "optimistic"
}

function DM_106_CASE() {
	add_drop_index_test "drop" "" ""
}

function DM_106() {
	# currently not support pessimistic
	# run_case 106 "double-source-pessimistic" \
	# "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int, c int, index new_idx1(b), index new_idx2(c));\"; \
	# run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int, c int, index new_idx1(b), index new_idx2(c));\"; \
	# run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int, c int, index new_idx1(b), index new_idx2(c));\"" \
	# "clean_table" "pessimistic"
	run_case 106 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int, c int, index new_idx1(b), index new_idx2(c));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int, c int, index new_idx1(b), index new_idx2(c));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int, c int, index new_idx1(b), index new_idx2(c));\"" \
		"clean_table" "optimistic"
}

function DM_107_CASE() {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column col1 int not null"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values (2,2);"
	# TODO: check the handle-error message in the future
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"fail to handle shard ddl \[ALTER TABLE \`shardddl\`.\`tb\` ADD COLUMN \`col1\` INT NOT NULL\]" 1 \
		"because schema conflict detected" 1

	run_sql_source2 "insert into ${shardddl1}.${tb1} values(3);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column col1 int not null;"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values (4,4);"

	run_sql_source2 "insert into ${shardddl1}.${tb2} values(5);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column col1 int not null"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values (6,6);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 3 'fail'
}

function DM_107() {
	# FIXME: should be positive in the future
	# run_case 107 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
	run_case 107 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function different_field_flag_test() {
	type1=$1
	val1=$2
	type2=$3
	val2=$4
	type3=$5
	val3=$6
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column col1 $type1"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values (2,${val1});"

	run_sql_source2 "insert into ${shardddl1}.${tb1} values(3);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column col1 $type2"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values (4,${val2});"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"because schema conflict detected" 1

	run_sql_source2 "insert into ${shardddl1}.${tb2} values(5);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column col1 $type3"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values (6,${val3});"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 3 'fail'
}

function DM_108_CASE() {
	different_field_flag_test \
		"decimal(5,2)" "2" \
		"decimal(7,4)" "4" \
		"decimal(9,6)" "6"
}

function DM_108() {
	run_case 108 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_109_CASE() {
	different_field_flag_test \
		"varchar(3)" "'222'" \
		"varchar(4)" "'4444'" \
		"varchar(5)" "'66666'"
}

function DM_109() {
	run_case 109 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_110_CASE() {
	different_field_flag_test \
		"varchar(5)" "'22222'" \
		"varchar(4)" "'4444'" \
		"varchar(3)" "'666'"
}

function DM_110() {
	run_case 110 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_111_CASE() {
	different_field_flag_test \
		"int(11) zerofill" "2" \
		"int(11)" "4" \
		"int(11) zerofill" "'66666'"
}

function DM_111() {
	run_case 111 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_112_CASE() {
	different_field_flag_test \
		"int(11) unsigned" "2" \
		"int(11)" "4" \
		"int(11) unsigned" "'66666'"
}

function DM_112() {
	run_case 112 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_113_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column (b int, c int);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} add column (b int, c int);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9);"

	run_sql_source2 "alter table ${shardddl1}.${tb2} add column (b int, c int);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,12,12);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Add multiple fileds to a single table.
function DM_113 {
	run_case 113 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
	run_case 113 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_114_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop column b, drop column c;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6,6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} drop column b, drop column c;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9,9);"

	run_sql_source2 "alter table ${shardddl1}.${tb2} drop column b, drop column c;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Drop multiple fields from a single table.
function DM_114 {
	run_case 114 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int, c int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int, c int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int, c int);\"" \
		"clean_table" "pessimistic"
	run_case 114 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int, c int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int, c int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int, c int);\"" \
		"clean_table" "optimistic"
}

function DM_115_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column (b int, c int);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} add column (b int, c int);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} drop column b, drop column c;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop column b, drop column c;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(13);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(14);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(15);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Add multiple fileds and rollback.
function DM_115 {
	run_case 115 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_116_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column b int, drop column c;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} add column b int, drop column c;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9);"

	run_sql_source2 "alter table ${shardddl1}.${tb2} add column b int, drop column c;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,12);"

	# FIXME: add,drop,add a same column may cause data inconsistency.
	# For example:
	# table1: add column b(t1) -> drop column b(t3) -> add column b(t5)
	# table2: add column b(t2) -> drop column b(dm master update etcd t4, dm worker execute ddl t6)
	# timeline:
	# t1 < t2 < .. < t6
	# Under this condition, DM should pause the task and report an error.
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Add and Drop multiple fields at the same time.
function DM_116 {
	run_case 116 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c int);\"" \
		"clean_table" "pessimistic"
	run_case 116 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c int);\"" \
		"clean_table" "optimistic"
}

function DM_117_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop column b, drop column c;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6,6);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column (b int, c int);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9,9);"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"because schema conflict detected" 1 \
		"add column b that wasn't fully dropped in downstream" 1

	# try to fix data
	echo 'create table tb1(a int primary key, b int, c int) engine=innodb default charset=latin1;' >${WORK_DIR}/schema.sql
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-schema set test ${WORK_DIR}/schema.sql -s mysql-replica-01 -d ${shardddl1} -t ${tb1}" \
		"\"result\": true" 2

	# skip this error
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"handle-error test skip" \
		"\"result\": true" 2 \
		"\"source 'mysql-replica-02' has no error\"" 1

	run_sql_tidb "update ${shardddl}.${tb} set b=null, c=null where a=1;"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Drop multiple fields and rollback.
function DM_117 {
	run_case 117 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int, c int) engine=innodb default charset=latin1;\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int, c int) engine=innodb default charset=latin1;\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int, c int) engine=innodb default charset=latin1;\"" \
		"clean_table" "optimistic"
}

function DM_118_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2,2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3,3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop index idx, add index idx(c3,c1,c2);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6,6,6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} drop index idx, add index idx(c3,c1,c2);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9,9,9);"

	run_sql_source2 "alter table ${shardddl1}.${tb2} drop index idx, add index idx(c3,c1,c2);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10,10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,11,11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,12,12,12);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Adjust index fields combination.
function DM_118 {
	run_case 118 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, c3 int, index idx(c1, c2, c3));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, c3 int, index idx(c1, c2, c3));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int, c3 int, index idx(c1, c2, c3));\"" \
		"clean_table" "pessimistic"
	run_case 118 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, c3 int, index idx(c1, c2, c3));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, c3 int, index idx(c1, c2, c3));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int, c3 int, index idx(c1, c2, c3));\"" \
		"clean_table" "optimistic"
}

function run() {
	init_cluster
	init_database
	start=71
	end=118
	except=(072 074 075 083 084 087 088 089 090 091 092 093)
	for i in $(seq -f "%03g" ${start} ${end}); do
		if [[ ${except[@]} =~ $i ]]; then
			continue
		fi
		DM_${i}
		sleep 1
	done
}

cleanup_data $shardddl
cleanup_data $shardddl1
cleanup_data $shardddl2
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
