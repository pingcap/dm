#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
source $cur/../_utils/shardddl_lib.sh

function DM_119_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2,2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3,3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add index idx(c1,c2);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6,6,6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} add index idx(c1,c3);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9,9,9);"

	# FIXME: DM should detect conflicts and give human readable error messages.
	# For example:
	# if [[ "$1" = "pessimistic" ]]; then
	#     check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
	# else
	#     run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
	#         "query-status test" \
	#         "because schema conflict detected" 1
	# fi
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 10 "fail"
}

# Add index with the same name but with different fields.
function DM_119 {
	run_case 119 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, c3 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, c3 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int, c3 int);\"" \
		"clean_table" "pessimistic"
	run_case 119 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, c3 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, c3 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int, c3 int);\"" \
		"clean_table" "optimistic"
}

function DM_120_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add index idx1(c1), add index idx2(c2);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6,6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} add index idx1(c1), add index idx2(c2);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9,9);"

	run_sql_source2 "alter table ${shardddl1}.${tb2} add index idx1(c1), add index idx2(c2);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,12,12);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Add multiple indexes to a single table.
function DM_120 {
	run_case 120 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int);\"" \
		"clean_table" "pessimistic"
	run_case 120 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int);\"" \
		"clean_table" "optimistic"
}

function DM_121_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add index idx(c1);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6,6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} add index idx(c1,c2);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9,9);"

	# FIXME: DM should detect conflicts and give human readable error messages.
	# For example:
	# if [[ "$1" = "pessimistic" ]]; then
	#     check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
	# else
	#     run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
	#         "query-status test" \
	#         "because schema conflict detected" 1
	# fi
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 10 "fail"
}

# Add index with the same name but with different fields.
function DM_121 {
	run_case 121 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int);\"" \
		"clean_table" "pessimistic"
	run_case 121 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int);\"" \
		"clean_table" "optimistic"
}

function DM_122_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop index idx1, drop index idx2;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6,6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} drop index idx1, drop index idx2;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9,9);"

	run_sql_source2 "alter table ${shardddl1}.${tb2} drop index idx1, drop index idx2;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,12,12);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Drop multiple indexes from a single table.
function DM_122 {
	run_case 122 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, index idx1(c1), index idx2(c2));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, index idx1(c1), index idx2(c2));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int, index idx1(c1), index idx2(c2));\"" \
		"clean_table" "pessimistic"
	run_case 122 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, index idx1(c1), index idx2(c2));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, index idx1(c1), index idx2(c2));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int, index idx1(c1), index idx2(c2));\"" \
		"clean_table" "optimistic"
}

function DM_123_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,1,1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2,2,2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3,3,3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop index idx1, add index idx1(c2,c1), drop index idx2, add index idx2(c4,c3);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,4,4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,5,5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6,6,6,6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} drop index idx1, add index idx1(c2,c1), drop index idx2, add index idx2(c4,c3);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,7,7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9,9,9,9);"

	run_sql_source2 "alter table ${shardddl1}.${tb2} drop index idx1, add index idx1(c2,c1), drop index idx2, add index idx2(c4,c3);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10,10,10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,11,11,11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,12,12,12,12);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Adjust multiple indexes combination.
function DM_123 {
	run_case 123 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, c3 int, c4 int, index idx1(c1, c2), index idx2(c3, c4));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, c3 int, c4 int, index idx1(c1, c2), index idx2(c3, c4));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int, c3 int, c4 int, index idx1(c1, c2), index idx2(c3, c4));\"" \
		"clean_table" "pessimistic"
	run_case 123 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, c3 int, c4 int, index idx1(c1, c2), index idx2(c3, c4));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, c3 int, c4 int, index idx1(c1, c2), index idx2(c3, c4));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int, c3 int, c4 int, index idx1(c1, c2), index idx2(c3, c4));\"" \
		"clean_table" "optimistic"
}

function DM_124_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add index idx1(c1), add index idx2(c2);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6,6);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop index idx1, drop index idx2;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9,9);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Add multiple indexes and then rollback.
function DM_124 {
	# run_case 124 "double-source-pessimistic" \
	# "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int);\"; \
	#  run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int);\"; \
	#  run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int);\"" \
	# "clean_table" "pessimistic"
	run_case 124 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int);\"" \
		"clean_table" "optimistic"
}

function DM_125_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop index idx1, drop index idx2;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6,6);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add index idx1(c1), add index idx2(c2);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9,9);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	# FIXME: dm-master should remove this lock after all shards are synced.
	run_dm_ctl_with_rematch $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"show-ddl-locks" \
		"\"unsynced\": \[[\n ]*\]" 1
}

# Drop multiple indexes and then rollback.
function DM_125 {
	# run_case 125 "double-source-pessimistic" \
	# "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, index idx1(c1), index idx2(c2));\"; \
	#  run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, index idx1(c1), index idx2(c2));\"; \
	#  run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int, index idx1(c1), index idx2(c2));\"" \
	# "clean_table" "pessimistic"
	run_case 125 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, index idx1(c1), index idx2(c2));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, index idx1(c1), index idx2(c2));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int, index idx1(c1), index idx2(c2));\"" \
		"clean_table" "optimistic"
}

function DM_126_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,1,1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2,2,2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3,3,3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop index idx1, add index idx1(c2,c1), drop index idx2, add index idx2(c4,c3);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,4,4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,5,5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6,6,6,6);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop index idx1, add index idx1(c1,c2), drop index idx2, add index idx2(c3,c4);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,7,7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9,9,9,9);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	# FIXME: dm-master should remove this lock after all shards are synced.
	run_dm_ctl_with_rematch $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"show-ddl-locks" \
		"\"unsynced\": \[[\n ]*\]" 1
}

# Ajust multiple indexes combination and then rollback.
function DM_126 {
	# run_case 126 "double-source-pessimistic" \
	# "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, c3 int, c4 int, index idx1(c1, c2), index idx2(c3, c4));\"; \
	#  run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, c3 int, c4 int, index idx1(c1, c2), index idx2(c3, c4));\"; \
	#  run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int, c3 int, c4 int, index idx1(c1, c2), index idx2(c3, c4));\"" \
	# "clean_table" "pessimistic"
	run_case 126 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, c3 int, c4 int, index idx1(c1, c2), index idx2(c3, c4));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, c3 int, c4 int, index idx1(c1, c2), index idx2(c3, c4));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int, c3 int, c4 int, index idx1(c1, c2), index idx2(c3, c4));\"" \
		"clean_table" "optimistic"
}

function DM_127_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add index idx2(c2), drop index idx1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6,6);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add index idx1(c1), drop index idx2;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9,9);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	# FIXME: dm-master should remove this lock after all shards are synced.
	run_dm_ctl_with_rematch $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"show-ddl-locks" \
		"\"unsynced\": \[[\n ]*\]" 1
}

# Add and drop index at the same time and then rollback.
function DM_127 {
	# run_case 127 "double-source-pessimistic" \
	# "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, index idx1(c1));\"; \
	#  run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, index idx1(c1));\"; \
	#  run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int, index idx1(c1));\"" \
	# "clean_table" "pessimistic"
	run_case 127 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, index idx1(c1));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, index idx1(c1));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int, index idx1(c1));\"" \
		"clean_table" "optimistic"
}

function DM_128_CASE() {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} modify b int not null;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} modify b int not null;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9);"

	run_sql_source2 "alter table ${shardddl1}.${tb2} modify b int not null;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,12);"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Change NULL to NOT NULL.
function DM_128() {
	run_case 128 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int);\"" \
		"clean_table" "pessimistic"
	run_case 128 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int);\"" \
		"clean_table" "optimistic"
}

function DM_129_CASE() {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} modify b int default 10;"
	run_sql_source1 "insert into ${shardddl1}.${tb1}(a) values(4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} modify b int default 10;"
	run_sql_source1 "insert into ${shardddl1}.${tb1}(a) values(7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1}(a) values(8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9);"

	run_sql_source2 "alter table ${shardddl1}.${tb2} modify b int default 10;"
	run_sql_source1 "insert into ${shardddl1}.${tb1}(a) values(10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1}(a) values(11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2}(a) values(12);"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Change NOT NULL to NULL with the same default value.
function DM_129() {
	run_case 129 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int not null);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int not null);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int not null);\"" \
		"clean_table" "pessimistic"
	run_case 129 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int not null);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int not null);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int not null);\"" \
		"clean_table" "optimistic"
}

function DM_130_CASE() {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} modify b int default 0;"
	run_sql_source1 "insert into ${shardddl1}.${tb1}(a) values(4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} modify b int default -1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1}(a) values(7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1}(a) values(8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9);"

	run_sql_source2 "alter table ${shardddl1}.${tb2} modify b int default -1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1}(a) values(10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1}(a) values(11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2}(a) values(12);"
	if [[ "$1" = "pessimistic" ]]; then
		check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
	else
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"because schema conflict detected" 1
	fi
}

# Change NOT NULL to NULL with the different default value.
function DM_130() {
	run_case 130 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int not null);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int not null);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int not null);\"" \
		"clean_table" "pessimistic"
	run_case 130 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int not null);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int not null);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int not null);\"" \
		"clean_table" "optimistic"
}

function DM_131_CASE() {
	# Test rollback NULL to NOT NULL.
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} modify b int not null;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} modify b int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# Test rollback NOT NULL to NULL
	run_sql_source1 "alter table ${shardddl1}.${tb1} modify b int not null;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} modify b int not null;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} modify b int not null;"

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,12);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} modify b int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(13,13);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(14,14);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(15,15);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} modify b int not null;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(16,16);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(17,17);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(18,18);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Modify nullable and then rollback.
function DM_131 {
	# run_case 131 "double-source-pessimistic" \
	# "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
	#  run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
	#  run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int);\"" \
	# "clean_table" "pessimistic"
	run_case 131 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int);\"" \
		"clean_table" "optimistic"
}

function DM_132_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop primary key, add primary key(a, b);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} drop primary key, add primary key(a, b);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9);"

	run_sql_source2 "alter table ${shardddl1}.${tb2} drop primary key, add primary key(a, b);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,12);"

	# FIXME: DM should report an error to user that data constraints become smaller and may not be able to rollback.
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Expand the primary key field.
function DM_132 {
	run_sql_tidb "create database if not exists ${shardddl}; create table ${shardddl}.${tb} (a int, b int, primary key(a) nonclustered);"
	run_case 132 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int);\"" \
		"clean_table" "pessimistic"
	run_sql_tidb "create database if not exists ${shardddl}; create table ${shardddl}.${tb} (a int, b int, primary key(a) nonclustered);"
	run_case 132 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int);\"" \
		"clean_table" "optimistic"
}

function DM_133_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop primary key, add primary key(a);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} drop primary key, add primary key(a);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9);"

	run_sql_source2 "alter table ${shardddl1}.${tb2} drop primary key, add primary key(a);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,12);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Shrink the primary key field.
function DM_133 {
	run_sql_tidb "create database if not exists ${shardddl}; create table ${shardddl}.${tb} (a int, b int, primary key(a,b) nonclustered);"
	run_case 133 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int, b int, primary key(a,b));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (a int, b int, primary key(a,b));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (a int, b int, primary key(a,b));\"" \
		"clean_table" "pessimistic"
	run_sql_tidb "create database if not exists ${shardddl}; create table ${shardddl}.${tb} (a int, b int, primary key(a,b) nonclustered);"
	run_case 133 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int, b int, primary key(a,b));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (a int, b int, primary key(a,b));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (a int, b int, primary key(a,b));\"" \
		"clean_table" "optimistic"
}

function DM_134_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop primary key, add primary key(b);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} drop primary key, add primary key(b);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9);"

	run_sql_source2 "alter table ${shardddl1}.${tb2} drop primary key, add primary key(b);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,12);"

	# FIXME: dm-master should give warnings to users that constraint is changed.
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Change the primary key field.
function DM_134 {
	run_sql_tidb "create database if not exists ${shardddl}; create table ${shardddl}.${tb} (a int, b int, primary key(a) nonclustered);"
	run_case 134 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int, b int, primary key(a));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (a int, b int, primary key(a));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (a int, b int, primary key(a));\"" \
		"clean_table" "pessimistic"
	run_sql_tidb "create database if not exists ${shardddl}; create table ${shardddl}.${tb} (a int, b int, primary key(a) nonclustered);"
	run_case 134 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int, b int, primary key(a));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (a int, b int, primary key(a));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (a int, b int, primary key(a));\"" \
		"clean_table" "optimistic"
}

function DM_135_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"

	if ! run_sql_source1 "alter table ${shardddl1}.${tb1} drop primary key, add primary key(b);" 2>&1 |
		grep "Incorrect table definition; there can be only one auto column and it must be defined as a key" >/dev/null; then
		echo "sql should be failed because there can be only one auto column and it must be defined as a key" >&2
		return 255
	fi
}

function DM_135() {
	run_sql_tidb "create database if not exists ${shardddl}; create table ${shardddl}.${tb} (a int auto_increment, b int, primary key(a) nonclustered);"
	run_case 135 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int auto_increment primary key, b int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (a int auto_increment primary key, b int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (a int auto_increment primary key, b int);\"" \
		"clean_table" "pessimistic"
	run_sql_tidb "create database if not exists ${shardddl}; create table ${shardddl}.${tb} (a int auto_increment, b int, primary key(a) nonclustered);"
	run_case 135 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int auto_increment primary key, b int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (a int auto_increment primary key, b int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (a int auto_increment primary key, b int);\"" \
		"clean_table" "optimistic"
}

function DM_136_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop index uk, add unique key uk(a, b);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6,6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} drop index uk, add unique key uk(a, b);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9,9);"

	run_sql_source2 "alter table ${shardddl1}.${tb2} drop index uk, add unique key uk(a, b);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,12,12);"

	# FIXME: DM should report an error to user that data constraints become smaller and may not be able to rollback.
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Expand the unique key field.
function DM_136 {
	# run_case 136 "double-source-pessimistic" \
	#     "run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a int, b int, unique key uk(a));\"; \
	#      run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a int, b int, unique key uk(a));\"; \
	#      run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a int, b int, unique key uk(a));\"" \
	#     "clean_table" "pessimistic"

	run_case 136 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a int, b int, unique key uk(a));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a int, b int, unique key uk(a));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a int, b int, unique key uk(a));\"" \
		"clean_table" "optimistic"
}

function DM_137_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop index uk, add unique key uk(a);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6,6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} drop index uk, add unique key uk(a);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9,9);"

	run_sql_source2 "alter table ${shardddl1}.${tb2} drop index uk, add unique key uk(a);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,12,12);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Shrink the unique key field.
function DM_137 {
	# run_case 137 "double-source-pessimistic" \
	#     "run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a int, b int, unique key uk(a, b));\"; \
	#      run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a int, b int, unique key uk(a, b));\"; \
	#      run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a int, b int, unique key uk(a, b));\"" \
	#     "clean_table" "pessimistic"

	run_case 137 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a int, b int, unique key uk(a, b));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a int, b int, unique key uk(a, b));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a int, b int, unique key uk(a, b));\"" \
		"clean_table" "optimistic"
}

function DM_138_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add unique key uk(a);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} add unique key uk(a);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9);"

	run_sql_source2 "alter table ${shardddl1}.${tb2} add unique key uk(a);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,12);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Add the unique key.
function DM_138 {
	run_case 138 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a int);\"" \
		"clean_table" "pessimistic"

	run_case 138 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a int);\"" \
		"clean_table" "optimistic"
}

function DM_139_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop index uk;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} drop index uk;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9);"

	run_sql_source2 "alter table ${shardddl1}.${tb2} drop index uk;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,12);"

	# FIXME: DM should report an error to user that this operation may not be able to rollback.
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Drop the unique key.
function DM_139 {
	run_case 139 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a int, unique key uk(a));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a int, unique key uk(a));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a int, unique key uk(a));\"" \
		"clean_table" "pessimistic"

	run_case 139 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a int, unique key uk(a));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a int, unique key uk(a));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a int, unique key uk(a));\"" \
		"clean_table" "optimistic"
}

function DM_140_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10),(11),(12),(13),(14),(15);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(20),(21),(22),(23),(24),(25);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(30),(31),(32),(33),(34),(35);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} partition by range(id)(partition p0 values less than (106));"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"ALTER TABLE \`${shardddl1}\`.\`${tb1}\` PARTITION BY RANGE (\`id\`) (PARTITION \`p0\` VALUES LESS THAN (106))" 1 \
		"alter table partition is unsupported" 1
}

# Add partitioning
function DM_140 {
	run_case 140 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
	run_case 140 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_141_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10),(11),(12),(13),(14),(15);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(20),(21),(22),(23),(24),(25);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(30),(31),(32),(33),(34),(35);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} remove partitioning"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"ALTER TABLE \`${shardddl1}\`.\`${tb1}\` REMOVE PARTITIONING" 1 \
		"Unsupported remove partitioning" 1
}

# Remove partitioning.
function DM_141 {
	run_case 141 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key) partition by range(id)(partition p0 values less than (100));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key) partition by range(id)(partition p0 values less than (100));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key) partition by range(id)(partition p0 values less than (100));\"" \
		"clean_table" "pessimistic"
	run_case 141 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key) partition by range(id)(partition p0 values less than (100));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key) partition by range(id)(partition p0 values less than (100));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key) partition by range(id)(partition p0 values less than (100));\"" \
		"clean_table" "optimistic"
}

function DM_142_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10),(11),(12),(13),(14),(15);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(20),(21),(22),(23),(24),(25);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(30),(31),(32),(33),(34),(35);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add partition (partition p1 values less than (200));"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(110),(111),(112),(113),(114),(115);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add partition (partition p1 values less than (200));"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(120),(121),(122),(123),(124),(125);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add partition (partition p1 values less than (200));"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(130),(131),(132),(133),(134),(135);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Add new partition.
function DM_142 {
	run_case 142 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key) partition by range(id)(partition p0 values less than (100));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key) partition by range(id)(partition p0 values less than (100));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key) partition by range(id)(partition p0 values less than (100));\"" \
		"clean_table" "pessimistic"

	run_case 142 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key) partition by range(id)(partition p0 values less than (100));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key) partition by range(id)(partition p0 values less than (100));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key) partition by range(id)(partition p0 values less than (100));\"" \
		"clean_table" "optimistic"
}

function DM_143_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10),(11),(12),(13),(14),(15),(110),(111),(112),(113),(114),(115);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(20),(21),(22),(23),(24),(25),(120),(121),(122),(123),(124),(125);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(30),(31),(32),(33),(34),(35),(130),(131),(132),(133),(134),(135);"

	run_sql_source1 "delete from ${shardddl1}.${tb1} where id >= 100;"
	run_sql_source2 "delete from ${shardddl1}.${tb1} where id >= 100;"
	run_sql_source2 "delete from ${shardddl1}.${tb2} where id >= 100;"
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop partition p1;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} drop partition p1;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} drop partition p1;"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Remove partition.
function DM_143 {
	run_case 143 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key) partition by range(id) \
        (partition p0 values less than (100), partition p1 values less than (200));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key) partition by range(id) \
         (partition p0 values less than (100), partition p1 values less than (200));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key) partition by range(id) \
         (partition p0 values less than (100), partition p1 values less than (200));\"" \
		"clean_table" "pessimistic"
	run_case 143 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key) partition by range(id) \
        (partition p0 values less than (100), partition p1 values less than (200));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key) partition by range(id) \
         (partition p0 values less than (100), partition p1 values less than (200));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key) partition by range(id) \
         (partition p0 values less than (100), partition p1 values less than (200));\"" \
		"clean_table" "optimistic"
}

function DM_144_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10),(11),(12),(13),(14),(15),(110),(111),(112),(113),(114),(115);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(20),(21),(22),(23),(24),(25),(120),(121),(122),(123),(124),(125);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(30),(31),(32),(33),(34),(35),(130),(131),(132),(133),(134),(135);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} reorganize partition p0,p1 into (partition p0 values less than (200))"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"ALTER TABLE \`${shardddl1}\`.\`${tb1}\` REORGANIZE PARTITION \`p0\`,\`p1\` INTO (PARTITION \`p0\` VALUES LESS THAN (200))" 1 \
		"Unsupported reorganize partition" 1
}

# Reorganize partition.
function DM_144 {
	run_case 144 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key) partition by range(id) \
        (partition p0 values less than (100), partition p1 values less than (200));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key) partition by range(id) \
         (partition p0 values less than (100), partition p1 values less than (200));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key) partition by range(id) \
         (partition p0 values less than (100), partition p1 values less than (200));\"" \
		"clean_table" "pessimistic"
	run_case 144 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key) partition by range(id) \
        (partition p0 values less than (100), partition p1 values less than (200));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key) partition by range(id) \
         (partition p0 values less than (100), partition p1 values less than (200));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key) partition by range(id) \
         (partition p0 values less than (100), partition p1 values less than (200));\"" \
		"clean_table" "optimistic"
}

function DM_145_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(100),(101),(102),(103),(104),(105);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(200),(201),(202),(203),(204),(205);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(300),(301),(302),(303),(304),(305);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} engine=innodb;"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"This type of ALTER TABLE is currently unsupported" 1
}

# Defragment.
function DM_145 {
	run_case 145 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
	run_case 145 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_146_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(100),(101),(102),(103),(104),(105);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(200),(201),(202),(203),(204),(205);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(300),(301),(302),(303),(304),(305);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} ROW_FORMAT=COMPACT;"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"This type of ALTER TABLE is currently unsupported" 1
}

# Modify row format.
function DM_146 {
	run_case 146 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
	run_case 146 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_147_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column b int, drop column c;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column c int, drop column b;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9);"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"because schema conflict detected" 1 \
		"add column c that wasn't fully dropped in downstream" 1

	# try to fix data
	echo 'create table tbl(a int primary key, b int, c int) engine=innodb default charset=latin1;' >${WORK_DIR}/schema.sql
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-schema set test ${WORK_DIR}/schema.sql -s mysql-replica-01 -d ${shardddl1} -t ${tb1}" \
		"\"result\": true" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"handle-error test replace \"alter table ${shardddl1}.${tb1} drop column b\"" \
		"\"result\": true" 2 \
		"\"source 'mysql-replica-02' has no error\"" 1

	run_sql_tidb "update ${shardddl}.${tb} set c=null where a=1;"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Add and Drop multiple fields and then rollback.
function DM_147 {
	run_case 147 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c int) engine=innodb default charset=latin1;\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c int) engine=innodb default charset=latin1;\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c int) engine=innodb default charset=latin1;\"" \
		"clean_table" "optimistic"
}

function DM_148_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column b int after id, add column c int after b;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} add column b int after id, add column c int after b;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9);"

	run_sql_source2 "alter table ${shardddl1}.${tb2} add column b int after id, add column c int after b;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10,10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,11,11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,12,12,12);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Add multiple fields in a specific order.
function DM_148 {
	run_case 148 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a int);\"" \
		"clean_table" "pessimistic"
	run_case 148 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a int);\"" \
		"clean_table" "optimistic"
}

function DM_149_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,\"aaaaaaa\");"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,\"bbbbbbb\");"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,\"ccccccc\");"

	run_sql_source1 "alter table ${shardddl1}.${tb1} modify column a varchar(20);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,\"aaaaaaaaaaaaaa\");"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,\"bbbbbbb\");"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,\"ccccccc\");"

	run_sql_source2 "alter table ${shardddl1}.${tb1} modify column a varchar(20);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,\"aaaaaaaaaaaaaa\");"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,\"bbbbbbbbbbbbbb\");"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,\"ccccccc\");"

	run_sql_source2 "alter table ${shardddl1}.${tb2} modify column a varchar(20);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,\"aaaaaaaaaaaaaa\");"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,\"bbbbbbbbbbbbbb\");"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,\"cccccccccccccc\");"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Increase field length.
function DM_149 {
	run_case 149 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a varchar(10));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a varchar(10));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a varchar(10));\"" \
		"clean_table" "pessimistic"
	run_case 149 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a varchar(10));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a varchar(10));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a varchar(10));\"" \
		"clean_table" "optimistic"
}

function DM_150_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,\"aaaaaaa\");"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,\"bbbbbbb\");"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,\"ccccccc\");"

	run_sql_source1 "alter table ${shardddl1}.${tb1} modify column a varchar(10);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,\"aaaaaaa\");"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,\"bbbbbbb\");"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,\"ccccccc\");"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unsupported modify column: length 10 is less than origin 20" 1
}

# Increase field length.
function DM_150 {
	run_case 150 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a varchar(20));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a varchar(20));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a varchar(20));\"" \
		"clean_table" "pessimistic"
	run_case 150 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a varchar(20));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a varchar(20));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a varchar(20));\"" \
		"clean_table" "optimistic"
}

function DM_151_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} modify column a double;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4.0);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6);"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unsupported modify column: type double not match origin int(11), and tidb_enable_change_column_type is false" 1
}

function DM_151 {
	run_case 151 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a int);\"" \
		"clean_table" "pessimistic"
	run_case 151 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a int);\"" \
		"clean_table" "optimistic"
}

function DM_152_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

	# Add multiple fields.
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column a int, add column b varchar(20), add column c double;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column a int, add column b varchar(20), add column c double;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column a int, add column b varchar(20), add column c double;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,\"aaaa\",4.0);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,\"bbbb\",5.0);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6,\"cccc\",6.0);"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# Add multiple indexes.
	run_sql_source1 "alter table ${shardddl1}.${tb1} add unique index uni_a(a), add index idx_b(b);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add unique index uni_a(a), add index idx_b(b);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add unique index uni_a(a), add index idx_b(b);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,\"aaaa\",7.0);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,\"bbbb\",8.0);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9,\"cccc\",9.0);"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# Add and drop indexes.
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop index idx_b, add index idx_c(c);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} drop index idx_b, add index idx_c(c);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} drop index idx_b, add index idx_c(c);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10,\"aaaa\",10.0);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,11,\"bbbb\",11.0);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,12,\"cccc\",12.0);"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# Add and drop fields.
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop column b, add column d int;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} drop column b, add column d int;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} drop column b, add column d int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(13,13,13.0,13);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(14,14,14.0,14);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(15,15,15.0,15);"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# Drop all indexes.
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop index uni_a, drop index idx_c;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} drop index uni_a, drop index idx_c;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} drop index uni_a, drop index idx_c;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(16,16,16.0,16);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(17,17,17.0,17);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(18,18,18.0,18);"

	# Drop all fields.
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop column a, drop column c, drop column d;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} drop column a, drop column c, drop column d;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} drop column a, drop column c, drop column d;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(19);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(20);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(21);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_152 {
	run_case 152 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function run() {
	init_cluster
	init_database
	start=119
	end=152
	for i in $(seq -f "%03g" ${start} ${end}); do
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
