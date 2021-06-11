#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
source $cur/../_utils/shardddl_lib.sh

function DM_036_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int first;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,'aaa');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'ccc');"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 int after a;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,'ddd');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,'eee');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,'fff');"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column new_col1 int after b;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,'ggg');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,'hhh');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,'iii',9);"
	run_sql_tidb_with_retry "select count(1) from ${shardddl}.${tb};" "count(1): 9"
}

function DM_036() {
	# currently not support pessimistic
	# run_case 036 "double-source-pessimistic" \
	# "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
	#  run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
	#  run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10));\";" \
	# "clean_table" "pessimistic"

	run_case 036 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10));\";" \
		"clean_table" "optimistic"
}

function DM_037_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add new_col1 int default 0;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add new_col1 int default -1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(3,3);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add new_col1 int default -1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(5,5);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(6,6);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(7,7);"
	if [[ "$1" = "pessimistic" ]]; then
		check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
	else
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"because schema conflict detected" 1
	fi
}

function DM_037() {
	run_case 037 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
	run_case 037 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_038_CASE() {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

	# TODO: remove sleep after we support detect ASAP in optimistic mode
	sleep 1
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column col1 datetime default now();"
	run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values (4);"
	sleep 1
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column col1 datetime default now();"
	run_sql_source2 "insert into ${shardddl1}.${tb1} (id) values (5);"
	sleep 1
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column col1 datetime default now();"
	run_sql_source2 "insert into ${shardddl1}.${tb2} (id) values (6);"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 3 'fail'
}

function DM_038() {
	run_case 038 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
	run_case 038 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_039_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column col1 varchar(10) character set utf8 collate utf8_bin;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column col1 varchar(10) character set utf8 collate utf8_bin;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,'bbb');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,'ccc');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column col1 varchar(10) character set utf8 collate utf8_bin;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,'ddd');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,'eee');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,'fff');"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_039() {
	run_case 039 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
	run_case 039 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_040_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column col1 varchar(10) character set utf8 collate utf8_bin;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column col1 varchar(10) character set utf8mb4 collate utf8mb4_bin;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,'bbb');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,'ccc');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column col1 varchar(10) character set utf8mb4 collate utf8mb4_bin;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,'ddd');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,'eee');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,'fff');"
	if [[ "$1" = "pessimistic" ]]; then
		check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
	else
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"because schema conflict detected" 1
	fi
}

function DM_040() {
	run_case 040 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
	run_case 040 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_041_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int as (id+1);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values(1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} (id) values(2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} (id) values(3);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 int as (id+1);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values(4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} (id) values(5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} (id) values(6);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column new_col1 int as (id+1);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values(7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} (id) values(8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} (id) values(9);"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_041() {
	run_case 041 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
	run_case 041 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_043_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int as (id+1);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values(1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} (id) values(2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} (id) values(3);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 int as (id+2);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values(4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} (id) values(5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} (id) values(6);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column new_col1 int as (id+2);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values(7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} (id) values(8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} (id) values(9);"
	if [[ "$1" = "pessimistic" ]]; then
		check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
	else
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"because schema conflict detected" 1
	fi
}

function DM_043() {
	run_case 043 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
	run_case 043 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_ADD_DROP_COLUMNS_CASE {
	# add cols
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column col1 int, add column col2 int, add column col3 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,now(),1,1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,now());"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,now());"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column col1 int, add column col2 int, add column col3 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,now(),4,4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,now(),5,5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,now());"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column col1 int, add column col2 int, add column col3 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,now(),7,7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,now(),8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,now(),9,9,9);"

	# drop cols
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop column col1, drop column col2;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(11,now(),11);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(12,now(),12,12,12);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(13,now(),13,13,13);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} drop column col1, drop column col2;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(14,now(),14);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(15,now(),15);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(16,now(),16,16,16);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} drop column col1, drop column col2;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(17,now(),17);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(18,now(),18);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(19,now(),19);"

	# add and drop
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column col4 int, drop column col3;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(21,now(),21);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(22,now(),22);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(23,now(),23);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column col4 int, drop column col3;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(24,now(),24);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(25,now(),25);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(26,now(),26);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column col4 int, drop column col3;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(27,now(),27);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(28,now(),28);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(29,now(),29);"

	# drop and add
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop column col4, add column col5 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(31,now(),31);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(32,now(),32);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(33,now(),33);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} drop column col4, add column col5 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(34,now(),34);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(35,now(),35);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(36,now(),36);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} drop column col4, add column col5 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(37,now(),37);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(38,now(),38);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(39,now(),39);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_ADD_DROP_COLUMNS() {
	run_case ADD_DROP_COLUMNS "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, id datetime);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, id datetime);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, id datetime);\"" \
		"clean_table" "pessimistic"
	run_case ADD_DROP_COLUMNS "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, id datetime);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, id datetime);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, id datetime);\"" \
		"clean_table" "optimistic"
}

function DM_COLUMN_INDEX_CASE {
	# add col and index
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column col3 int, add index idx_col1(col1);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3,3);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column col3 int, add index idx_col1(col1);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6,6);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column col3 int, add index idx_col1(col1);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9,9,9);"

	# drop col and index
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop column col2, drop index idx_col1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(11,11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(12,12,12,12);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(13,13,13,13);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} drop column col2, drop index idx_col1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(14,14,14);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(15,15,15);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(16,16,16,16);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} drop column col2, drop index idx_col1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(17,17,17);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(18,18,18);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(19,19,19);"

	# drop col, add index
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop column col1, add index idx_col3(col3);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(21,21);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(22,22,22);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(23,23,23);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} drop column col1, add index idx_col3(col3);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(24,24);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(25,25);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(26,26,26);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} drop column col1, add index idx_col3(col3);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(27,27);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(28,28);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(29,29);"

	# add col, drop index
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column col4 int, drop index idx_col3;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(31,31,31);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(32,32);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(33,33);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column col4 int, drop index idx_col3;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(34,34,34);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(35,35,35);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(36,36);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column col4 int, drop index idx_col3;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(37,37,37);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(38,38,38);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(39,39,39);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_COLUMN_INDEX() {
	run_case COLUMN_INDEX "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, col1 int, col2 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, col1 int, col2 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, col1 int, col2 int);\"" \
		"clean_table" "pessimistic"
	run_case COLUMN_INDEX "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, col1 int, col2 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, col1 int, col2 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, col1 int, col2 int);\"" \
		"clean_table" "optimistic"
}

function DM_INIT_SCHEMA_CASE() {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6);"
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop new_col1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9);"

	check_log_contain_with_retry 'finish to handle ddls in optimistic shard mode.*DROP COLUMN' \
		$WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log

	restart_master

	run_sql_source1 "alter table ${shardddl1}.${tb1} add new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12);"
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop new_col1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(13);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(14);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(15);"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 3
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_INIT_SCHEMA() {
	run_case INIT_SCHEMA "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function restart_worker() {
	echo "restart dm-worker" $1
	if [[ "$1" = "1" ]]; then
		ps aux | grep dm-worker1 | awk '{print $2}' | xargs kill || true
		check_port_offline $WORKER1_PORT 20
	else
		ps aux | grep dm-worker2 | awk '{print $2}' | xargs kill || true
		check_port_offline $WORKER2_PORT 20
	fi
	export GO_FAILPOINTS=$2

	if [[ "$1" = "1" ]]; then
		run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
		check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	else
		run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
		check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	fi
}

function DM_DROP_COLUMN_EXEC_ERROR_CASE() {
	# get worker of source1
	w="1"
	got=$(grep "mysql-replica-01" $WORK_DIR/worker1/log/dm-worker.log | wc -l)
	if [[ "$got" -eq 0 ]]; then
		w="2"
	fi

	restart_worker $w "github.com/pingcap/dm/syncer/ExecDDLError=return()"

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'ccc');"

	run_sql_source2 "alter table ${shardddl1}.${tb1} drop column b;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} drop column b;"

	check_log_contain_with_retry 'finish to handle ddls in optimistic shard mode.*tb1 drop column' \
		$WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log
	check_log_contain_with_retry 'finish to handle ddls in optimistic shard mode.*tb2 drop column' \
		$WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop column b;"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"execute .* error" 1

	restart_master

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} add column b varchar(10);"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"because schema conflict detected" 1 \
		"add column b that wasn't fully dropped in downstream" 1

	restart_worker $w ""
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column b varchar(10);"
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column b varchar(10);"

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,'ddd');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,'eee');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,'fff');"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 3
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_DROP_COLUMN_EXEC_ERROR() {
	run_case DROP_COLUMN_EXEC_ERROR "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10));\"" \
		"clean_table" "optimistic"
}

function DM_DROP_COLUMN_ALL_DONE_CASE() {
	# get worker of source1
	w="1"
	got=$(grep "mysql-replica-01" $WORK_DIR/worker1/log/dm-worker.log | wc -l)
	if [[ "$got" -eq 0 ]]; then
		w="2"
	fi

	restart_worker $w "github.com/pingcap/dm/syncer/ExecDDLError=return()"

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'ccc');"

	run_sql_source2 "alter table ${shardddl1}.${tb1} drop column b;"
	check_log_contain_with_retry 'finish to handle ddls in optimistic shard mode.*tb1 drop column' \
		$WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop column b;"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"execute .* error" 1

	run_sql_source2 "alter table ${shardddl1}.${tb2} drop column b;"
	check_log_contain_with_retry 'finish to handle ddls in optimistic shard mode.*tb2 drop column' \
		$WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log

	restart_master

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} add column b varchar(10);"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"because schema conflict detected" 1 \
		"add column b that wasn't fully dropped in downstream" 1

	restart_worker $w ""
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column b varchar(10);"
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column b varchar(10);"

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,'ddd');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,'eee');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,'fff');"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 3
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_DROP_COLUMN_ALL_DONE() {
	run_case DROP_COLUMN_ALL_DONE "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10));\"" \
		"clean_table" "optimistic"
}

function run() {
	init_cluster
	init_database
	DM_DROP_COLUMN_EXEC_ERROR
	DM_ADD_DROP_COLUMNS
	DM_COLUMN_INDEX
	DM_INIT_SCHEMA
	DM_DROP_COLUMN_ALL_DONE
	start=36
	end=45
	except=(042 044 045)
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
