#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
source $cur/lib.sh

# 4215, 4217
function DM_4215_CASE() {
	run_sql_source1 "insert into ${db}.${tb1} values(1);"
	run_sql_source2 "insert into ${db}.${tb1} values(2);"
	run_sql_source1 "alter table ${db}.${tb1} add column c int unique;"
	run_sql_source2 "alter table ${db}.${tb1} add column c int unique;"
	run_sql_source1 "alter table ${db}.${tb1} add column d int unique;"
	run_sql_source2 "alter table ${db}.${tb1} add column d int unique;"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"unsupported add column 'c' constraint UNIQUE KEY" 2

	first_pos1=$(get_start_pos 127.0.0.1:$MASTER_PORT $source1)
	first_pos2=$(get_start_pos 127.0.0.1:$MASTER_PORT $source2)
	first_name1=$(get_start_name 127.0.0.1:$MASTER_PORT $source1)
	first_name2=$(get_start_name 127.0.0.1:$MASTER_PORT $source2)

	second_pos1=$(get_next_query_pos $MYSQL_PORT1 $MYSQL_PASSWORD1 $first_pos1)
	second_pos2=$(get_next_query_pos $MYSQL_PORT2 $MYSQL_PASSWORD2 $first_pos2)

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"handle-error test -s $source1 -b $first_name1:$second_pos1 replace alter table ${db}.${tb1} add column d int;alter table ${db}.${tb1} add unique(d);" \
		"\"result\": true" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"unsupported add column 'c' constraint UNIQUE KEY" 2

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"handle-error test -s $source1 -b $first_name1:$first_pos1 replace alter table ${db}.${tb1} add column c int;alter table ${db}.${tb1} add unique(c);" \
		"\"result\": true" 2

	run_sql_source1 "insert into ${db}.${tb1} values(3,3,3);"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"unsupported add column 'c' constraint UNIQUE KEY" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"handle-error test -s $source2 -b $first_name2:$first_pos2 replace alter table ${db}.${tb1} add column c int;alter table ${db}.${tb1} add unique(c);" \
		"\"result\": true" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"unsupported add column 'd' constraint UNIQUE KEY" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"handle-error test -s $source2 -b $first_name2:$second_pos2 replace alter table ${db}.${tb1} add column d int;alter table ${db}.${tb1} add unique(d);" \
		"\"result\": true" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 3

	run_sql_source2 "insert into ${db}.${tb1} values(4,4,4);"

	run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 4"
}

function DM_4215() {
	run_case 4215 "double-source-pessimistic" "init_table 11 21" "clean_table" "pessimistic"
	run_case 4215 "double-source-optimistic" "init_table 11 21" "clean_table" "optimistic"
}

function DM_4216_CASE() {
	run_sql_source1 "insert into ${db}.${tb1} values(1);"
	run_sql_source2 "insert into ${db}.${tb1} values(2);"
	run_sql_source1 "alter table ${db}.${tb1} add column c int unique;"
	run_sql_source2 "alter table ${db}.${tb1} add column c int unique;"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"unsupported add column .* constraint UNIQUE KEY" 2

	start_location1=$(get_start_location 127.0.0.1:$MASTER_PORT $source1)
	start_location2=$(get_start_location 127.0.0.1:$MASTER_PORT $source2)

	if [ "$start_location1" = "$start_location2" ]; then
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"handle-error test -b $start_location1 replace alter table ${db}.${tb1} add column c int;alter table ${db}.${tb1} add unique(c);" \
			"\"result\": true" 3
	else
		# WARN: may replace unknown event like later insert, test will fail
		# It hasn't happened yet.
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"handle-error test -b $start_location1 replace alter table ${db}.${tb1} add column c int;alter table ${db}.${tb1} add unique(c);" \
			"\"result\": true" 3

		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"unsupported add column .* constraint UNIQUE KEY" 1

		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"handle-error test -b $start_location2 replace alter table ${db}.${tb1} add column c int;alter table ${db}.${tb1} add unique(c);" \
			"\"result\": true" 3
	fi

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 3

	run_sql_source1 "insert into ${db}.${tb1} values(3,3);"
	run_sql_source2 "insert into ${db}.${tb1} values(4,4);"

	run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where id<100;" "count(1): 4"
}

function DM_4216() {
	run_case 4216 "double-source-pessimistic" "init_table 11 21" "clean_table" "pessimistic"
	run_case 4216 "double-source-optimistic" "init_table 11 21" "clean_table" "optimistic"

	# test different error locations
	run_case 4216 "double-source-pessimistic" "init_table 11 21;run_sql_source1 \"insert into ${db}.${tb1} values(100);\"" "clean_table" "pessimistic"
	run_case 4216 "double-source-optimistic" "init_table 11 21;run_sql_source1 \"insert into ${db}.${tb1} values(100);\"" "clean_table" "optimistic"
}

function DM_4219_CASE() {
	run_sql_source1 "insert into ${db}.${tb1} values(1);"
	run_sql_source2 "insert into ${db}.${tb1} values(2);"
	run_sql_source1 "alter table ${db}.${tb1} add column c int unique;"
	run_sql_source2 "alter table ${db}.${tb1} add column c int unique;"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"unsupported add column .* constraint UNIQUE KEY" 2

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"handle-error test -s $source1 replace alter table ${db}.${tb1} add column c int;alter table ${db}.${tb1} add unique(c);" \
		"\"result\": true" 2

	run_sql_source1 "insert into ${db}.${tb1} values(3,3);"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"unsupported add column .* constraint UNIQUE KEY" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"handle-error test -s $source2 replace alter table ${db}.${tb1} add column c int;alter table ${db}.${tb1} add unique(c);" \
		"\"result\": true" 2

	run_sql_source2 "insert into ${db}.${tb1} values(4,4);"

	run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 4"
}

function DM_4219() {
	run_case 4219 "double-source-pessimistic" "init_table 11 21" "clean_table" "pessimistic"
	run_case 4219 "double-source-optimistic" "init_table 11 21" "clean_table" "optimistic"
}

# 4220, 4224, 4226, 4229, 4194, 4195, 4196
function DM_4220_CASE() {
	run_sql_source1 "insert into ${db}.${tb1} values(1);"
	run_sql_source2 "insert into ${db}.${tb1} values(2);"
	run_sql_source1 "alter table ${db}.${tb1} modify id varchar(10);"
	run_sql_source1 "alter table ${db}.${tb1} modify id varchar(20);"
	run_sql_source1 "alter table ${db}.${tb1} modify id varchar(30);"
	run_sql_source2 "alter table ${db}.${tb1} modify id varchar(10);"
	run_sql_source2 "alter table ${db}.${tb1} modify id varchar(20);"
	run_sql_source2 "alter table ${db}.${tb1} modify id varchar(30);"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unsupported modify column.*varchar(10)" 2

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"handle-error test revert" \
		"operator not exist" 2

	first_pos1=$(get_start_pos 127.0.0.1:$MASTER_PORT $source1)
	first_pos2=$(get_start_pos 127.0.0.1:$MASTER_PORT $source2)
	first_name1=$(get_start_name 127.0.0.1:$MASTER_PORT $source1)
	first_name2=$(get_start_name 127.0.0.1:$MASTER_PORT $source2)

	second_pos1=$(get_next_query_pos $MYSQL_PORT1 $MYSQL_PASSWORD1 $first_pos1)
	second_pos2=$(get_next_query_pos $MYSQL_PORT2 $MYSQL_PASSWORD2 $first_pos2)
	third_pos1=$(get_next_query_pos $MYSQL_PORT1 $MYSQL_PASSWORD1 $second_pos1)
	third_pos2=$(get_next_query_pos $MYSQL_PORT2 $MYSQL_PASSWORD2 $second_pos2)

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"handle-error test skip -s $source1 -b $first_name1:$second_pos1" \
		"\"result\": true" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"handle-error test skip -s $source2 -b $first_name2:$second_pos2" \
		"\"result\": true" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unsupported modify column.*varchar(10)" 2

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"handle-error test revert" \
		"operator not exist" 2

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"handle-error test skip -s $source1 -b $first_name1:$third_pos1" \
		"\"result\": true" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"handle-error test skip -s $source2 -b $first_name2:$third_pos2" \
		"\"result\": true" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unsupported modify column.*varchar(10)" 2

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"handle-error test revert -s $source1 -b $first_name1:$third_pos1" \
		"\"result\": true" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"handle-error test revert -s $source2 -b $first_name2:$third_pos2" \
		"\"result\": true" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unsupported modify column.*varchar(10)" 2

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"handle-error test skip" \
		"\"result\": true" 3

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unsupported modify column.*varchar(30)" 2

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"handle-error test skip" \
		"\"result\": true" 3

	# flush checkpoint, otherwise revert may "succeed"
	run_sql_source1 "alter table ${db}.${tb1} add column new_col int;"
	run_sql_source2 "alter table ${db}.${tb1} add column new_col int;"

	run_sql_source1 "insert into ${db}.${tb1} values(3,3);"
	run_sql_source2 "insert into ${db}.${tb1} values(4,4);"
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 4"

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"handle-error test revert -s $source1 -b $first_name1:$third_pos1" \
		"operator not exist" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"handle-error test revert -s $source2 -b $first_name2:$third_pos2" \
		"operator not exist" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"handle-error test revert -b $first_name2:$third_pos2" \
		"operator not exist" 2

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"handle-error test revert" \
		"source.*has no error" 2

	run_sql_source1 "insert into ${db}.${tb1} values(5,5);"
	run_sql_source2 "insert into ${db}.${tb1} values(6,6);"

	run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 6"
}

function DM_4220() {
	run_case 4220 "double-source-pessimistic" "init_table 11 21" "clean_table" "pessimistic"
	run_case 4220 "double-source-optimistic" "init_table 11 21" "clean_table" "optimistic"
}

# 4185, 4187, 4186
function DM_4185_CASE() {
	run_sql_source1 "insert into ${db}.${tb1} values(1);"
	run_sql_source2 "insert into ${db}.${tb1} values(2);"
	run_sql_source1 "alter table ${db}.${tb1} modify id varchar(10);"
	run_sql_source2 "alter table ${db}.${tb1} modify id varchar(10);"
	run_sql_source1 "alter table ${db}.${tb1} modify id varchar(20);"
	run_sql_source2 "alter table ${db}.${tb1} modify id varchar(20);"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unsupported modify column.*varchar(10)" 2

	first_pos1=$(get_start_pos 127.0.0.1:$MASTER_PORT $source1)
	first_pos2=$(get_start_pos 127.0.0.1:$MASTER_PORT $source2)
	first_name1=$(get_start_name 127.0.0.1:$MASTER_PORT $source1)
	first_name2=$(get_start_name 127.0.0.1:$MASTER_PORT $source2)
	second_pos1=$(get_next_query_pos $MYSQL_PORT1 $MYSQL_PASSWORD1 $first_pos1)
	second_pos2=$(get_next_query_pos $MYSQL_PORT2 $MYSQL_PASSWORD2 $first_pos2)

	if [ "$second_pos1" = "$second_pos2" ]; then
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"handle-error test skip -b $first_name1:$second_pos1" \
			"\"result\": true" 3
	else
		# WARN: may skip unknown event like later insert, test will fail
		# It hasn't happened yet.
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"handle-error test skip -b $first_name1:$second_pos1" \
			"\"result\": true" 3

		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"Unsupported modify column.*varchar(10)" 2

		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"handle-error test skip -b $first_name1:$second_pos2" \
			"\"result\": true" 3
	fi

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unsupported modify column.*varchar(10)" 2

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"handle-error test skip" \
		"\"result\": true" 3

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 3

	run_sql_source1 "insert into ${db}.${tb1} values(3);"
	run_sql_source2 "insert into ${db}.${tb1} values(4);"

	run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where id<100;" "count(1): 4"
}

function DM_4185() {
	run_case 4185 "double-source-pessimistic" "init_table 11 21" "clean_table" "pessimistic"
	run_case 4185 "double-source-optimistic" "init_table 11 21" "clean_table" "optimistic"

	# test different error locations
	run_case 4185 "double-source-pessimistic" "init_table 11 21;run_sql_source1 \"insert into ${db}.${tb1} values(100);\"" "clean_table" "pessimistic"
	run_case 4185 "double-source-optimistic" "init_table 11 21;run_sql_source1 \"insert into ${db}.${tb1} values(100);\"" "clean_table" "optimistic"
}

# 4201, 4203, 4205
function DM_4201_CASE() {
	run_sql_source1 "insert into ${db}.${tb1} values(1);"
	run_sql_source1 "alter table ${db}.${tb1} modify id varchar(10);"
	run_sql_source1 "alter table ${db}.${tb1} modify id varchar(20);"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unsupported modify column.*varchar(10)" 1

	first_pos1=$(get_start_pos 127.0.0.1:$MASTER_PORT $source1)
	first_name1=$(get_start_name 127.0.0.1:$MASTER_PORT $source1)
	second_pos1=$(get_next_query_pos $MYSQL_PORT1 $MYSQL_PASSWORD1 $first_pos1)

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"handle-error test skip -b $first_name1:$second_pos1" \
		"\"result\": true" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unsupported modify column.*varchar(10)" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"handle-error test skip" \
		"\"result\": true" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 2

	run_sql_source1 "insert into ${db}.${tb1} values(2);"

	run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where id<100;" "count(1): 2"
}

function DM_4201() {
	run_case 4201 "single-source-no-sharding" "init_table 11" "clean_table" ""
}

function run() {
	init_cluster
	init_database

	implement=(4215 4216 4219 4220 4185 4201)
	for i in ${implement[@]}; do
		DM_${i}
		sleep 1
	done
}

cleanup_data $db
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
