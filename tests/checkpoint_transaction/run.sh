#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

function run_big_transaction() {
	nums=$(($1 * 100))
	sql_file=$2
	sed -i "s/call (100);/call ($nums);/g" $sql_file
	run_sql_file $sql_file $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
}

function run() {
	export GO_FAILPOINTS="github.com/pingcap/dm/syncer/checkCheckpointInMiddleOfTransaction=return"

	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_contains 'Query OK, 1 row affected'

	# run dm master
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	check_metric $MASTER_PORT 'start_leader_counter' 3 0 2

	# bound source1 to worker1
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	dmctl_operate_source create $cur/conf/source1.yaml $SOURCE_ID1

	# start a task in all mode
	dmctl_start_task_standalone $cur/conf/dm-task.yaml

	# check diff
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# try 10 times, make sure checkpoint in the middle position of transaction
	for ((i = 1; i <= 10; i++)); do
		# copy file
		cp $cur/data/db1.increment1.sql $WORK_DIR/db1.increment1.sql
		run_big_transaction $i $WORK_DIR/db1.increment1.sql
		sleep 0.1  # wait big_transaction start
		echo "pause task and check status"
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"pause-task test" \
			"\"result\": true" 2
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"\"stage\": \"Paused\"" 1
		num=$(grep "not receive xid job yet" $WORK_DIR/worker1/log/dm-worker.log | wc -l)

		if [ $num -gt 0 ]; then
			break
		fi
		echo "resume task and check status"
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"resume-task test" \
			"\"result\": true" 2
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"\"stage\": \"Running\"" 1
	done
	[[ $i -lt 10 ]]

	echo "start check pause diff"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	echo "resume task and check status"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-task test" \
		"\"result\": true" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Running\"" 1

	# try 10 times, make sure checkpoint in the middle position of transaction
	for ((i = 1; i <= 10; i++)); do
		# copy file
		cp $cur/data/db1.increment2.sql $WORK_DIR/db1.increment2.sql
		run_big_transaction $i $WORK_DIR/db1.increment2.sql
		sleep 0.1  # wait big_transaction start
		echo "stop task"
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"stop-task test" \
			"\"result\": true" 2

		num=$(grep "not receive xid job yet" $WORK_DIR/worker1/log/dm-worker.log | wc -l)

		if [ $num -gt 0 ]; then
			break
		fi
		# start a task in all mode
		dmctl_start_task_standalone $cur/conf/dm-task.yaml
		# check diff
		check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	done
	[[ $i -lt 10 ]]

	echo "start check stop diff"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	export GO_FAILPOINTS=""
}

cleanup_data checkpoint_transaction
# also cleanup dm processes in case of last run failed
cleanup_process
run
cleanup_process

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
