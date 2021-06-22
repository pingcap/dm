#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
API_VERSION="v1alpha1"

function run() {
	# TODO: test generated column
	# TODO: test alter table during sync
	# TODO: test complex filter grammar
	# TODO: test UPDATE SQL
	run_dm_master $WORK_DIR/master $MASTER_PORT1 $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT1

	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	dmctl_operate_source create $cur/conf/source1.yaml $SOURCE_ID1

	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	dmctl_start_task_standalone $cur/conf/dm-task.yaml

	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	# too many expression filter will slow the sync
	sleep 5
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 2 \
		"\"synced\": true" 1

  check_not_contains "count(*): 60"
  run_sql_source1 "select count(*) from expr_filter.t1"
  check_contains "count(*): 60"

  check_not_contains "count(*): 30"
  run_sql_tidb "select count(*) from expr_filter.t1"
  check_contains "count(*): 30"

  check_not_contains "count(*): 0"
  run_sql_tidb "select count(*) from expr_filter.t1 where should_skip = 1"
  check_contains "count(*): 0"
}

cleanup_data expr_filter
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
