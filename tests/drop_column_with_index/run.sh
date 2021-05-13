#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

function run() {
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	export GO_FAILPOINTS="github.com/pingcap/dm/syncer/SyncerGetEventError=return"

	# start DM worker and master
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $SOURCE_ID1 worker1" \
		"\"result\": true" 1

	# start DM task only
	dmctl_start_task_standalone "$cur/conf/dm-task.yaml" "--remove-meta"

	# use sync_diff_inspector to check full dump loader
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	check_log_contain_with_retry "mock upstream instance restart" $WORK_DIR/worker1/log/dm-worker.log
	# check_log_contain_with_retry "dispatch auto resume task" $WORK_DIR/worker1/log/dm-worker.log
	check_log_contain_with_retry "meet error when read from local binlog, will switch to remote binlog" $WORK_DIR/worker1/log/dm-worker.log

	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	# use sync_diff_inspector to check data now!
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# check column covered by multi-column indices won't drop, and its indices won't drop
	run_sql "alter table drop_column_with_index.t1 drop column c2;" $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql "show index from drop_column_with_index.t1" $TIDB_PORT $TIDB_PASSWORD
	check_count "Column_name: c2" 3

	export GO_FAILPOINTS=""
}

cleanup_data drop_column_with_index
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
