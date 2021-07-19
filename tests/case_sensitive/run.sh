#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
API_VERSION="v1alpha1"

function run() {
	run_sql_both_source "SET @@GLOBAL.SQL_MODE='ANSI_QUOTES,NO_AUTO_VALUE_ON_ZERO'"
	inject_points=(
		"github.com/pingcap/dm/dm/worker/TaskCheckInterval=return(\"500ms\")"
		"github.com/pingcap/dm/relay/NewUpstreamServer=return(true)"
	)
	export GO_FAILPOINTS="$(join_string \; ${inject_points[@]})"

	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_contains 'Query OK, 2 rows affected'
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_contains 'Query OK, 3 rows affected'
	# manually create the route table
	run_sql 'CREATE DATABASE IF NOT EXISTS `UPPER_DB_ROUTE`' $TIDB_PORT $TIDB_PASSWORD

	# start DM worker and master
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	check_metric $MASTER_PORT 'start_leader_counter' 3 0 2
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	# make sure source1 is bound to worker1
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	# start DM task only
	cp $cur/conf/dm-task.yaml $WORK_DIR/dm-task.yaml
	dmctl_start_task "$WORK_DIR/dm-task.yaml" "--remove-meta"
	# check task has started
	check_metric $WORKER1_PORT "dm_worker_task_state{source_id=\"mysql-replica-01\",task=\"test\"}" 3 1 3
	check_metric $WORKER2_PORT "dm_worker_task_state{source_id=\"mysql-replica-02\",task=\"test\"}" 3 1 3

	# use sync_diff_inspector to check full dump loader
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# restart dm-worker1
	pkill -hup -f dm-worker1.toml 2>/dev/null || true
	wait_pattern_exit dm-worker1.toml
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	# make sure worker1 have bound a source, and the source should same with bound before
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"worker1" 1

	# restart dm-worker2
	pkill -hup -f dm-worker2.toml 2>/dev/null || true
	wait_pattern_exit dm-worker2.toml
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	check_metric $WORKER1_PORT "dm_worker_task_state{source_id=\"mysql-replica-01\",task=\"test\"}" 3 1 3
	check_metric $WORKER2_PORT "dm_worker_task_state{source_id=\"mysql-replica-02\",task=\"test\"}" 3 1 3

	sleep 10
	echo "after restart dm-worker, task should resume automatically"

	# wait for task running
	check_http_alive 127.0.0.1:$MASTER_PORT/apis/${API_VERSION}/status/test '"stage": "Running"' 10
	sleep 2 # still wait for subtask running on other dm-workers

	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	# use sync_diff_inspector to check data now!
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# test block-allow-list by the way
	run_sql "show databases;" $TIDB_PORT $TIDB_PASSWORD
	check_not_contains "Upper_Db_IGNORE"
	check_contains "Upper_DB1"
	check_contains "lower_db"
	# test route-rule
	check_contains "UPPER_DB_ROUTE"

	run_sql "show tables from UPPER_DB_ROUTE" $TIDB_PORT $TIDB_PASSWORD
	check_contains "do_table_route"
	check_not_contains "Do_table_ignore"

	run_sql "select count(*) from UPPER_DB_ROUTE.do_table_route" $TIDB_PORT $TIDB_PASSWORD
	# ensure the truncate is ignored and the new row is inserted
	check_contains "count(*): 5"
	# test binlog event filter
	run_sql "truncate table Upper_DB.Do_Table" $MYSQL_PORT1 $MYSQL_PASSWORD1
	# insert another row
	run_sql "INSERT INTO Upper_DB.Do_Table (id, name) values (103, 'new');" $MYSQL_PORT1 $MYSQL_PASSWORD1
	sleep 2
	run_sql "select count(*) from UPPER_DB_ROUTE.do_table_route" $TIDB_PORT $TIDB_PASSWORD
	# ensure the truncate is ignored and the new row is inserted
	check_contains "count(*): 6"

	export GO_FAILPOINTS=''
}

trap cleanup_process EXIT
trap "cleanup_data Upper_DB Upper_DB1 lower_db UPPER_DB_ROUTE sync_diff_inspector" EXIT

# cleanup_data all_mode
# also cleanup dm processes in case of last run failed
cleanup_process $*
cleanup_data Upper_DB Upper_DB1 lower_db UPPER_DB_ROUTE
run $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
