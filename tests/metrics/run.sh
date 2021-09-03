#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

function run() {
	# add changeTickerInterval to keep metric from updating to zero too quickly when there is no work in the queue.
	inject_points=(
		"github.com/pingcap/dm/syncer/BlockDDLJob=return(1)"
		"github.com/pingcap/dm/syncer/changeTickerInterval=return(10)"
		"github.com/pingcap/dm/syncer/ShowLagInLog=return(1)" # test lag metric >= 1 beacuse we inject BlockDDLJob(ddl) to sleep(1)
	)
	export GO_FAILPOINTS="$(join_string \; ${inject_points[@]})"

	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	# start DM worker and master
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT

	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker2/relay_log" $WORK_DIR/source2.yaml

	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	# start DM task
	cp $cur/conf/dm-task.yaml $WORK_DIR/dm-task.yaml
	dmctl_start_task "$WORK_DIR/dm-task.yaml" "--remove-meta"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 3

	check_metric $WORKER1_PORT 'dm_worker_task_state{source_id="mysql-replica-01",task="test",worker="worker1"}' 10 1 3
	check_metric $WORKER2_PORT 'dm_worker_task_state{source_id="mysql-replica-02",task="test",worker="worker2"}' 10 1 3

	# check ddl job lag
	run_sql_source1 "alter table metrics.t1 add column new_col1 int;"
	run_sql_source2 "alter table metrics.t2 add column new_col1 int;"

	# check new metric dm_syncer_flush_checkpoints_time_interval exists
	check_metric $WORKER1_PORT 'dm_syncer_flush_checkpoints_time_interval_sum{source_id="mysql-replica-01",task="test",worker="worker1"}' 5 -1 99999
	check_metric $WORKER2_PORT 'dm_syncer_flush_checkpoints_time_interval_sum{source_id="mysql-replica-02",task="test",worker="worker2"}' 5 -1 99999

	# check two worker's lag >= 1
	check_log_contain_with_retry "[ShowLagInLog]" $WORK_DIR/worker1/log/dm-worker.log
	check_log_contain_with_retry "[ShowLagInLog]" $WORK_DIR/worker2/log/dm-worker.log

	check_metric $WORKER1_PORT 'dm_syncer_replication_lag_sum{source_id="mysql-replica-01",task="test",worker="worker1"}' 5 0 999
	check_metric $WORKER2_PORT 'dm_syncer_replication_lag_sum{source_id="mysql-replica-02",task="test",worker="worker2"}' 5 0 999
	echo "check ddl lag done!"

	# restart dm worker
	kill_dm_worker
	rm -rf $WORK_DIR/worker1/log/dm-worker.log # clean up the old log
	rm -rf $WORK_DIR/worker2/log/dm-worker.log # clean up the old log
	inject_points=(
		"github.com/pingcap/dm/syncer/BlockExecuteSQLs=return(2)"
		"github.com/pingcap/dm/syncer/changeTickerInterval=return(10)"
		"github.com/pingcap/dm/syncer/ShowLagInLog=return(2)" # test lag metric >= 2 beacuse we inject BlockExecuteSQLs to sleep(2) although skip lag is 0 (locally), but we use that lag of all dml/skip lag, so lag still >= 2
	)
	export GO_FAILPOINTS="$(join_string \; ${inject_points[@]})"

	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	run_sql_source1 "truncate table metrics.t1;"                                        # make skip job
	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1 # make dml job
	run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2 # make dml job

	# check new metric: dm_syncer_replication_lag_sum,dm_syncer_replication_lag_gauge,
	# finished_transaction_total,dm_syncer_ideal_qps,dm_syncer_binlog_event_row,replication_transaction_batch exists
	check_metric $WORKER1_PORT 'dm_syncer_replication_lag_sum{source_id="mysql-replica-01",task="test",worker="worker1"}' 5 -1 999
	check_metric $WORKER2_PORT 'dm_syncer_replication_lag_sum{source_id="mysql-replica-02",task="test",worker="worker2"}' 5 -1 999

	check_metric $WORKER1_PORT 'dm_syncer_replication_lag_gauge{source_id="mysql-replica-01",task="test",worker="worker1"}' 5 1 999
	check_metric $WORKER2_PORT 'dm_syncer_replication_lag_gauge{source_id="mysql-replica-02",task="test",worker="worker2"}' 5 1 999

	check_metric $WORKER1_PORT 'dm_syncer_finished_transaction_total{source_id="mysql-replica-01",task="test",worker="worker1"}' 5 1 99999
	check_metric $WORKER2_PORT 'dm_syncer_finished_transaction_total{source_id="mysql-replica-02",task="test",worker="worker2"}' 5 1 99999

	check_metric $WORKER1_PORT 'dm_syncer_ideal_qps{source_id="mysql-replica-01",task="test",worker="worker1"' 5 1 99999
	check_metric $WORKER2_PORT 'dm_syncer_ideal_qps{source_id="mysql-replica-02",task="test",worker="worker2"' 5 1 99999

	check_metric $WORKER1_PORT 'dm_syncer_binlog_event_row_sum{source_id="mysql-replica-01",task="test",worker="worker1"}' 5 0 99999
	check_metric $WORKER2_PORT 'dm_syncer_binlog_event_row_sum{source_id="mysql-replica-02",task="test",worker="worker2"}' 5 0 99999

	check_metric $WORKER1_PORT 'dm_syncer_replication_transaction_batch_count' 5 0 99999
	check_metric $WORKER2_PORT 'dm_syncer_replication_transaction_batch_count' 5 0 99999

	# check two worker's lag >= 2
	check_log_contain_with_retry "[ShowLagInLog]" $WORK_DIR/worker1/log/dm-worker.log
	check_log_contain_with_retry "[ShowLagInLog]" $WORK_DIR/worker2/log/dm-worker.log
	check_metric $WORKER1_PORT 'dm_syncer_replication_lag_sum{source_id="mysql-replica-01",task="test",worker="worker1"}' 5 1 999
	check_metric $WORKER2_PORT 'dm_syncer_replication_lag_sum{source_id="mysql-replica-02",task="test",worker="worker2"}' 5 1 999
	echo "check dml/skip lag done!"

	# restart dm worker
	kill_dm_worker
	export GO_FAILPOINTS=''
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	# check the dmctl query-status no new dml, lag should be set to 0
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"secondsBehindMaster\": \"0\"" 2
	echo "check zero job done!"

	# restart dm-worker1
	pkill -hup -f dm-worker1.toml 2>/dev/null || true
	wait_pattern_exit dm-worker1.toml

	inject_points=(
		"github.com/pingcap/dm/syncer/changeTickerInterval=return(5)"
		"github.com/pingcap/dm/syncer/noJobInQueueLog=return()"
		"github.com/pingcap/dm/syncer/IgnoreSomeTypeEvent=return(\"HeartbeatEvent\")"
	)
	# Since the following test needs to ensure that the dml queue is empty for a long time,
	# it needs to ignore upstream heartbeat events to ensure that flushjobs are not triggered
	export GO_FAILPOINTS="$(join_string \; ${inject_points[@]})"
	# First set the ticker interval to 5s -> expect the execSQL interval to be greater than 5s
	# At 5s, the first no job log will appear in the log
	# At 6s, the ticker has already waited 1s and the ticker goes to 1/5th of the way
	# At 6s, a dml job is added to job chan and the ticker is reset
	# At 11s the ticker write the log of the second no job
	# Check that the interval between the two ticker logs is > 5s

	rm -rf $WORK_DIR/worker1/log/dm-worker.log # clean up the old log
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	check_log_contain_with_retry 'no job in queue, update lag to zero' $WORK_DIR/worker1/log/dm-worker.log
	echo "make a dml job"
	run_sql_source1 "insert into metrics.t1 (id, name, ts) values (1004, 'zmj4', '2022-05-11 12:01:05')"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	echo "sleep 6s"
	sleep 6
	check_ticker_interval $WORK_DIR/worker1/log/dm-worker.log 5
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task test" \
		"\"result\": true" 3
	export GO_FAILPOINTS=''
}

cleanup_data metrics
# also cleanup dm processes in case of last run failed
cleanup_process
run
cleanup_process

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
