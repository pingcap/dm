#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

function check_secondsBehindMaster() {
	min_val=$1
	need_cnt=$2
	$PWD/bin/dmctl.test DEVEL --master-addr=:$MASTER_PORT query-status "test" >$WORK_DIR/query-status.log
	query_msg=$(cat $WORK_DIR/query-status.log)
	cnt=$(echo "${query_msg}" | jq -r --arg min_val $min_val '.sources[].subTaskStatus[].sync | select((.secondsBehindMaster|tonumber)>($min_val | tonumber)).secondsBehindMaster' | wc -l)
	if [ $cnt != $need_cnt ]; then
		echo "check secondsBehindMaster faild, cnt: $cnt need_cnt: $need_cnt"
		exit 1
	fi
}

function run() {
	# add changeTickerInterval to keep metric from updating to zero too quickly when there is no work in the queue.
	export GO_FAILPOINTS="github.com/pingcap/dm/syncer/BlockSyncerUpdateLag=return(\"ddl,1\");github.com/pingcap/dm/syncer/changeTickerInterval=return(10)"
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

	# check ddl job lag
	run_sql_source1 "alter table metrics.t1 add column new_col1 int;"
	run_sql_source2 "alter table metrics.t2 add column new_col1 int;"

	# test dml lag metric >= 1 beacuse we inject updateReplicationLag(ddl) to sleep(1)
	check_metric $WORKER1_PORT 'dm_syncer_replication_lag{source_id="mysql-replica-01",task="test",worker="worker1"}' 5 0 999
	check_metric $WORKER2_PORT 'dm_syncer_replication_lag{source_id="mysql-replica-02",task="test",worker="worker2"}' 5 0 999

	# check two worker's secondsBehindMaster > 0
	check_secondsBehindMaster 0 2
	echo "check ddl done!"

	# restart dm worker
	kill_dm_worker
	export GO_FAILPOINTS="github.com/pingcap/dm/syncer/BlockSyncerUpdateLag=return(\"insert,2\");github.com/pingcap/dm/syncer/changeTickerInterval=return(10)"
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	run_sql_source1 "truncate table metrics.t1;"                                        # make skip job
	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1 # make dml job
	run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2 # make dml job

	# test dml lag metric >= 2 beacuse we inject updateReplicationLag(insert) to sleep(2)
	# although skip lag is 0 (locally), but we use that lag of all dml/skip lag, so lag still >= 2
	check_metric $WORKER1_PORT 'dm_syncer_replication_lag{source_id="mysql-replica-01",task="test",worker="worker1"}' 5 1 999
	check_metric $WORKER2_PORT 'dm_syncer_replication_lag{source_id="mysql-replica-02",task="test",worker="worker2"}' 5 1 999

	# new metric finished_transaction_total,dm_syncer_ideal_qps,dm_syncer_binlog_event_row,dm_syncer_flush_checkpoints_count exists

	check_metric $WORKER1_PORT 'dm_syncer_finished_transaction_total{source_id="mysql-replica-01",task="test",worker="worker1"}' 5 1 99999
	check_metric $WORKER2_PORT 'dm_syncer_finished_transaction_total{source_id="mysql-replica-02",task="test",worker="worker2"}' 5 1 99999

	check_metric $WORKER1_PORT 'dm_syncer_ideal_qps{source_id="mysql-replica-01",task="test",worker="worker1"' 5 1 99999
	check_metric $WORKER2_PORT 'dm_syncer_ideal_qps{source_id="mysql-replica-02",task="test",worker="worker2"' 5 1 99999

	check_metric $WORKER1_PORT 'dm_syncer_binlog_event_row{source_id="mysql-replica-01",task="test",worker="worker1"}' 5 0 99999
	check_metric $WORKER2_PORT 'dm_syncer_binlog_event_row{source_id="mysql-replica-02",task="test",worker="worker2"}' 5 0 99999

	check_metric $WORKER1_PORT 'dm_syncer_flush_checkpoints_count{source_id="mysql-replica-01",task="test",worker="worker1"}' 5 0 99999
	check_metric $WORKER2_PORT 'dm_syncer_flush_checkpoints_count{source_id="mysql-replica-02",task="test",worker="worker2"}' 5 0 99999

	check_secondsBehindMaster 1 2
	echo "check dml/skip done!"

	# restart dm worker
	kill_dm_worker
	export GO_FAILPOINTS=''
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	# check the dmctl query-status no new dml, lag should be set to 0
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"secondsBehindMaster\": \"0\"" 2
	echo "check zero job done!"

	kill_dm_worker
	export GO_FAILPOINTS="github.com/pingcap/dm/syncer/changeTickerInterval=return(5)"
	# First set the ticker interval to 5s -> expect the execSQL interval to be greater than 5s
	# At 5s, the first no job log will appear in the log
	# At 6s, the ticker has already waited 1s and the ticker goes to 1/5th of the way
	# At 6s, a dml job is added to jobchan and the ticker is reset
	# At 11s the ticker write the log of the second nojob
	# Check that the interval between the two ticker logs is > 5s
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	echo "sleep 6s"
	sleep 6
	echo "make a dml job"
	run_sql_source1 "use metrics;insert into t1 (id, name, ts) values (1004, 'zmj4', '2022-05-11 12:01:05')"
	echo "sleep 6s"
	sleep 6
	$cur/../_utils/check_ticker_interval.py $WORK_DIR/worker1/log/dm-worker.log 5
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task test" \
		"\"result\": true" 3
	cleanup_data metrics
	cleanup_process $*
	export GO_FAILPOINTS=''
}

cleanup_data metrics
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
