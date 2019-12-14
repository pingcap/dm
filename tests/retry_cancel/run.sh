#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

function run() {
    run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1
    check_contains 'Query OK, 2 rows affected'
    run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2
    check_contains 'Query OK, 2 rows affected'

    # inject error for loading data
    export GO_FAILPOINTS='github.com/pingcap/dm/pkg/conn/retryableError=return("retry_cancel")'

    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
    run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT

    # start-task with retry_cancel enabled
    dmctl_start_task

    sleep 5 # should sleep > retryTimeout (now 3s)

    # query-task, it should still be running (retrying)
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status test" \
        "\"stage\": \"Running\"" 4

    # check log, retrying in load unit
    check_log_contains $WORK_DIR/worker1/log/dm-worker.log '\["execute statements"\] \[task=test\] \[unit=load\] \[retry=0\] \[queries="\[CREATE DATABASE `retry_cancel`;\]"\]'

    # stop-task, should not block too much time
    start_time=$(date +%s)
    dmctl_stop_task test
    duration=$(( $(date +%s)-$start_time ))
    if [[ $duration -gt 3 ]]; then
        echo "stop-task tasks for full import too long duration $duration"
        exit 1
    fi

    # stop DM-worker, then update failpoint for checkpoint
    kill_dm_worker
    export GO_FAILPOINTS='github.com/pingcap/dm/pkg/conn/retryableError=return("UPDATE `dm_meta`.`test_loader_checkpoint`")'

    # start DM-worker again
    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
    sleep 2 # wait gRPC from DM-master to DM-worker established again

    dmctl_start_task

    sleep 5 # should sleep > retryTimeout (now 3s)

    # query-task, it should still be running (retrying)
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status test" \
        "\"stage\": \"Running\"" 4

    # check log, retrying in load unit
    check_log_contains $WORK_DIR/worker1/log/dm-worker.log 'Error 1213: failpoint inject retryable error for UPDATE `dm_meta`.`test_loader_checkpoint`'

    # stop-task, should not block too much time
    start_time=$(date +%s)
    dmctl_stop_task test
    duration=$(( $(date +%s)-$start_time ))
    if [[ $duration -gt 3 ]]; then
        echo "stop-task tasks for updating loader checkpoint too long duration $duration"
        exit 1
    fi

    # stop DM-worker, then disable failponits
    kill_dm_worker
    export GO_FAILPOINTS=''

    # start DM-worker again
    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
    sleep 2 # wait gRPC from DM-master to DM-worker established again

    # start-task with retry_cancel disabled
    dmctl_start_task

    # use sync_diff_inspector to check full dump loader
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    # ---------- test for incremental replication ----------
    # stop DM-worker, then enable failponits
    kill_dm_worker
    export GO_FAILPOINTS="github.com/pingcap/dm/pkg/conn/retryableError=return(\"retry_cancel\")"

    # run sql files to trigger incremental replication
    run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1
    run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2

    # start DM-worker again
    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

    sleep 2
    echo "start task after incremental stopped"
    dmctl_start_task

    sleep 5 # should sleep > retryTimeout (now 3s)

    # query-task, it should still be running (retrying)
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status test" \
        "\"stage\": \"Running\"" 4

    # check log, retrying in binlog replication unit
    check_log_contains $WORK_DIR/worker1/log/dm-worker.log '\["execute statements"\] \[task=test\] \[unit="binlog replication"\] \[retry=0\] \[queries="\[REPLACE INTO `retry_cancel`'

    # stop-task, should not block too much time
    start_time=$(date +%s)
    dmctl_stop_task test
    duration=$(( $(date +%s)-$start_time ))
    if [[ $duration -gt 3 ]]; then
        echo "stop-task tasks for incremental replication too long duration $duration"
        exit 1
    fi

    # stop DM-worker, then disable failponits
    kill_dm_worker
    export GO_FAILPOINTS=''

    # start DM-worker again
    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
    sleep 2 # wait gRPC from DM-master to DM-worker established again

    # start-task with retry_cancel disabled
    dmctl_start_task

    # use sync_diff_inspector to check data now!
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

cleanup_data retry_cancel
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
