#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

function run() {
    run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1
    run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status -w worker-x task-y" \
        "can not query task-y task's status(in workers \[worker-x\]):" 1

    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml

    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status wrong_args_count more_than_one" \
        "query-status \[-w worker ...\] \[task_name\]" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status" \
        "\"result\": true" 3 \
        "\"msg\": \"no sub task started\"" 2

    # start DM task only
    dmctl_start_task

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status" \
        "\"result\": true" 3 \
        "\"unit\": \"Sync\"" 2 \
        "\"stage\": \"Running\"" 4

    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1
    run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2

    # TODO: check sharding partition id
    # use sync_diff_inspector to check data now!
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

cleanup1 dmctl
# also cleanup dm processes in case of last run failed
cleanup2 $*
run $*
cleanup2 $*

wait_process_exit dm-master.test
wait_process_exit dm-worker.test

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
