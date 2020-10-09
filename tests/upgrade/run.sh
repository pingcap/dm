#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare

WORK_DIR=$TEST_DIR/$TEST_NAME

function run() {
    run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
    check_contains 'Query OK, 2 rows affected'

    # run last release version binary
    run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml previous
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml previous
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    dmctl_operate_source create $cur/conf/source1.yaml $SOURCE_ID1

    dmctl_start_task "$cur/conf/dm-task.yaml" "--remove-meta"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    # kill worker and master, then run this PR version binary
    pkill dm-worker.test
    wait_process_exit dm-worker.test
    pkill dm-master.test
    wait_process_exit dm-master.test

    run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml current
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml current
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status test" \
        "\"stage\": \"Running\"" 2
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

cleanup_data upgrade
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"