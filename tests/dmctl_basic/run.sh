#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
TASK_CONF=$cur/conf/dm-task.yaml
TASK_NAME="test"

# used to coverage wrong usage of dmctl command
function usage_and_arg_test() {
    check_task_wrong_arg
    check_task_wrong_config_file
    check_task_with_master_down $TASK_CONF

    pause_relay_wrong_arg
    pause_relay_wihout_worker
    pause_relay_while_master_down

    resume_relay_wrong_arg
    resume_relay_wihout_worker
    resume_relay_while_master_down

    pause_task_wrong_arg
    pause_task_while_master_down

    resume_task_wrong_arg
    resume_task_while_master_down

    query_status_wrong_arg
    query_status_wrong_params

    start_task_wrong_arg
    start_task_wrong_config_file
    start_task_while_master_down $TASK_CONF

    stop_task_wrong_arg
    stop_task_while_master_down
}

function run() {
    run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1
    run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2

    cd $cur
    for file in "check_list"/*; do
        source $file
    done
    cd -

    usage_and_arg_test

    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml

    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT

    pause_relay_success
    query_status_stopped_relay
    pause_relay_fail
    resume_relay_success
    query_status_with_no_tasks

    check_task_pass $TASK_CONF
    check_task_not_pass $cur/conf/dm-task2.yaml

    dmctl_start_task

    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    query_status_with_tasks
    pause_task_success $TASK_NAME

    run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1
    run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2

    resume_task_success $TASK_NAME

    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 20
}

cleanup1 dmctl
# also cleanup dm processes in case of last run failed
cleanup2 $*
run $*
cleanup2 $*

wait_process_exit dm-master.test
wait_process_exit dm-worker.test

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
