#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
TASK_CONF=$cur/conf/dm-task.yaml
TASK_NAME="test"
WORKER1_CONF=$cur/conf/dm-worker1.toml
SQL_RESULT_FILE="$TEST_DIR/sql_res.$TEST_NAME.txt"

# used to coverage wrong usage of dmctl command
function usage_and_arg_test() {
    break_ddl_lock_wrong_arg
    break_ddl_lock_without_worker
    break_ddl_lock_shoud_specify_at_least_one
    break_ddl_lock_exec_skip_conflict
    break_ddl_lock_with_master_down 127.0.0.1:$WORKER1_PORT

    migrate_relay_wrong_arg
    migrate_relay_without_worker
    migrate_relay_with_master_down

    refresh_worker_tasks_wrong_arg
    refresh_worker_tasks_with_master_down

    switch_relay_master_wrong_arg
    switch_relay_master_without_worker
    switch_relay_master_with_master_down 127.0.0.1:$WORKER1_PORT

    unlock_ddl_lock_wrong_arg
    unlock_ddl_lock_invalid_force_remove
    unlock_ddl_lock_with_master_down

    query_error_wrong_arg
    query_error_while_master_down
}

function run() {
    cd $cur
    for file in "check_list"/*; do
        source $file
    done
    cd -

    usage_and_arg_test
}

cleanup1 dmctl_advance
# also cleanup dm processes in case of last run failed
cleanup2 $*
run $*
cleanup2 $*

wait_process_exit dm-master.test
wait_process_exit dm-worker.test

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
