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
    break_ddl_lock_while_master_down

    migrate_relay_wrong_arg
    migrate_relay_without_worker
    # todo: no sense to fix
    #migrate_relay_while_master_down

    switch_relay_master_wrong_arg
    switch_relay_master_without_worker
    switch_relay_master_while_master_down

    unlock_ddl_lock_wrong_arg
    unlock_ddl_lock_invalid_force_remove
    unlock_ddl_lock_while_master_down

    query_error_wrong_arg
    query_error_while_master_down

    sql_skip_wrong_arg
    sql_skip_binlogpos_sqlpattern_conflict
    sql_skip_invalid_binlog_pos
    sql_skip_invalid_regex
    sql_skip_sharding_with_binlogpos
    sql_skip_non_sharding_without_one_worker
    sql_skip_while_master_down

    sql_replace_wrong_arg
    sql_replace_invalid_binlog_pos
    sql_replace_non_sharding_without_one_worker
    # TODO: check SQLs error test
    sql_replace_while_master_down
}

function run() {
    cd $cur
    for file in "check_list"/*; do
        source $file
    done
    cd -

    usage_and_arg_test
}

cleanup_data dmctl_advance
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
