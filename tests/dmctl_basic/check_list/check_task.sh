#!/bin/bash

function check_task_wrong_arg() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "check-task" \
        "check-task <config-file> \[flags\]" 1
}

function check_task_wrong_config_file() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "check-task not_exists_config_file" \
        "get file content error" 1
}

# run this check if DM-master is not available
function check_task_while_master_down() {
    task_conf=$1
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "check-task $task_conf" \
        "fail to check task" 1
}

function check_task_pass() {
    task_conf=$1
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "check-task $task_conf" \
        "\"msg\": \"check pass!!!\"" 1 \
        "\"result\": true" 1
}

function check_task_not_pass() {
    task_conf=$1
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "check-task $task_conf" \
        "\"result\": false" 1
}
