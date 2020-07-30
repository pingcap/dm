#!/bin/bash

function check_task_wrong_arg() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "check-task" \
        "check-task <config-file> \[flags\]" 1
}

function check_task_wrong_config_file() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "check-task not_exists_config_file" \
        "error in get file content" 1
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

function check_task_error_database_config() {
    task_conf=$1
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "check-task $task_conf" \
        "Access denied for user" 1 \
        "Please check the database config in configuration file" 1
}
