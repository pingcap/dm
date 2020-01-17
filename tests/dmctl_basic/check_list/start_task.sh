#!/bin/bash

function start_task_wrong_arg() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "start-task" \
        "start-task \[-s source ...\] <config-file> \[flags\]" 1
}

function start_task_wrong_config_file() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "start-task not_exists_config_file" \
        "get file content error" 1
}

function start_task_while_master_down() {
    task_conf=$1
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "start-task $task_conf" \
        "can not start task:" 1
}
