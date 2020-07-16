#!/bin/bash

function start_task_wrong_arg() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "start-task" \
        "start-task \[-w worker ...\] <config-file> \[flags\]" 1
}

function start_task_wrong_config_file() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "start-task not_exists_config_file" \
        "get file content error" 1
}
