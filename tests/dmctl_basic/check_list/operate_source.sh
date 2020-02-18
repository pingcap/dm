#!/bin/bash

function operate_source_empty_arg() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "operate-source" \
        "operate-source <operate-type> <config-file> \[flags\]" 1
}

function operate_source_wrong_config_file() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "operate-source create not_exists_config_file" \
        "get file content error" 1
}

function operate_source_while_master_down() {
    task_conf=$1
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "operate-source create $task_conf" \
        "can not update task" 1
}

function operate_source_stop_not_created_config() {
    task_conf=$1
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "operate-source stop $task_conf" \
        "Stop Mysql-worker failed. worker has not been started" 1
}

