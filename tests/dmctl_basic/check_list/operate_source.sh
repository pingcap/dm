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
    source_conf=$1
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "operate-source create $source_conf" \
        "can not update task" 1
}

function operate_source_stop_not_created_config() {
    source_conf=$1
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "operate-source stop $source_conf" \
        "source config with ID mysql-replica-01 not exists" 1
}

