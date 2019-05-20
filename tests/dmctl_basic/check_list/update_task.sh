#!/bin/bash

function update_task_wrong_arg() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "update-task" \
        "update-task \[-w worker ...\] <config_file> \[flags\]" 1
}

function update_task_wrong_config_file() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "update-task not_exists_config_file" \
        "get file content error" 1
}

function update_task_while_master_down() {
    task_conf=$1
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "update-task $task_conf" \
        "can not update task:" 1
}

function update_task_worker_not_found() {
    task_conf=$1
    not_found_worker_addr=$2
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "update-task $task_conf -w $not_found_worker_addr " \
        "\"result\": true" 1 \
        "\"result\": false" 1 \
        "\"worker\": \"$not_found_worker_addr\"" 1 \
        "\"msg\": \"worker not found in task's config or deployment config\"" 1
}

function update_task_not_paused() {
    task_conf=$1
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "update-task $task_conf" \
        "\"result\": true" 1 \
        "\"result\": false" 2 \
        "can only update task on Paused stage, but current stage is Running" 2
}

function update_task_success_single_worker() {
    task_conf=$1
    worker_addr=$2
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "update-task $task_conf -w $worker_addr" \
        "\"result\": true" 2 \
        "\"worker\": \"$worker_addr\"" 1
}

function update_task_success() {
    task_conf=$1
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "update-task $task_conf" \
        "\"result\": true" 3
}
