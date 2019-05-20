#!/bin/bash

function update_relay_wrong_arg() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "update-relay" \
        "update-relay \[-w worker ...\] <config_file> \[flags\]" 1
}

function update_relay_wrong_config_file() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "update-relay not_exists_config_file" \
        "get file content error" 1
}

function update_relay_should_specify_one_dm_worker() {
    task_conf=$1
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "update-relay $task_conf" \
        "must specify one dm-worker (\`-w\` \/ \`--worker\`)" 1
}

function update_relay_while_master_down() {
    task_conf=$1
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "update-relay $task_conf -w 127.0.0.1:$WORKER1_PORT" \
        "can not update relay config:" 1
}

function update_relay_success() {
    task_conf=$1
    worker_addr=$2
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "update-relay $task_conf -w $worker_addr" \
        "\"result\": true" 1
}
