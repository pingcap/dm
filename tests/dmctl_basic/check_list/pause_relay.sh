#!/bin/bash

function pause_relay_wrong_arg() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "pause-relay wrong_arg" \
        "pause-relay <-w worker ...> \[flags\]" 1
}

function pause_relay_wihout_worker() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "pause-relay" \
        "must specify at least one DM-worker" 1
}

function pause_relay_while_master_down() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "pause-relay -w $MYSQL1_NAME -w $MYSQL2_NAME" \
        "can not pause relay unit:" 1
}

function pause_relay_success() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "pause-relay -w $MYSQL1_NAME -w $MYSQL2_NAME" \
        "\"result\": true" 3
}

function pause_relay_fail() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "pause-relay -w $MYSQL1_NAME -w $MYSQL2_NAME" \
        "\"result\": true" 1 \
        "\"result\": false" 2 \
        "\"msg\": \".*current stage is Paused, Running required" 2
}
