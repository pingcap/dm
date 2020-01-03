#!/bin/bash

function query_status_wrong_arg() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status wrong_args_count more_than_one" \
        "query-status \[-w worker ...\] \[task-name\]" 1
}

function query_status_wrong_params() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status -w worker-x task-y" \
        "can not query task-y task's status(in workers \[worker-x\]):" 1
}

function query_status_with_no_tasks() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status -w $MYSQL1_NAME,$MYSQL2_NAME" \
        "\"result\": true" 3 \
        "\"msg\": \"no sub task started\"" 2
}

function query_status_with_tasks() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status -w $MYSQL1_NAME,$MYSQL2_NAME" \
        "\"result\": true" 3 \
        "\"unit\": \"Sync\"" 2 \
        "\"stage\": \"Running\"" 4
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status" \
        "\"result\": true" 1 \
        "\"taskName\": \"test\"" 1 \
        "\"taskStatus\": \"Running\"" 1 \
        "\"workers\":" 1 \
        "\"$MYSQL1_NAME\"" 1 \
        "\"$MYSQL2_NAME\"" 1
}

function query_status_stopped_relay() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status -w $MYSQL1_NAME,$MYSQL2_NAME" \
        "\"result\": true" 3 \
        "\"stage\": \"Paused\"" 2
}
