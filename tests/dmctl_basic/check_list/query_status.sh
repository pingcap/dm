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
        "query-status" \
        "\"result\": true" 1 \
}

function query_status_with_tasks() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status" \
        "\"result\": true" 1 \
        "\"taskName\": \"test\"" 1 \
        "\"taskStatus\": \"Running\"" 1 \
        "\"workers\":" 1 \
        "\"127.0.0.1:$WORKER1_PORT\"" 1 \
        "\"127.0.0.1:$WORKER2_PORT\"" 1
}

function query_status_stopped_relay() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status" \
        "\"result\": true" 1 \
        "\"taskName\": \"test\"" 1 \
        "\"taskStatus\": \"Paused\"" 1 \
        "\"workers\":" 1 \
        "\"127.0.0.1:$WORKER1_PORT\"" 1 \
        "\"127.0.0.1:$WORKER2_PORT\"" 1
}
