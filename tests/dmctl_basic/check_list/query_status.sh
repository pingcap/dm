#!/bin/bash

function query_status_wrong_arg() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status wrong_args_count more_than_one" \
        "query-status \[-s source ...\] \[task-name\]" 1
}

function query_status_wrong_params() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status -s source-x task-y" \
        "can not query task-y task's status(in sources \[source-x\]):" 1
}

function query_status_with_no_tasks() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status -s $SOURCE_ID1,$SOURCE_ID2" \
        "\"result\": true" 3 \
        "\"msg\": \"no sub task started\"" 2
}

function query_status_with_tasks() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status -s $SOURCE_ID1,$SOURCE_ID2" \
        "\"result\": true" 3 \
        "\"unit\": \"Sync\"" 2 \
        "\"stage\": \"Running\"" 4
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status" \
        "\"result\": true" 1 \
        "\"taskName\": \"test\"" 1 \
        "\"taskStatus\": \"Running\"" 1 \
        "\"sources\":" 1 \
        "\"$SOURCE_ID1\"" 1 \
        "\"$SOURCE_ID2\"" 1
}

function query_status_stopped_relay() {
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status -s $SOURCE_ID1,$SOURCE_ID2" \
        "\"result\": true" 3 \
        "\"stage\": \"Paused\"" 2
}

function query_status_paused_tasks() {
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status -s $SOURCE_ID1,$SOURCE_ID2" \
        "\"result\": true" 3 \
        "\"stage\": \"Paused\"" 2
}

function query_status_running_tasks() {
    # Running is 4 (including relay)
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status -s $SOURCE_ID1,$SOURCE_ID2" \
        "\"result\": true" 3 \
        "\"stage\": \"Running\"" 4
}
