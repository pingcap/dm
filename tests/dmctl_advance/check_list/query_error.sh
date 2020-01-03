#!/bin/bash

function query_error_wrong_arg() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-error wrong_args_count more_than_one" \
        "query-error \[-w worker ...\] \[task-name\]" 1
}

function query_error_while_master_down() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-error -w $MYSQL1_NAME test-task" \
        "query error failed" 1
}
