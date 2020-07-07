#!/bin/bash

function query_error_wrong_arg() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-error wrong_args_count more_than_one" \
        "query-error \[-s source ...\] \[task-name\]" 1
}

