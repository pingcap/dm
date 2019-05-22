#!/bin/bash

function show_ddl_locks_wrong_arg() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "show-ddl-locks a b" \
        "show-ddl-locks \[-w worker ...\] \[task_name\] \[flags\]" 1
}

function show_ddl_locks_while_master_down() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "show-ddl-locks task_name -w 127.0.0.1:8262" \
        "can not show DDL locks for task task_name and workers \[127.0.0.1:8262\]" 1
}

function show_ddl_locks_no_locks() {
    task_name=$1
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "show-ddl-locks $task_name" \
        "\"msg\": \"no DDL lock exists\"" 1
}

function show_ddl_locks_with_locks() {
    lock_id=$1
    ddl=$2
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "show-ddl-locks" \
        "\"ID\": \"$lock_id\"" 1 \
        "$ddl" 1
}
