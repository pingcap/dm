#!/bin/bash

function show_ddl_locks_wrong_arg() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "show-ddl-locks a b" \
        "show-ddl-locks \[-s source ...\] \[task-name\] \[flags\]" 1
}

function show_ddl_locks_while_master_down() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "show-ddl-locks task-name -s $SOURCE_ID1" \
        "can not show DDL locks for task task-name and sources \[$SOURCE_ID1\]" 1
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
