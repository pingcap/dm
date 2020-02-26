#!/bin/bash

function pause_task_wrong_arg() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "pause-task" \
        "pause-task \[-s source ...\] <task-name> \[flags\]" 1
}

function pause_task_while_master_down() {
    task_name="any_task_name"
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "pause-task $task_name" \
        "can not pause task $task_name:" 1
}

function pause_task_success() {
    task_name=$1
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "pause-task $task_name" \
        "\"result\": true" 3 \
        "\"op\": \"Pause\"" 1 \
        "\"source\": \"$SOURCE_ID1\"" 1 \
        "\"source\": \"$SOURCE_ID2\"" 1
}
