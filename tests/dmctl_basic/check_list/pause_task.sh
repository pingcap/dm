#!/bin/bash

function pause_task_wrong_arg() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "pause-task" \
        "pause-task \[-w worker ...\] <task-name> \[flags\]" 1
}

function pause_task_success() {
    task_name=$1
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "pause-task $task_name" \
        "\"result\": true" 3 \
        "\"op\": \"Pause\"" 3
}
