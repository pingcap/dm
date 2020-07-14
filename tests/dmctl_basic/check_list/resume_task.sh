#!/bin/bash

function resume_task_wrong_arg() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "resume-task" \
        "resume-task \[-w worker ...\] <task-name> \[flags\]" 1
}

function resume_task_success() {
    task_name=$1
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "resume-task $task_name" \
        "\"result\": true" 3 \
        "\"op\": \"Resume\"" 3
}
