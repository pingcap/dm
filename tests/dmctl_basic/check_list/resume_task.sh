#!/bin/bash

function resume_task_wrong_arg() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "resume-task" \
        "resume-task \[-s source ...\] <task-name> \[flags\]" 1
}

function resume_task_while_master_down() {
    task_name="any_task_name"
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "resume-task $task_name" \
        "can not resume task $task_name:" 1
}

function resume_task_success() {
    task_name=$1
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "resume-task $task_name" \
        "\"result\": true" 3 \
        "\"op\": \"Resume\"" 1 \
        "\"source\": \"$SOURCE_ID1\"" 1 \
        "\"source\": \"$SOURCE_ID2\"" 1
}
