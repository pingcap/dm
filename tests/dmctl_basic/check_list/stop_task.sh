#!/bin/bash

function stop_task_wrong_arg() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "stop-task" \
        "stop-task \[-s source ...\] <task-name> \[flags\]" 1
}

function stop_task_while_master_down() {
    task_name="any_task_name"
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "stop-task $task_name" \
        "can not stop task $task_name:" 1
}
