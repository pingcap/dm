#!/bin/bash

function stop_task_wrong_arg() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "stop-task" \
        "stop-task \[-w worker ...\] <task-name | task-file> \[flags\]" 1
}
