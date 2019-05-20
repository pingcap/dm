#!/bin/bash

function refresh_worker_tasks_wrong_arg() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "refresh-worker-tasks invalid_arg" \
        "refresh-worker-tasks \[flags\]" 1
}

function refresh_worker_tasks_while_master_down() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "refresh-worker-tasks" \
        "can not refresh workerTasks:" 1
}
