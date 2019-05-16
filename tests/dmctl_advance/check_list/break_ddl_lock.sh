#!bin/bash

function break_ddl_lock_wrong_arg() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "break-ddl-lock" \
        "break-ddl-lock <-w worker ...> <task-name> \[--remove-id\] \[--exec\] \[--skip\] \[flags\]" 1
}

function break_ddl_lock_without_worker() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "break-ddl-lock test" \
        "must specify at least one dm-worker (\`-w\` \/ \`--worker\`)" 1
}

function break_ddl_lock_shoud_specify_at_least_one() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "break-ddl-lock test -w 127.0.0.1:$WORKER1_PORT" \
        "\`remove-id\`, \`exec\`, \`skip\` must specify at least one" 1
}

function break_ddl_lock_exec_skip_conflict() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "break-ddl-lock test -w 127.0.0.1:$WORKER1_PORT --exec --skip" \
        "\`exec\` and \`skip\` can not specify both at the same time" 1
}

function break_ddl_lock_with_master_down() {
    worker_addr=$1
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "break-ddl-lock test -w $worker_addr --exec" \
        "can not break DDL lock (in workers \[$worker_addr\]):" 1
}
