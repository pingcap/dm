#!bin/bash

function break_ddl_lock_wrong_arg() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "break-ddl-lock" \
        "break-ddl-lock <-w worker ...> <task-name> \[--remove-id\] \[--exec\] \[--skip\] \[flags\]" 1
}

function break_ddl_lock_without_worker() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "break-ddl-lock test" \
        "must specify at least one DM-worker (\`-w\` \/ \`--worker\`)" 1
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

function break_ddl_lock_while_master_down() {
    # worker address is only a placeholder in this test case, its value makes no sense
    worker_addr="127.0.0.1:$WORKER1_PORT"
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "break-ddl-lock test -w $worker_addr --exec" \
        "Error while dialing dial tcp" 1
}
