#!bin/bash

function break_ddl_lock_wrong_arg() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "break-ddl-lock" \
        "break-ddl-lock <-s source ...> <task-name> \[--remove-id\] \[--exec\] \[--skip\] \[flags\]" 1
}

function break_ddl_lock_without_worker() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "break-ddl-lock test" \
        "must specify at least one source (\`-s\` \/ \`--source\`)" 1
}

function break_ddl_lock_shoud_specify_at_least_one() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "break-ddl-lock test -s $SOURCE_ID1" \
        "\`remove-id\`, \`exec\`, \`skip\` must specify at least one" 1
}

function break_ddl_lock_exec_skip_conflict() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "break-ddl-lock test -s $SOURCE_ID1 --exec --skip" \
        "\`exec\` and \`skip\` can not specify both at the same time" 1
}

function break_ddl_lock_while_master_down() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "break-ddl-lock test -s $SOURCE_ID1 --exec" \
        "can not break DDL lock (in sources \[$SOURCE_ID1\]):" 1
}
