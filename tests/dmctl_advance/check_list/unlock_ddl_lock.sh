#!/bin/bash

function unlock_ddl_lock_wrong_arg() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "unlock-ddl-lock" \
        "unlock-ddl-lock \[-s source ...\] <lock-ID> \[flags\]" 1
}

function unlock_ddl_lock_invalid_force_remove() {
    force_remove_val="invalid-force-remove"
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "unlock-ddl-lock lock_id --force-remove=$force_remove_val" \
        "Error: invalid argument \"$force_remove_val\" for \"-f, --force-remove\"" 1
}
function unlock_ddl_lock_while_master_down() {
    lock_id="test-\`shard_db\`.\`shard_table\`"
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "unlock-ddl-lock $lock_id" \
        "can not unlock DDL lock $lock_id (in sources \[\]):" 1
}
