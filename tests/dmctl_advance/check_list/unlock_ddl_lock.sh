#!/bin/bash

function unlock_ddl_lock_wrong_arg() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "unlock-ddl-lock" \
        "unlock-ddl-lock <lock-ID> \[flags\]" 1
}

function unlock_ddl_lock_invalid_force_remove() {
    force_remove_val="invalid-force-remove"
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "unlock-ddl-lock lock_id --force-remove=$force_remove_val" \
        "Error: invalid argument \"$force_remove_val\" for \"-f, --force-remove\"" 1
}
