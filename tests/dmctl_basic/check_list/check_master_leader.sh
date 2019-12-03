#!/bin/bash

function show_master_leader_success() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "show-master-leader" \
        "\"result\": true" 1
}