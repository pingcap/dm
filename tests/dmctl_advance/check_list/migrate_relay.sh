#!/bin/bash

function migrate_relay_wrong_arg() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "migrate-relay" \
        "migrate-relay <worker> <binlogName> <binlogPos> \[flags\]" 1
}

function migrate_relay_without_worker() {
    binlog_pos="invalid-binlog-pos"
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "migrate-relay 127.0.0.1:$WORKER1_PORT bin-000001 $binlog_pos" \
        "strconv.Atoi: parsing \"$binlog_pos\": invalid syntax" 1
}

function migrate_relay_while_master_down() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "migrate-relay 127.0.0.1:$WORKER1_PORT bin-000001 194" \
        "can not migrate relay config:" 1
}
