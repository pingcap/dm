#!/bin/bash

function purge_relay_wrong_arg() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "purge-relay wrong_arg" \
        "purge-relay <-w worker> \[--filename\] \[--sub-dir\] \[flags\]" 1
}

function purge_relay_wihout_worker() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "purge-relay" \
        "must specify at least one dm-worker (\`-w\` \/ \`--worker\`)" 1
}

function purge_relay_filename_with_multi_workers() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "purge-relay --filename bin-000001 -w 127.0.0.1:$WORKER1_PORT -w 127.0.0.1:$WORKER2_PORT" \
        "for --filename, can only specify one dm-worker per time" 1
}

function purge_relay_while_master_down() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "purge-relay --filename bin-000001 -w 127.0.0.1:$WORKER1_PORT" \
        "can not purge relay log files:" 1
}

function purge_relay_success() {
    binlog_file=$1
    worker_addr=$2
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "purge-relay --filename $binlog_file -w $worker_addr" \
        "\"result\": true" 2
}
