#!/bin/bash

function switch_relay_master_wrong_arg() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "switch-relay-master invalid_arg" \
        "switch-relay-master <-w worker ...> \[flags\]" 1
}

function switch_relay_master_without_worker() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "switch-relay-master" \
        "must specify at least one dm-worker (\`-w\` \/ \`--worker\`)" 1
}

function switch_relay_master_while_master_down() {
    # worker_addr's value makes no sense
    worker_addr="127.0.0.1:$WORKER1_PORT"
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "switch-relay-master -w $worker_addr" \
        "can not switch relay's master server (in workers \[$worker_addr\]):" 1
}
