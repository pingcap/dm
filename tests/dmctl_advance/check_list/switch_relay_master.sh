#!/bin/bash

function switch_relay_master_wrong_arg() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "switch-relay-master invalid_arg" \
        "switch-relay-master <-s source ...> \[flags\]" 1
}

function switch_relay_master_without_worker() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "switch-relay-master" \
        "must specify at least one source (\`-s\` \/ \`--source\`)" 1
}

function switch_relay_master_while_master_down() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "switch-relay-master -s $SOURCE_ID1" \
        "can not switch relay's master server (in sources \[$SOURCE_ID1\]):" 1
}
