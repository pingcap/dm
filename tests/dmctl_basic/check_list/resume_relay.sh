#!/bin/bash

: '
TODO
» resume-relay -w 172.17.0.6:8262 -w 172.17.0.2:8262
{
    "op": "InvalidRelayOp",
    "result": true,
    "msg": "",
    "workers": [
        {
            "op": "ResumeRelay",
            "result": true,
            "worker": "172.17.0.2:8262",
            "msg": ""
        },
        {
            "op": "ResumeRelay",
            "result": true,
            "worker": "172.17.0.6:8262",
            "msg": ""
        }
    ]
}
'

function resume_relay_wrong_arg() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "resume-relay wrong_arg" \
        "resume-relay <-w worker ...> \[flags\]" 1
}

function resume_relay_wihout_worker() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "resume-relay" \
        "must specify at least one dm-worker" 1
}

function resume_relay_while_master_down() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "resume-relay -w 127.0.0.1:$WORKER1_PORT -w 127.0.0.1:$WORKER2_PORT" \
        "can not resume relay unit:" 1
}

function resume_relay_success() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "resume-relay -w 127.0.0.1:$WORKER1_PORT -w 127.0.0.1:$WORKER2_PORT" \
        "\"result\": true" 3
}
