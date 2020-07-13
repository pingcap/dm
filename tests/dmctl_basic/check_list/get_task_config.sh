#!/bin/bash

function get_task_config_wrong_name() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "get-task-config haha" \
        "task not found" 1
}

function get_task_config_to_file() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "get-task-config test --file $WORK_DIR/test.yaml" \
        "\"result\": true" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "stop-task test" \
        "\"result\": true" 3

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "get-task-config test --file $WORK_DIR/test.yaml" \
        "task not found" 1
    
    # start task with file
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "start-task $WORK_DIR/test.yaml" \
        "\"result\": true" 3
}

function get_task_config_recover_etcd() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "get-task-config test --file $WORK_DIR/test1.yaml" \
        "\"result\": true" 1

    ps aux | grep dm-master |awk '{print $2}'|xargs kill || true
    check_port_offline $MASTER_PORT1 20

    run_dm_master $WORK_DIR/master $MASTER_PORT $dm_master_conf
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "get-task-config test --file $WORK_DIR/test2.yaml" \
        "\"result\": true" 1

    diff $WORK_DIR/test1.yaml $WORK_DIR/test2.yaml || exit 1
}
