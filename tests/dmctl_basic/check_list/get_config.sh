#!/bin/bash

function get_config_wrong_arg() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "get-config" \
        "get-config <task | master | worker | source> <name> \[--file filename\] \[flags\]" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "get-config hihi haha" \
        "invalid config type 'hihi'" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "get-config task haha" \
        "task not found" 1

    # test alias
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "get-task-config haha" \
        "task not found" 1
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "get-task-config haha hihi" \
        "get-config <task | master | worker | source> <name> \[--file filename\] \[flags\]" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "get-config master haha" \
        "master not found" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "get-config worker haha" \
        "worker not found" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "get-config source haha" \
        "source not found" 1
}

function diff_get_config() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "get-config task test --file $WORK_DIR/get_task.yaml" \
        "\"result\": true" 1
    diff $WORK_DIR/get_task.yaml $cur/conf/get_task.yaml || exit 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "get-config master master1 --file $dm_master_conf" \
        "\"result\": true" 1
    diff $dm_master_conf $cur/conf/get_master1.toml || exit 1

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "get-config worker worker1 --file $dm_worker1_conf" \
        "\"result\": true" 1
    diff $dm_worker1_conf $cur/conf/get_worker1.toml || exit 1

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "get-config worker worker2 --file $dm_worker2_conf" \
        "\"result\": true" 1
    diff $dm_worker2_conf $cur/conf/get_worker2.toml || exit 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "get-config source mysql-replica-01 --file $WORK_DIR/get_source1.yaml" \
        "\"result\": true" 1
    diff $WORK_DIR/get_source1.yaml $cur/conf/get_source1.yaml || exit 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "get-config source mysql-replica-02 --file $WORK_DIR/get_source2.yaml" \
        "\"result\": true" 1
    diff $WORK_DIR/get_source2.yaml $cur/conf/get_source2.yaml || exit 1
}

function get_config_to_file() {
    diff_get_config

    sed -i "s/password: '\*\*\*\*\*\*'/password: ''/g" $WORK_DIR/get_task.yaml
    sed -i "s/password: '\*\*\*\*\*\*'/password: '123456'/g" $WORK_DIR/get_source1.yaml
    sed -i "s/password: '\*\*\*\*\*\*'/password: '123456'/g" $WORK_DIR/get_source2.yaml

    # stop task
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "stop-task test" \
        "\"result\": true" 3

    # restart source with get config
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "operate-source stop mysql-replica-01" \
        "\"result\": true" 2
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "operate-source create $WORK_DIR/get_source1.yaml" \
        "\"result\": true" 2

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "operate-source stop mysql-replica-02" \
        "\"result\": true" 2
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "operate-source create $WORK_DIR/get_source2.yaml" \
        "\"result\": true" 2

    # start task with get config
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "start-task $WORK_DIR/get_task.yaml" \
        "\"result\": true" 3

    # restart master with get config
    ps aux | grep dm-master |awk '{print $2}'|xargs kill || true
    check_port_offline $MASTER_PORT1 20
    run_dm_master $WORK_DIR/master $MASTER_PORT $dm_master_conf
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT

    # restart worker with get config
    ps aux | grep worker1 |awk '{print $2}'|xargs kill || true
    check_port_offline $WORKER1_PORT 20
    ps aux | grep worker2 |awk '{print $2}'|xargs kill || true
    check_port_offline $WORKER2_PORT 20
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "list-member --worker" \
        "offline" 2

    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $dm_worker1_conf
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $dm_worker2_conf
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "list-member --worker" \
        "bound" 2

    diff_get_config
}
