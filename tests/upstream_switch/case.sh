#!/bin/bash

set -eu

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
PATH=$CUR/../_utils:$PATH # for sync_diff_inspector

source $CUR/lib.sh

function clean_data() {
    echo "-------clean_data--------"

    exec_sql $master_57_host "stop slave;"
    exec_sql $slave_57_host "stop slave;"
    exec_sql $master_8_host "stop slave;"
    exec_sql $slave_8_host "stop slave;"

    exec_sql $master_57_host "drop database if exists db1;"
    exec_sql $master_57_host "drop database if exists db2;"
    exec_sql $master_57_host "drop database if exists ${db};"
    exec_sql $master_57_host "reset master;"
    exec_sql $slave_57_host "drop database if exists db1;"
    exec_sql $slave_57_host "drop database if exists db2;"
    exec_sql $slave_57_host "drop database if exists ${db};"
    exec_sql $slave_57_host "reset master;"
    exec_sql $master_8_host "drop database if exists db1;"
    exec_sql $master_8_host "drop database if exists db2;"
    exec_sql $master_8_host "drop database if exists ${db};"
    exec_sql $master_8_host "reset master;"
    exec_sql $slave_8_host "drop database if exists db1;"
    exec_sql $slave_8_host "drop database if exists db2;"
    exec_sql $slave_8_host "drop database if exists ${db};"
    exec_sql $slave_8_host "reset master;"
    exec_tidb $tidb_host "drop database if exists db1;"
    exec_tidb $tidb_host "drop database if exists db2;"
    exec_tidb $tidb_host "drop database if exists ${db};"
}

function prepare_binlogs() {
    echo "-------prepare_binlogs--------"

    prepare_more_binlogs $master_57_host
    prepare_less_binlogs $slave_57_host
    prepare_less_binlogs $master_8_host
    prepare_more_binlogs $slave_8_host
}

function setup_replica() {
    echo "-------setup_replica--------"

    master_57_status=($(get_master_status $master_57_host))
    slave_57_status=($(get_master_status $slave_57_host))
    master_8_status=($(get_master_status $master_8_host))
    slave_8_status=($(get_master_status $slave_8_host))
    echo "master_57_status" ${master_57_status[@]}
    echo "slave_57_status" ${slave_57_status[@]}
    echo "master_8_status" ${master_8_status[@]}
    echo "slave_8_status" ${slave_8_status[@]}

    # master <-- slave
    change_master_to_pos $slave_57_host $master_57_host ${master_57_status[0]} ${master_57_status[1]}

    # master <--> master
    change_master_to_gtid $slave_8_host $master_8_host
    change_master_to_gtid $master_8_host $slave_8_host
}

function run_dm_components() {
    echo "-------run_dm_components--------"

    pkill -9 dm-master || true
    pkill -9 dm-worker || true

    run_dm_master $WORK_DIR/master $MASTER_PORT $CUR/conf/dm-master.toml
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "list-member" \
        "alive" 1

    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $CUR/conf/dm-worker1.toml
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $CUR/conf/dm-worker2.toml
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "list-member" \
        "alive" 1 \
        "free" 2
}

function create_sources() {
    echo "-------create_sources--------"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "operate-source create $CUR/conf/source1.yaml" \
        "\"result\": true" 2
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "operate-source create $CUR/conf/source2.yaml" \
        "\"result\": true" 2
}

function gen_full_data() {
    echo "-------gen_full_data--------"

    exec_sql $host1 "create database ${db};"
    exec_sql $host1 "create table ${db}.${tb}(id int primary key, a int);"
    for i in $(seq 1 1000); do
        exec_sql $host1 "insert into ${db}.${tb} values($i,$i);"
    done

    exec_sql $host2 "create database ${db};"
    exec_sql $host2 "create table ${db}.${tb}(id int primary key, a int);"
    for i in $(seq 1001 2000); do
        exec_sql $host2 "insert into ${db}.${tb} values($i,$i);"
    done
}


function gen_incr_data() {
    echo "-------gen_incr_data--------"

    for i in $(seq 2001 2500); do
        exec_sql $host1 "insert into ${db}.${tb} values($i,$i);"
    done
    for i in $(seq 2501 3000); do
        exec_sql $host2 "insert into ${db}.${tb} values($i,$i);"
    done
    exec_sql $host1 "alter table ${db}.${tb} add column b int;"
    exec_sql $host2 "alter table ${db}.${tb} add column b int;"
    for i in $(seq 3001 3500); do
        exec_sql $host1 "insert into ${db}.${tb} values($i,$i,$i);"
    done
    for i in $(seq 3501 4000); do
        exec_sql $host2 "insert into ${db}.${tb} values($i,$i,$i);"
    done

    docker-compose -f $CUR/docker-compose.yml pause mysql57_master
    docker-compose -f $CUR/docker-compose.yml pause mysql8_master
    wait_mysql $host1 2
    wait_mysql $host2 2

    for i in $(seq 4001 4500); do
        exec_sql $host1 "insert into ${db}.${tb} values($i,$i,$i);"
    done
    for i in $(seq 4501 5000); do
        exec_sql $host2 "insert into ${db}.${tb} values($i,$i,$i);"
    done
    exec_sql $host1 "alter table ${db}.${tb} add column c int;"
    exec_sql $host2 "alter table ${db}.${tb} add column c int;"

    docker-compose -f $CUR/docker-compose.yml unpause mysql8_master
    wait_mysql $host2 1

    for i in $(seq 5001 5500); do
        exec_sql $host1 "insert into ${db}.${tb} values($i,$i,$i,$i);"
    done
    for i in $(seq 5501 6000); do
        exec_sql $host2 "insert into ${db}.${tb} values($i,$i,$i,$i);"
    done
}

function start_task() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "start-task $CUR/conf/task-pessimistic.yaml --remove-meta" \
        "\result\": true" 3
}

function verify_result() {
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status task_pessimistic -s mysql-replica-02" \
        "relaySubDir.*000003" 1
}

function clean_task() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "stop-task task_pessimistic" \
        "\result\": true" 3
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "pause-relay -s mysql-replica-02" \
        "\result\": true" 2
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "operate-source stop mysql-replica-01" \
        "\result\": true" 2
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "operate-source stop mysql-replica-02" \
        "\result\": true" 2
    docker-compose -f $CUR/docker-compose.yml unpause mysql57_master
}

function check_master() {
    wait_mysql $host1 1
    wait_mysql $host2 1
}

function test() {
    check_master
    install_sync_diff
    clean_data
    prepare_binlogs
    setup_replica
    gen_full_data
    run_dm_components
    create_sources
    start_task
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
    gen_incr_data
    verify_result
    clean_task
}

test
