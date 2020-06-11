#!/bin/bash

set -eu

shardddl="shardddl"
shardddl1="shardddl1"
shardddl2="shardddl2"
tb1="tb1"
tb2="tb2"
tb3="tb3"
tb4="tb4"
tb="tb"

function run_sql_source1() {
    run_sql "$1" $MYSQL_PORT1 $MYSQL_PASSWORD1
}

function run_sql_source2() {
    run_sql "$1" $MYSQL_PORT2 $MYSQL_PASSWORD2
}

function run_sql_tidb() {
    run_sql "$1" $TIDB_PORT $TIDB_PASSWORD
}

function run_sql_both_source() {
    run_sql_source1 "$1"
    run_sql_source2 "$1"
}

function init_cluster(){
    run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
    cp $cur/conf/source1.toml $WORK_DIR/source1.toml
    cp $cur/conf/source2.toml $WORK_DIR/source2.toml
    sed -i "/relay-binlog-name/i\relay-dir = \"$WORK_DIR/worker1/relay_log\"" $WORK_DIR/source1.toml
    sed -i "/relay-binlog-name/i\relay-dir = \"$WORK_DIR/worker2/relay_log\"" $WORK_DIR/source2.toml
    dmctl_operate_source create $WORK_DIR/source1.toml $SOURCE_ID1
    dmctl_operate_source create $WORK_DIR/source2.toml $SOURCE_ID2
}

function init_database() {
    run_sql_both_source "drop database if exists ${shardddl1};"
    run_sql_both_source "drop database if exists ${shardddl2};"
    run_sql_both_source "create database if not exists ${shardddl1};"
    run_sql_both_source "create database if not exists ${shardddl2};"
}

function extract() {
    str="$1"
    s=${str:0:1}
    d=${str:1:1}
    t=${str:2:1}
}

function init_table() {
    for i in $@; do
        extract $i
        run_sql_source${s} "create table shardddl${d}.tb${t} (id int);"
    done
}

function clean_table() {
    run_sql_both_source "drop table if exists ${shardddl1}.${tb1};"
    run_sql_both_source "drop table if exists ${shardddl1}.${tb2};"
    run_sql_both_source "drop table if exists ${shardddl2}.${tb1};"
    run_sql_both_source "drop table if exists ${shardddl2}.${tb2};"
    run_sql_tidb "drop table if exists ${shardddl}.${tb};"
    run_sql_tidb "drop database if exists dm_meta;"
}

function run_case() {
    case=$1
    task_conf=$2
    init_table_cmd=$3
    clean_table_cmd=$4
    shard_mode=$5

    echo "[$(date)] <<<<<< start DM-${case} ${shard_mode} >>>>>>"

    eval ${init_table_cmd}

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "start-task $cur/conf/${task_conf}.yaml --remove-meta"
    
    DM_${case}_CASE $5

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test"

    eval ${clean_table_cmd}

    echo "[$(date)] <<<<<< finish DM-${case} ${shard_mode} >>>>>>"
}

function run_sql_tidb_with_retry() {
    rc=0
    for ((k=1; k<11; k++)); do
        run_sql_tidb "$1"
        if grep -Fq "$2" "$TEST_DIR/sql_res.$TEST_NAME.txt"; then
            rc=1
            break
        fi
        echo "run tidb sql failed $k-th time, retry later"
        sleep 2
    done
    if [[ $rc = 0 ]]; then
        echo "TEST FAILED: OUTPUT DOES NOT CONTAIN '$2'"
        echo "____________________________________"
        cat "$TEST_DIR/sql_res.$TEST_NAME.txt"
        echo "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^"
        exit 1
    fi
}

function check_log_contain_with_retry() {
    text=$1
    log1=$2
    log2=""
    if [[ "$#" -ge 3 ]]; then
        log2=$3
    fi
    rc=0
    for ((k=1;k<11;k++)); do
        got=`grep "$text" $log1 | wc -l`
        if [[ ! $got = 0 ]]; then
            rc=1
            break
        fi
        if [[ ! "$log2" = "" ]]; then
            got=`grep "$text" $log2 | wc -l`
            if [[ ! $got = 0 ]]; then
                rc=1
                break
            fi
        fi
        echo "check log contain failed $k-th time, retry later"
        sleep 2
    done
    if [[ $rc = 0 ]]; then
        echo "log dosen't contain $text"
        exit 1
    fi
}