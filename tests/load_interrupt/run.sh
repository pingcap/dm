#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

COUNT=200
function prepare_datafile() {
    for i in $(seq 2); do
        data_file="$WORK_DIR/db$i.prepare.sql"
        echo 'DROP DATABASE if exists load_interrupt;' >> $data_file
        echo 'CREATE DATABASE load_interrupt;' >> $data_file
        echo 'USE load_interrupt;' >> $data_file
        echo "CREATE TABLE t$i(i TINYINT, j INT UNIQUE KEY);" >> $data_file
        for j in $(seq $COUNT); do
            echo "INSERT INTO t$i VALUES ($i,${j}000$i),($i,${j}001$i);" >> $data_file
        done
    done
}

function check_row_count() {
    index=$1
    lines=$(($(wc -l $WORK_DIR/db$index.prepare.sql|awk '{print $1}') - 4))
    # each line has two insert values
    lines=$((lines * 2))
    run_sql "SELECT FLOOR(offset / end_pos * $lines) as cnt from dm_meta.test_loader_checkpoint where cp_table = 't$index'" $TIDB_PORT
    estimate=$(tail -n 1 "$TEST_DIR/sql_res.$TEST_NAME.txt")
    run_sql "SELECT count(1) as cnt from $TEST_NAME.t$index" $TIDB_PORT
    row_count=$(tail -n 1 "$TEST_DIR/sql_res.$TEST_NAME.txt")
    echo "estimate row count: $estimate, real row count: $row_count"
    [ "$estimate" == "$row_count" ]
}

function run() {
    THRESHOLD=1024
    prepare_datafile

    run_sql_file $WORK_DIR/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1
    run_sql_file $WORK_DIR/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2

    export GO_FAILPOINTS="github.com/pingcap/dm/loader/LoadExceedOffsetExit=return($THRESHOLD)"

    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml

    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT

    $cur/../bin/dmctl_start_task "$cur/conf/dm-task.yaml"

    check_port_offline $WORKER1_PORT 20
    check_port_offline $WORKER2_PORT 20

    run_sql "SELECT count(*) from dm_meta.test_loader_checkpoint where cp_schema = '$TEST_NAME' and offset < $THRESHOLD" $TIDB_PORT
    check_contains "count(*): 2"
    check_row_count 1
    check_row_count 2

    export GO_FAILPOINTS=''
    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    run_sql "SELECT count(*) from dm_meta.test_loader_checkpoint where cp_schema = '$TEST_NAME' and offset = end_pos" $TIDB_PORT
    check_contains "count(*): 2"
}

cleanup1 load_interrupt
# also cleanup dm processes in case of last run failed
cleanup2 $*
run $*
cleanup2 $*

wait_process_exit dm-master.test
wait_process_exit dm-worker.test

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
