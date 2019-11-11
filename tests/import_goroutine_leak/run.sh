#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

COUNT=200
function prepare_datafile() {
    for i in $(seq 2); do
        data_file="$WORK_DIR/db$i.prepare.sql"
        echo 'DROP DATABASE if exists import_goroutine_leak;' >> $data_file
        echo 'CREATE DATABASE import_goroutine_leak;' >> $data_file
        echo 'USE import_goroutine_leak;' >> $data_file
        echo "CREATE TABLE t$i(i TINYINT, j INT UNIQUE KEY);" >> $data_file
        for j in $(seq $COUNT); do
            echo "INSERT INTO t$i VALUES ($i,${j}000$i),($i,${j}001$i);" >> $data_file
        done
    done
}

function run() {
    prepare_datafile

    run_sql_file $WORK_DIR/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1
    run_sql_file $WORK_DIR/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2


    # check workers of import unit exit
    inject_points=("github.com/pingcap/dm/loader/dispatchError=return(1)"
                   "github.com/pingcap/dm/loader/LoadDataSlowDown=sleep(1000)"
                   "github.com/pingcap/dm/loader/executeSQLError=return(1)"
                   "github.com/pingcap/dm/loader/workerCantClose=return(1)"
                   )
    export GO_FAILPOINTS="$(join_string \; ${inject_points[@]})"

    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
    run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT

    dmctl_start_task

    check_port_offline $WORKER1_PORT 20
    check_port_offline $WORKER2_PORT 20

    # dm-worker1 panics
    err_cnt=`grep "panic" $WORK_DIR/worker1/log/stdout.log | wc -l`
    if [ $err_cnt -ne 1 ]; then
        echo "dm-worker1 doesn't panic, panic count ${err_cnt}"
        exit 2
    fi

     # check workers of import unit exit
    inject_points=("github.com/pingcap/dm/loader/dispatchError=return(1)"
                   "github.com/pingcap/dm/loader/LoadDataSlowDown=sleep(1000)"
                   "github.com/pingcap/dm/loader/executeSQLError=return(1)"
                   )
    export GO_FAILPOINTS="$(join_string \; ${inject_points[@]})"
    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

    export GO_FAILPOINTS=''
}

cleanup_data import_goroutine_leak
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
