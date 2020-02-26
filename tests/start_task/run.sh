#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare

WORK_DIR=$TEST_DIR/$TEST_NAME

function prepare_data() {
    run_sql 'DROP DATABASE if exists start_task;' $MYSQL_PORT1 $MYSQL_PASSWORD1
    run_sql 'CREATE DATABASE start_task;' $MYSQL_PORT1 $MYSQL_PASSWORD1
    run_sql "CREATE TABLE start_task.t(i TINYINT, j INT UNIQUE KEY);" $MYSQL_PORT1 $MYSQL_PASSWORD1
    for j in $(seq 100); do
        run_sql "INSERT INTO start_task.t VALUES ($j,${j}000$j),($j,${j}001$j);" $MYSQL_PORT1 $MYSQL_PASSWORD1
    done
}

function run() {
    failpoints=(
        # 1152 is ErrAbortingConnection
        "github.com/pingcap/dm/pkg/utils/FetchTargetDoTablesFailed=return(1152)"
        "github.com/pingcap/dm/pkg/utils/FetchAllDoTablesFailed=return(1152)"
    )

    for(( i=0;i<${#failpoints[@]};i++)) do
        WORK_DIR=$TEST_DIR/$TEST_NAME/$i

        echo "failpoint=${failpoints[i]}"
        export GO_FAILPOINTS=${failpoints[i]}

        prepare_data

        run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
        check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
        run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
        check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
        # operate mysql config to worker
        cp $cur/conf/source1.toml $WORK_DIR/source1.toml
        sed -i "/relay-binlog-name/i\relay-dir = \"$WORK_DIR/worker1/relay_log\"" $WORK_DIR/source1.toml
        dmctl_operate_source create $WORK_DIR/source1.toml $SOURCE_ID1

        echo "check un-accessible DM-worker exists"
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status -s 127.0.0.1:8888" \
            "127.0.0.1:8888 relevant worker-client not found" 1

        echo "start task and will failed"
        task_conf="$cur/conf/dm-task.yaml"
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $task_conf" \
            "\"result\": false" 1 \
            "ERROR" 1

        echo "reset go failpoints, and need restart dm-worker, then start task again"
        kill_dm_master

        export GO_FAILPOINTS=''

        run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
        check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
        sleep 5

        dmctl_start_task_standalone $task_conf

        check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

        cleanup_process
    done
}

cleanup_data start_task

cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
