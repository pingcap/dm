#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare

WORK_DIR=$TEST_DIR/$TEST_NAME

function prepare_data() {
    run_sql 'DROP DATABASE if exists initial_unit;' $MYSQL_PORT1 $MYSQL_PASSWORD1
    run_sql 'CREATE DATABASE initial_unit;' $MYSQL_PORT1 $MYSQL_PASSWORD1
    run_sql "CREATE TABLE initial_unit.t(i TINYINT, j INT UNIQUE KEY);" $MYSQL_PORT1 $MYSQL_PASSWORD1
    for j in $(seq 100); do
        run_sql "INSERT INTO initial_unit.t VALUES ($j,${j}000$j),($j,${j}001$j);" $MYSQL_PORT1 $MYSQL_PASSWORD1
    done
}

function run() {
    failpoints=(
                # 1152 is ErrAbortingConnection
                "github.com/pingcap/dm/syncer/LoadCheckpointFailed=return(1152)"
                "github.com/pingcap/dm/syncer/GetMasterStatusFailed=return(1152)"
    )

    for(( i=0;i<${#failpoints[@]};i++)) do
        WORK_DIR=$TEST_DIR/$TEST_NAME/$i

        prepare_data

        echo "failpoint=${failpoints[i]}"
        export GO_FAILPOINTS=${failpoints[i]}

        run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
        check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
        run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
        check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT

        echo "start task and query status, the sync unit will initial failed"
        task_conf="$cur/conf/dm-task.yaml"
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $task_conf" \
            "\"result\": true" 2

        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "\"stage\": \"Paused\"" 1 \
            "\"unit\": \"InvalidUnit\"" 1 \
            "fail to initial unit Sync of subtask test" 1

        echo "resume task will also initial failed"
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "resume-task test" \
            "\"result\": true" 2

        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "\"stage\": \"Paused\"" 1 \
            "\"unit\": \"InvalidUnit\"" 1 \
            "fail to initial unit Sync of subtask test" 1

        echo "reset go failpoints, and need restart dm-worker"
        echo "then resume task, task will recover success"
        export GO_FAILPOINTS=''

        kill_dm_worker

        run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
        check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
        sleep 2
    
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "resume-task test" \
            "\"result\": true" 2

        check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "resume-task test" \
            "\"result\": true" 1 \
            "\"result\": false" 1 \
            "current stage is not paused not valid" 1

        cleanup_process
        run_sql "drop database if exists initial_unit" $TIDB_PORT $TIDB_PASSWORD
        run_sql "drop database if exists dm_meta" $TIDB_PORT $TIDB_PASSWORD
    done
}

cleanup_data initial_unit
cleanup_process

run $*

cleanup_process

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
