#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare

WORK_DIR=$TEST_DIR/$TEST_NAME

function prepare_data1() {
    run_sql 'DROP DATABASE if exists relay_interrupt;' $MYSQL_PORT1
    run_sql 'CREATE DATABASE relay_interrupt;' $MYSQL_PORT1
    run_sql "CREATE TABLE relay_interrupt.t(i TINYINT, j INT UNIQUE KEY);" $MYSQL_PORT1
    for j in $(seq 100); do
        run_sql "INSERT INTO relay_interrupt.t VALUES ($j,${j}000$j),($j,${j}001$j);" $MYSQL_PORT1
    done
}

function prepare_data2() {
    run_sql "DELETE FROM relay_interrupt.t limit 1;" $MYSQL_PORT1
}

function run() {
    failpoints=(
        # 1152 is ErrAbortingConnection
        "github.com/pingcap/dm/pkg/utils/GetGlobalVariableFailed=return(\"server_uuid,1152\")"
        "github.com/pingcap/dm/pkg/utils/GetGlobalVariableFailed=return(\"sql_mode,1152\")"
    )

    for(( i=0;i<${#failpoints[@]};i++)) do
        WORK_DIR=$TEST_DIR/$TEST_NAME/$i

        echo "failpoint=${failpoints[i]}"
        export GO_FAILPOINTS=${failpoints[i]}

        prepare_data1

        run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
        check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
        run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
        check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT

        echo "query status, relay log failed"
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status -w 127.0.0.1:$WORKER1_PORT" \
            "no sub task started" 1 \
            "ERROR" 1

        echo "start task and query status, task have error message"
        task_conf="$cur/conf/dm-task.yaml"
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $task_conf" \
            "\"result\": true" 2

        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status -w 127.0.0.1:$WORKER1_PORT" \
            "no valid relay sub directory exists" 1 \
            "ERROR" 1
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status" \
            "\"taskName\": \"test\"" 1 \
            "\"taskStatus\": \"Error - Some error occurred in subtask. Please run \`query-status test\` to get more details.\"" 1

        echo "reset go failpoints, and need restart dm-worker"
        echo "then resume task, task will recover success"
        kill_dm_worker
        export GO_FAILPOINTS=''

        run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
        check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

        sleep 2

        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "resume-task test" \
            "\"result\": true" 2

        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "\"binlogType\": \"local\"" 1

        check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

        prepare_data2
        echo "read binlog from relay log failed, and will use remote binlog"
        kill_dm_worker
        export GO_FAILPOINTS="github.com/pingcap/dm/pkg/streamer/GetEventFromLocalFailed=return()"
        run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
        check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "\"binlogType\": \"remote\"" 1

        check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

        export GO_FAILPOINTS=''
        cleanup_process
    done
}

cleanup_data relay_interrupt

run $*

cleanup_process

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
