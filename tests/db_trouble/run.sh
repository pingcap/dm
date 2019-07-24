#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare
source $cur/checkpoint_test
source $cur/start_task_test
source $cur/relay_test

WORK_DIR=$TEST_DIR/$TEST_NAME

function prepare_data() {
    run_sql 'DROP DATABASE if exists db_trouble;' $MYSQL_PORT1
    run_sql 'CREATE DATABASE db_trouble;' $MYSQL_PORT1
    run_sql "CREATE TABLE db_trouble.t(i TINYINT, j INT UNIQUE KEY);" $MYSQL_PORT1
    for j in $(seq 100); do
        run_sql "INSERT INTO db_trouble.t VALUES ($j,${j}000$j),($j,${j}001$j);" $MYSQL_PORT1
    done
}

function run() {
    #run_checkpoint_test

    #clean

    #run_start_task_test

    run_relay_test
}

function run2() {
    #failpoints=("github.com/pingcap/dm/pkg/utils/FetchAllDoTablesFailed=return(true)"  start task
    #            "github.com/pingcap/dm/pkg/utils/FetchTargetDoTablesFailed=return(true)" start task
    #            "github.com/pingcap/dm/pkg/utils/GetMasterStatusFailed=return(true)"
    #            "github.com/pingcap/dm/pkg/utils/GetGlobalVariableFailed=return('server_id')"  mariadb
    #            "github.com/pingcap/dm/pkg/utils/GetGlobalVariableFailed=return('gtid_domain_id')" mariadb
    #            "github.com/pingcap/dm/pkg/utils/GetGlobalVariableFailed=return('server_uuid')" relay failed
    #            "github.com/pingcap/dm/pkg/utils/GetGlobalVariableFailed=return('sql_mode')" relay failed
    #            "github.com/pingcap/dm/pkg/utils/GetGlobalVariableFailed=return('gtid_binlog_pos')" mariadb
    #            "github.com/pingcap/dm/syncer/LoadCheckpointFailed=return(true)"  dm-worker init
    #)

    failpoints=(
                "github.com/pingcap/dm/syncer/LoadCheckpointFailed=return(true)"
    )

    for(( i=0;i<${#failpoints[@]};i++)) do
        prepare_data

        echo "failpoint=${failpoints[i]}"
        export GO_FAILPOINTS=${failpoints[i]}
        run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
        echo "check worker 127.0.0.1:$WORKER1_PORT online"
        check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
        run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
        echo "check master 127.0.0.1:$MASTER_PORT"
        check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT

        dmctl_start_task
        sleep 1
        dmctl_pause_task "test"
        dmctl_resume_task "test"

        check_port_offline $WORKER1_PORT 20

        export GO_FAILPOINTS=''
        run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
        check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
        check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    done
}

function clean() {
    cleanup1 db_trouble
    # also cleanup dm processes in case of last run failed
    cleanup2 $*

    wait_process_exit dm-master.test
    wait_process_exit dm-worker.test
}

clean

run $*

#clean



echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
