#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

function run() {
    run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1
    check_contains 'Query OK, 2 rows affected'
    run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2
    check_contains 'Query OK, 3 rows affected'

    export GO_FAILPOINTS='github.com/pingcap/dm/syncer/ReSyncExit=return(true)'
    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
    run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
    run_dm_tracer $WORK_DIR/tracer $TRACER_PORT $cur/conf/dm-tracer.toml
    check_port_alive $TRACER_PORT

    dmctl_start_task
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    # DM-worker exit during re-sync after sharding group synced
    run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1
    run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2

    check_port_offline $WORKER1_PORT 20
    check_port_offline $WORKER2_PORT 20

    export GO_FAILPOINTS='github.com/pingcap/dm/syncer/ShardSyncedExecutionExit=return(true);github.com/pingcap/dm/syncer/SafeModeInitPhaseSeconds=return(0)'

    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    # DM-worker exit when waiting for sharding group synced
    run_sql_file $cur/data/db1.increment2.sql $MYSQL_HOST1 $MYSQL_PORT1
    run_sql_file $cur/data/db2.increment2.sql $MYSQL_HOST2 $MYSQL_PORT2

    i=0
    while [ $i -lt 10 ]; do
        # we can't determine which DM-worker is the sharding lock owner, so we try both of them
        # DM-worker1 is sharding lock owner and exits
        if [ "$(check_port_return $WORKER1_PORT)" == "0" ]; then
            echo "DM-worker1 is sharding lock owner and detects it offline"
            export GO_FAILPOINTS='github.com/pingcap/dm/syncer/SafeModeInitPhaseSeconds=return(0)'
            run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
            check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
            break
        fi
        # DM-worker2 is sharding lock owner and exits
        if [ "$(check_port_return $WORKER2_PORT)" == "0" ]; then
            echo "DM-worker2 is sharding lock owner and detects it offline"
            export GO_FAILPOINTS='github.com/pingcap/dm/syncer/SafeModeInitPhaseSeconds=return(0)'
            run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
            check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
            break
        fi

        ((i+=1))
        echo "wait for one of DM-worker offine failed, retry later" && sleep 1
    done
    if [ $i -ge 10 ]; then
        echo "wait DM-worker offline timeout"
        exit 1
    fi

    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    cd $cur && GO111MODULE=on go build -o bin/check_safe_mode check_safe_mode.go && cd -
    $cur/bin/check_safe_mode
}

cleanup1 safe_mode_target
# also cleanup dm processes in case of last run failed
cleanup2 $*
run $*
cleanup2 $*

wait_process_exit dm-master.test
wait_process_exit dm-worker.test
wait_process_exit dm-tracer.test

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
