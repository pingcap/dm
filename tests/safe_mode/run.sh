#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

function run() {
    run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
    check_contains 'Query OK, 2 rows affected'
    run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
    check_contains 'Query OK, 3 rows affected'

    export GO_FAILPOINTS='github.com/pingcap/dm/syncer/ReSyncExit=return(true)'
    run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
    # operate mysql config to worker
    cp $cur/conf/source1.toml $WORK_DIR/source1.toml
    cp $cur/conf/source2.toml $WORK_DIR/source2.toml
    sed -i "/relay-binlog-name/i\relay-dir = \"$WORK_DIR/worker1/relay_log\"" $WORK_DIR/source1.toml
    sed -i "/relay-binlog-name/i\relay-dir = \"$WORK_DIR/worker2/relay_log\"" $WORK_DIR/source2.toml
    dmctl_operate_source create $WORK_DIR/source1.toml $SOURCE_ID1
    dmctl_operate_source create $WORK_DIR/source2.toml $SOURCE_ID2

    dmctl_start_task
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    # DM-worker exit during re-sync after sharding group synced
    run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
    run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

    check_port_offline $WORKER1_PORT 20
    check_port_offline $WORKER2_PORT 20

    export GO_FAILPOINTS='github.com/pingcap/dm/syncer/ShardSyncedExecutionExit=return(true);github.com/pingcap/dm/syncer/SafeModeInitPhaseSeconds=return(300)'

    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

    sleep 5
    echo "check sync diff after set SafeModeInitPhaseSeconds failpoint"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    # DM-worker exit when waiting for sharding group synced
    run_sql_file $cur/data/db1.increment2.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
    run_sql_file $cur/data/db2.increment2.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

    OWNER_PORT=""
    i=0
    while [ $i -lt 10 ]; do
        # we can't determine which DM-worker is the sharding lock owner, so we try both of them
        # DM-worker1 is sharding lock owner and exits
        if [ "$(check_port_return $WORKER1_PORT)" == "0" ]; then
            echo "DM-worker1 is sharding lock owner and detects it offline"
            export GO_FAILPOINTS='github.com/pingcap/dm/syncer/SafeModeInitPhaseSeconds=return(0)'
            run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
            check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
            check_instance_id="1"
            OWNER_PORT=$WORKER1_PORT
            break
        fi
        # DM-worker2 is sharding lock owner and exits
        if [ "$(check_port_return $WORKER2_PORT)" == "0" ]; then
            echo "DM-worker2 is sharding lock owner and detects it offline"
            export GO_FAILPOINTS='github.com/pingcap/dm/syncer/SafeModeInitPhaseSeconds=return(0)'
            run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
            check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
            check_instance_id="2"
            OWNER_PORT=$WORKER2_PORT
            break
        fi

        ((i+=1))
        echo "wait for one of DM-worker offine failed, retry later" && sleep 1
    done
    if [ $i -ge 10 ]; then
        echo "wait DM-worker offline timeout"
        exit 1
    fi

    sleep 5
    echo "check sync diff after restart DDL owner"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

cleanup_data safe_mode_target
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
