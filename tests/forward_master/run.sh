#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

function run() {
    export GO_FAILPOINTS="github.com/pingcap/dm/dm/worker/TaskCheckInterval=return(\"500ms\")"

    run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1
    check_contains 'Query OK, 2 rows affected'
    run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2
    check_contains 'Query OK, 3 rows affected'

    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

    run_dm_master $WORK_DIR/master1 $MASTER_PORT $cur/conf/dm-master1.toml
    run_dm_master $WORK_DIR/master2 $MASTER2_PORT $cur/conf/dm-master2.toml
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER2_PORT

    # start DM task only
    dmctl_start_task

    # request to leader master
    dmctl_operate_task test query-status
    # request to follower master
    dmctl_operate_task test query-status "127.0.0.1:$MASTER2_PORT"
    # test for with invalid address, this should success
    dmctl_operate_task test query-status "127.0.0.1:12345,127.0.0.1:$MASTER_PORT,127.0.0.1:$MASTER2_PORT"

    # use sync_diff_inspector to check full dump loader
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    kill -9 $(ps -ef | grep dm-master1 | grep -v grep | awk '{print $2}')
    sleep 8

    # master2 will be leader now, so this will success
    dmctl_stop_task test 127.0.0.1:$MASTER_PORT,127.0.0.1:$MASTER2_PORT

    run_dm_master $WORK_DIR/master1 $MASTER_PORT $cur/conf/dm-master1.toml
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT

    run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1
    run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2

    # use sync_diff_inspector to check data now!
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    check_metric $WORKER1_PORT 'dm_syncer_replication_lag{task="test"}' 3 0 1
    check_metric $WORKER2_PORT 'dm_syncer_replication_lag{task="test"}' 3 0 1

    export GO_FAILPOINTS=''
}

cleanup_data forward_master
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
