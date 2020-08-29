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

    echo "use previous dm-master and dm-worker"
    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml previous
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT 
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml previous
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
    run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml previous
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT

    echo "start DM task only"
    dmctl_start_task

    echo "use sync_diff_inspector to check full dump loader"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    pkill -hup dm-worker.test.previous 2>/dev/null || true
    wait_process_exit dm-worker.test.previous

    echo "restart dm-worker, one use the current version, and the other one use the previous version"
    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml current
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml previous

    run_sql_file $cur/data/db1.increment.1.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
    run_sql_file $cur/data/db2.increment.1.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

    echo "use sync_diff_inspector to check increment data first time"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
    
    echo "use current dm-master"
    pkill -hup dm-master.test.previous 2>/dev/null || true
    wait_process_exit dm-master.test.previous
    run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml current
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
    sleep 3
    
    echo "pause task and check status"
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "pause-task test" \
        "\"result\": true" 3

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status test" \
        "\"stage\": \"Paused\"" 2

    run_sql_file $cur/data/db1.increment.2.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
    run_sql_file $cur/data/db2.increment.2.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

    echo "resume task and check status"
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "resume-task test" \
        "\"result\": true" 3

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status test" \
        "\"stage\": \"Running\"" 4

    echo "use sync_diff_inspector to check data second time"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    echo "all dm-worker use current version"
    pkill -hup dm-worker.test.previous 2>/dev/null || true
    wait_process_exit dm-worker.test.previous
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml current

    run_sql_file $cur/data/db1.increment.3.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
    run_sql_file $cur/data/db2.increment.3.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

    echo "use sync_diff_inspector to check data third time"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

cleanup_data compatibility
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
