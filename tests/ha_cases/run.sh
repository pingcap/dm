#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
API_VERSION="v1alpha1"
# import helper functions
source $cur/lib.sh


function test_running() {
    echo "[$(date)] <<<<<< start test_running >>>>>>"
    cleanup
    prepare_sql
    start_cluster

    echo "use sync_diff_inspector to check full dump loader"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    echo "flush logs to force rotate binlog file"
    run_sql "flush logs;" $MYSQL_PORT1 $MYSQL_PASSWORD1
    run_sql "flush logs;" $MYSQL_PORT2 $MYSQL_PASSWORD2

    echo "apply increment data before restart dm-worker to ensure entering increment phase"
    run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
    run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

    echo "use sync_diff_inspector to check increment data"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}


function test_kill_master() {
    test_running
    echo "[$(date)] <<<<<< start test_kill_master >>>>>>"

    echo "kill dm-master1"
    ps aux | grep dm-master1 | awk '{print $2}' | xargs kill || true
    check_port_offline $MASTER_PORT1 20
    rm -rf $WORK_DIR/master1/default.master1

    echo "waiting 5 seconds"
    sleep 5
    echo "check task is running"
    check_http_alive 127.0.0.1:$MASTER_PORT2/apis/${API_VERSION}/status/test '"name":"test","stage":"Running"' 10

    echo "check master2,3 are running"
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT2" \
        "query-status test" \
        "\"stage\": \"Running\"" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT3" \
        "query-status test" \
        "\"stage\": \"Running\"" 2

    run_sql_file $cur/data/db1.increment2.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
    run_sql_file $cur/data/db2.increment2.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
    sleep 2

    echo "use sync_diff_inspector to check increment2 data now!"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}


function test_kill_worker() {
    test_running
    echo "[$(date)] <<<<<< start test_kill_worker >>>>>>"


    echo "kill dm-worker2"
    ps aux | grep dm-worker2 |awk '{print $2}'|xargs kill || true
    check_port_offline $WORKER2_PORT 20
    rm -rf $WORK_DIR/worker2/relay_log

    run_dm_worker $WORK_DIR/worker3 $WORKER3_PORT $cur/conf/dm-worker3.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER3_PORT

    echo "wait and check task running"
    check_http_alive 127.0.0.1:$MASTER_PORT/apis/${API_VERSION}/status/test '"name":"test","stage":"Running"' 10

    echo "query-status from all dm-master"
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT1" \
        "query-status test" \
        "\"stage\": \"Running\"" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT2" \
        "query-status test" \
        "\"stage\": \"Running\"" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT3" \
        "query-status test" \
        "\"stage\": \"Running\"" 2

    run_sql_file $cur/data/db1.increment2.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
    run_sql_file $cur/data/db2.increment2.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
    sleep 2

    echo "use sync_diff_inspector to check increment2 data now!"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}


function test_kill_worker_in_sync() {
    test_running
    echo "[$(date)] <<<<<< start test_kill_worker_in_sync >>>>>>"

    echo "start dumping random SQLs into source"
    pocket_pid1=$(start_random_sql_to "root:123456@tcp(127.0.0.1:3306)/ha_test")
    pocket_pid2=$(start_random_sql_to "root:123456@tcp(127.0.0.1:3307)/ha_test")

    sleep 1

    echo "start worker3"
    run_dm_worker $WORK_DIR/worker3 $WORKER3_PORT $cur/conf/dm-worker3.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER3_PORT

    echo "kill dm-worker2"
    ps aux | grep dm-worker2 |awk '{print $2}'|xargs kill || true

    echo "wait and check task running"
    check_http_alive 127.0.0.1:$MASTER_PORT/apis/${API_VERSION}/status/test '"name":"test","stage":"Running"' 10

    echo "query-status from all dm-master"
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT1" \
        "query-status test" \
        "\"stage\": \"Running\"" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT2" \
        "query-status test" \
        "\"stage\": \"Running\"" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT3" \
        "query-status test" \
        "\"stage\": \"Running\"" 2
    
    sleep 1

    echo "kill tipocket"
    kill $pocket_pid1 $pocket_pid2 || true

    # waiting for syncing
    sleep 1
    
    echo "use sync_diff_inspector to check increment2 data now!"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}


function test_kill_master_in_sync() {
    test_running
    echo "[$(date)] <<<<<< start test_kill_worker_in_sync >>>>>>"

    echo "start dumping random SQLs into source"
    pocket_pid1=$(start_random_sql_to "root:123456@tcp(127.0.0.1:3306)/ha_test")
    pocket_pid2=$(start_random_sql_to "root:123456@tcp(127.0.0.1:3307)/ha_test")

    sleep 1

    echo "kill dm-master1"
    ps aux | grep dm-master1 |awk '{print $2}'|xargs kill || true

    echo "wait and check task running"
    check_http_alive 127.0.0.1:$MASTER_PORT2/apis/${API_VERSION}/status/test '"name":"test","stage":"Running"' 10

    echo "query-status from all dm-master"
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT2" \
        "query-status test" \
        "\"stage\": \"Running\"" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT3" \
        "query-status test" \
        "\"stage\": \"Running\"" 2
    
    sleep 1

    echo "kill tipocket"
    kill $pocket_pid1 $pocket_pid2 || true

    # waiting for syncing
    sleep 1
    
    echo "use sync_diff_inspector to check increment2 data now!"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}


function run() {
    test_kill_master_in_sync
}


cleanup_data ha_test
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
