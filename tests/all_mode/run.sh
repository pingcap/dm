#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
API_VERSION="v1alpha1"

function run() {
    export GO_FAILPOINTS="github.com/pingcap/dm/dm/worker/TaskCheckInterval=return(\"500ms\")"

    run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1
    check_contains 'Query OK, 2 rows affected'
    run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2
    check_contains 'Query OK, 3 rows affected'

    # start DM worker and master
    run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
    # operate mysql config to worker
    cp $cur/conf/mysql1.toml $WORK_DIR/mysql1.toml
    cp $cur/conf/mysql2.toml $WORK_DIR/mysql2.toml
    sed -i "/relay-binlog-name/i\relay-dir = \"$WORK_DIR/worker1/relay_log\"" $WORK_DIR/mysql1.toml
    sed -i "/relay-binlog-name/i\relay-dir = \"$WORK_DIR/worker2/relay_log\"" $WORK_DIR/mysql2.toml
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "operate-worker create $WORK_DIR/mysql1.toml" \
        "true" 1
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "operate-worker create $WORK_DIR/mysql2.toml" \
        "true" 1


    # start DM task only
    dmctl_start_task

    # use sync_diff_inspector to check full dump loader
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    pkill -hup dm-worker.test 2>/dev/null || true
    wait_process_exit dm-worker.test

    # restart dm-worker
    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

    sleep 5
    echo "after restart dm-worker, task should resume automatically"
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "start-task $cur/conf/dm-task.yaml" \
        "\"result\": true" 1 \
        "start sub task test: sub task test already exists" 2
    sleep 2

    # wait for task running
    check_http_alive 127.0.0.1:$MASTER_PORT/apis/${API_VERSION}/status/test '"name":"test","stage":"Running"' 10
    sleep 2 # still wait for subtask running on other dm-workers

    # wait for task running
    check_http_alive 127.0.0.1:$MASTER_PORT/apis/${API_VERSION}/status/test '"name":"test","stage":"Running"' 10
    sleep 2 # still wait for subtask running on other dm-workers

    # kill tidb
    pkill -hup tidb-server 2>/dev/null || true
    wait_process_exit tidb-server

    run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1
    run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2

    sleep 2
    # dm-worker execute sql failed, and will try auto resume task
    check_log_contains $WORK_DIR/worker1/log/dm-worker.log "dispatch auto resume task"
    check_log_contains $WORK_DIR/worker2/log/dm-worker.log "dispatch auto resume task"

    # restart tidb, and task will recover success
    run_tidb_server 4000
    sleep 2

    # use sync_diff_inspector to check data now!
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    check_metric $WORKER1_PORT 'dm_syncer_replication_lag{task="test"}' 3 0 1
    check_metric $WORKER2_PORT 'dm_syncer_replication_lag{task="test"}' 3 0 1

    export GO_FAILPOINTS=''
}

cleanup_data all_mode
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
