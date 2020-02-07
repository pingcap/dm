#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
API_VERSION="v1alpha1"

function run() {
    run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1
    check_contains 'Query OK, 2 rows affected'
    run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2
    check_contains 'Query OK, 3 rows affected'

    echo "start DM worker and master"
    run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
    echo "operate mysql config to worker"
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


    echo "start DM task"
    dmctl_start_task

    echo "use sync_diff_inspector to check full dump loader"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    echo "send sql-skip request for two sources"
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "sql-skip --source \"mysql-replica-01\" --sql-pattern \"~(?i)DROP\\s+TABLE\\s+\" test" \
        "\"result\": true" 2

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "sql-skip --source \"mysql-replica-02\" --sql-pattern \"~(?i)TRUNCATE\\s+TABLE\\s+\" test" \
        "\"result\": true" 2
   
    echo "flush logs to force rotate binlog file"
    run_sql "flush logs;" $MYSQL_PORT1
    run_sql "flush logs;" $MYSQL_PORT2

    echo "start dm-worker3 and kill dm-worker2"
    ps aux | grep dm-worker2 |awk '{print $2}'|xargs kill || true
    check_port_offline $WORKER2_PORT 20
    rm -rf $WORK_DIR/worker2/relay_log

    run_dm_worker $WORK_DIR/worker3 $WORKER3_PORT $cur/conf/dm-worker3.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER3_PORT

    echo "wait and check task running"
    check_http_alive 127.0.0.1:$MASTER_PORT/apis/${API_VERSION}/status/test '"name":"test","stage":"Running"' 10
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status test" \
        "\"stage\": \"Running\"" 4

    run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1
    run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2
    sleep 2

    echo "use sync_diff_inspector to check data now!"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    echo "run DDL on MySQL, and these DDL will be skipped by DM"
    run_sql "DROP TABLE ha_test.t1"  $MYSQL_PORT1
    run_sql "TRUNCATE TABLE ha_test.t2" $MYSQL_PORT2

    echo "check data in TiDB"
    run_sql "select count(*) from ha_test.t1" $TIDB_PORT
    check_contains "count(*): 6"
    run_sql "select count(*) from ha_test.t2" $TIDB_PORT
    check_contains "count(*): 2"
}

cleanup_data ha_test
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
