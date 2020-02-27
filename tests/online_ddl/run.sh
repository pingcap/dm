#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare
ONLINE_DDL_ENABLE=${ONLINE_DDL_ENABLE:-true}
BASE_TEST_NAME=$TEST_NAME

function real_run() {
    online_ddl_scheme=$1
    run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
    check_contains 'Query OK, 2 rows affected'
    run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
    check_contains 'Query OK, 3 rows affected'

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

    # start DM task only
    cp $cur/conf/dm-task.yaml $WORK_DIR/dm-task-${online_ddl_scheme}.yaml
    sed -i "s/online-ddl-scheme-placeholder/${online_ddl_scheme}/g" $WORK_DIR/dm-task-${online_ddl_scheme}.yaml
    dmctl_start_task "$WORK_DIR/dm-task-${online_ddl_scheme}.yaml"

    echo "use sync_diff_inspector to check full dump data"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    run_sql_file_online_ddl $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1 online_ddl $online_ddl_scheme
    run_sql_file_online_ddl $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2 online_ddl $online_ddl_scheme

    echo "use sync_diff_inspector to check increment data"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    echo "start dm-worker3 and kill dm-worker2"
    ps aux | grep dm-worker2 |awk '{print $2}'|xargs kill || true
    check_port_offline $WORKER2_PORT 20

    run_dm_worker $WORK_DIR/worker3 $WORKER3_PORT $cur/conf/dm-worker3.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER3_PORT

    echo "wait and check task running"
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status test" \
        "\"stage\": \"Running\"" 2

    run_sql_file $cur/data/db1.increment2.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
    run_sql_file $cur/data/db2.increment2.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
    sleep 2

    echo "use sync_diff_inspector to check increment2 data now!"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function run() {
    online_ddl_scheme=$1
    TEST_NAME=${BASE_TEST_NAME}_$online_ddl_scheme
    WORK_DIR=$TEST_DIR/$TEST_NAME

    cleanup_data online_ddl
    # also cleanup dm processes in case of last run failed
    cleanup_process $*
    real_run $*
    cleanup_process $*

    echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
}

if [ "$ONLINE_DDL_ENABLE" == true ]; then
    run gh-ost
    run pt
else
    echo "[$(date)] <<<<<< skip online ddl test! >>>>>>"
fi
