#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare
ONLINE_DDL_ENABLE=${ONLINE_DDL_ENABLE:-true}
BASE_TEST_NAME=$TEST_NAME

function real_run() {
    online_ddl_scheme=$1
    run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1
    check_contains 'Query OK, 2 rows affected'
    run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2
    check_contains 'Query OK, 3 rows affected'

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
    cp $cur/conf/dm-task.yaml $WORK_DIR/dm-task-${online_ddl_scheme}.yaml
    sed -i "s/online-ddl-scheme-placeholder/${online_ddl_scheme}/g" $WORK_DIR/dm-task-${online_ddl_scheme}.yaml
    dmctl_start_task "$WORK_DIR/dm-task-${online_ddl_scheme}.yaml"

    # use sync_diff_inspector to check full dump loader
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    run_sql_file_online_ddl $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 online_ddl $online_ddl_scheme
    run_sql_file_online_ddl $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 online_ddl $online_ddl_scheme

    # use sync_diff_inspector to check data now!
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
