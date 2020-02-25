#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
API_VERSION="v1alpha1"
MASTER_PORT1=8261
MASTER_PORT2=8361
MASTER_PORT3=8461
MASTER_PORT4=8561
MASTER_PORT5=8661

function run() {
    run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
    check_contains 'Query OK, 2 rows affected'
    run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
    check_contains 'Query OK, 3 rows affected'

    echo "start DM worker and master"
    # start 5 dm-master
    run_dm_master $WORK_DIR/master1 $MASTER_PORT1 $cur/conf/dm-master1.toml
    run_dm_master $WORK_DIR/master2 $MASTER_PORT2 $cur/conf/dm-master2.toml
    run_dm_master $WORK_DIR/master3 $MASTER_PORT3 $cur/conf/dm-master3.toml
    run_dm_master $WORK_DIR/master4 $MASTER_PORT4 $cur/conf/dm-master4.toml
    run_dm_master $WORK_DIR/master5 $MASTER_PORT5 $cur/conf/dm-master5.toml
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT1
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT2
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT3
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT4
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT5

    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
    echo "operate mysql config to worker"
    cp $cur/conf/source1.toml $WORK_DIR/source1.toml
    cp $cur/conf/source2.toml $WORK_DIR/source2.toml
    sed -i "/relay-binlog-name/i\relay-dir = \"$WORK_DIR/worker1/relay_log\"" $WORK_DIR/source1.toml
    sed -i "/relay-binlog-name/i\relay-dir = \"$WORK_DIR/worker2/relay_log\"" $WORK_DIR/source2.toml
    dmctl_operate_source create $WORK_DIR/source1.toml $SOURCE_ID1
    dmctl_operate_source create $WORK_DIR/source2.toml $SOURCE_ID2


    echo "start DM task"
    dmctl_start_task

    echo "use sync_diff_inspector to check full dump loader"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    echo "flush logs to force rotate binlog file"
    run_sql "flush logs;" $MYSQL_PORT1 $MYSQL_PASSWORD1
    run_sql "flush logs;" $MYSQL_PORT2 $MYSQL_PASSWORD2

    echo "kill dm-master1 and kill dm-master2"
    ps aux | grep dm-master1 |awk '{print $2}'|xargs kill || true
    check_port_offline $MASTER_PORT1 20
    ps aux | grep dm-master2 |awk '{print $2}'|xargs kill || true
    check_port_offline $MASTER_PORT2 20

    echo "wait and check task running"
    check_http_alive 127.0.0.1:$MASTER_PORT3/apis/${API_VERSION}/status/test '"name":"test","stage":"Running"' 10
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT3" \
        "query-status test" \
        "\"stage\": \"Running\"" 2

    run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
    run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
    sleep 2

    echo "use sync_diff_inspector to check data now!"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

cleanup_data ha_master_test
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
