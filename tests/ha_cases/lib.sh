#!/bin/bash

set -eu

# cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
# source $cur/../_utils/test_prepare


# start a tipocket process to generate random SQL and execute them on passed DSN argument
# make sure pocket (binary of tipocket) can be found in PATH
# more details at https://github.com/pingcap/tipocket
function start_random_sql_to() {
    dsn=$1
    eval "pocket -dsn1 \"${dsn}\" -log /var/log/tipoket -stable > /var/log/tipoket.run.log 2>&1 &"
    pid=$!
    # return pocket's pid for being killed in future
    echo $pid
}
# unit test case:
# pid=$(start_random_sql_to "root:123456@tcp(127.0.0.1:3306)/pocket")


# build tables etc.
function prepare_sql() {
    echo "import prepare data"
    run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
    check_contains 'Query OK, 2 rows affected'
    run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
    check_contains 'Query OK, 3 rows affected'
}


function start_cluster() {
    echo "start DM worker and master"
    run_dm_master $WORK_DIR/master1 $MASTER_PORT1 $cur/conf/dm-master1.toml
    run_dm_master $WORK_DIR/master2 $MASTER_PORT2 $cur/conf/dm-master2.toml
    run_dm_master $WORK_DIR/master3 $MASTER_PORT3 $cur/conf/dm-master3.toml
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT1
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT2
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT3

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
}


function cleanup() {
    cleanup_data ha_test
    cleanup_process $*
}