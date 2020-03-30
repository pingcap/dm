#!/bin/bash

set -eu

# cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
# source $cur/../_utils/test_prepare

ha_test="ha_test"
ha_test2="ha_test2"

# start a tipocket process to generate random SQL and execute them on passed DSN argument
# make sure pocket (binary of tipocket) can be found in PATH
# more details at https://github.com/pingcap/tipocket
function start_random_sql_to() {
    dsn=$1
    config_name="pocket-dml.toml"
    if [ $2 = 1 ]; then
        config_name="pocket-hybrid.toml"
    fi
    bin="pocket"
    if [ $3 = 1 ]; then
        bin="pocketb"
    fi
    eval "${bin} -dsn1 \"${dsn}\" -config \"$cur/conf/$config_name\" > /var/log/tipoket.run.log 2>&1 &"
    pid=$!
    # return pocket's pid for being killed in future
    echo $pid
}
# unit test case (with ddl):
# pid=$(start_random_sql_to "root:123456@tcp(127.0.0.1:3306)/pocket" 0 1)

function run_sql_file_withdb() {
    sql=$1
    host=$2
    port=$3
    pswd=$4
    db=$5
    cp $cur/data/$sql $WORK_DIR/$sql
    sed -i "s/database-placeholder/$db/g" $WORK_DIR/$sql
    run_sql_file $WORK_DIR/$sql $host $port $pswd
}

# build tables etc.
function prepare_sql() {
    echo "import prepare data"
    run_sql_file_withdb $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1 $ha_test
    check_contains 'Query OK, 2 rows affected'
    run_sql_file_withdb $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2 $ha_test
    check_contains 'Query OK, 3 rows affected'
}


# build tables etc. for multi tasks
function prepare_sql_multi_task() {
    echo "import prepare data"
    run_sql_file_withdb $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1 $ha_test
    check_contains 'Query OK, 2 rows affected'
    run_sql_file_withdb $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2 $ha_test
    check_contains 'Query OK, 3 rows affected'
    run_sql_file_withdb $cur/data/db1.prepare.sql $MYSQL_HOST3 $MYSQL_PORT1 $MYSQL_PASSWORD1 $ha_test2
    check_contains 'Query OK, 2 rows affected'
    run_sql_file_withdb $cur/data/db2.prepare.sql $MYSQL_HOST4 $MYSQL_PORT2 $MYSQL_PASSWORD2 $ha_test2
    check_contains 'Query OK, 3 rows affected'
}


function start_cluster() {
    echo "start DM worker and master cluster"
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


function start_standalone_cluster() {
    echo "start DM worker and master standalone cluster"
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
    sed -i "/relay-binlog-name/i\relay-dir = \"$WORK_DIR/worker1/relay_log\"" $WORK_DIR/source1.toml
    dmctl_operate_source create $WORK_DIR/source1.toml $SOURCE_ID1


    echo "start DM task"
    dmctl_start_task_standalone $cur/conf/standalone-task.yaml
}


function start_multi_tasks_cluster() {
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
    run_dm_worker $WORK_DIR/worker3 $WORKER3_PORT $cur/conf/dm-worker3.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER3_PORT
    run_dm_worker $WORK_DIR/worker4 $WORKER4_PORT $cur/conf/dm-worker4.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER4_PORT
    run_dm_worker $WORK_DIR/worker5 $WORKER5_PORT $cur/conf/dm-worker5.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER5_PORT
    echo "operate mysql config to worker"
    cp $cur/conf/source1.toml $WORK_DIR/source1.toml
    cp $cur/conf/source2.toml $WORK_DIR/source2.toml
    cp $cur/conf/source3.toml $WORK_DIR/source3.toml
    cp $cur/conf/source4.toml $WORK_DIR/source4.toml
    sed -i "/relay-binlog-name/i\relay-dir = \"$WORK_DIR/worker1/relay_log\"" $WORK_DIR/source1.toml
    sed -i "/relay-binlog-name/i\relay-dir = \"$WORK_DIR/worker2/relay_log\"" $WORK_DIR/source2.toml
    sed -i "/relay-binlog-name/i\relay-dir = \"$WORK_DIR/worker3/relay_log\"" $WORK_DIR/source3.toml
    sed -i "/relay-binlog-name/i\relay-dir = \"$WORK_DIR/worker4/relay_log\"" $WORK_DIR/source4.toml
    dmctl_operate_source create $WORK_DIR/source1.toml $SOURCE_ID1
    dmctl_operate_source create $WORK_DIR/source2.toml $SOURCE_ID2
    dmctl_operate_source create $WORK_DIR/source3.toml $SOURCE_ID3
    dmctl_operate_source create $WORK_DIR/source4.toml $SOURCE_ID4

    echo "start DM task"

    dmctl_start_task

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "start-task $cur/conf/dm-task2.yaml" \
        "\"result\": true" 3 \
        "\"source\": \"$SOURCE_ID3\"" 1 \
        "\"source\": \"$SOURCE_ID4\"" 1
}


function cleanup() {
    cleanup_data ha_test
    cleanup_data ha_test2
    echo "clean source table"
    mysql_ports=(MYSQL_PORT1 MYSQL_PORT2)
    for i in ${mysql_ports[@]}; do
        $(mysql -h127.1 -p123456 -P$i -e "drop database if exists ha_test;")
    done
    cleanup_process $*
}


function isolate_port() {
    port=$1
    iptables -I OUTPUT -p tcp --dport $port -j DROP
}


function disable_isolate_port() {
    port=$1
    iptables -D OUTPUT -p tcp --dport $port -j DROP
}