#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
TASK_NAME="test"

function run() {
    run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1
    check_contains 'Query OK, 2 rows affected'
    run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2
    check_contains 'Query OK, 3 rows affected'

    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml

    check_port_alive $MASTER_PORT
    check_port_alive $WORKER1_PORT
    check_port_alive $WORKER2_PORT

    cd $cur && GO111MODULE=on go build -o bin/dmctl dmctl.go && cd -
    cd $cur && GO111MODULE=on go build -o bin/dmctl_stop dmctl_stop.go && cd -

    cat $cur/conf/dm-task.yaml > $WORK_DIR/dm-task.yaml
    sed -i "s/task-mode-placeholder/full/g" $WORK_DIR/dm-task.yaml
    # avoid cannot unmarshal !!str `binlog-...` into uint32 error
    sed -i "s/binlog-pos-placeholder-1/4/g" $WORK_DIR/dm-task.yaml
    sed -i "s/binlog-pos-placeholder-2/4/g" $WORK_DIR/dm-task.yaml
    $cur/bin/dmctl "$WORK_DIR/dm-task.yaml"

    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    $cur/bin/dmctl_stop $TASK_NAME

    run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1
    run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2

    cat $cur/conf/dm-task.yaml > $WORK_DIR/dm-task.yaml
    sed -i "s/task-mode-placeholder/incremental/g" $WORK_DIR/dm-task.yaml
    name1=$(grep "Log: " $WORK_DIR/worker1/dumped_data.$TASK_NAME/metadata|awk -F: '{print $2}'|tr -d ' ')
    pos1=$(grep "Pos: " $WORK_DIR/worker1/dumped_data.$TASK_NAME/metadata|awk -F: '{print $2}'|tr -d ' ')
    name2=$(grep "Log: " $WORK_DIR/worker2/dumped_data.$TASK_NAME/metadata|awk -F: '{print $2}'|tr -d ' ')
    pos2=$(grep "Pos: " $WORK_DIR/worker2/dumped_data.$TASK_NAME/metadata|awk -F: '{print $2}'|tr -d ' ')
    sed -i "s/binlog-name-placeholder-1/$name1/g" $WORK_DIR/dm-task.yaml
    sed -i "s/binlog-pos-placeholder-1/$pos1/g" $WORK_DIR/dm-task.yaml
    sed -i "s/binlog-name-placeholder-2/$name2/g" $WORK_DIR/dm-task.yaml
    sed -i "s/binlog-pos-placeholder-2/$pos2/g" $WORK_DIR/dm-task.yaml
    $cur/bin/dmctl "$WORK_DIR/dm-task.yaml"

    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

cleanup1 $TEST_NAME
# also cleanup dm processes in case of last run failed
cleanup2 $*
run $*
cleanup2 $*

wait_process_exit dm-master.test
wait_process_exit dm-worker.test

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
