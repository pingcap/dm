#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
TASK_NAME="test"

function run() {
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

    # start a task in `full` mode
    echo "start task in full mode"
    cat $cur/conf/dm-task.yaml > $WORK_DIR/dm-task.yaml
    sed -i "s/task-mode-placeholder/full/g" $WORK_DIR/dm-task.yaml
    # avoid cannot unmarshal !!str `binlog-...` into uint32 error
    sed -i "s/binlog-pos-placeholder-1/4/g" $WORK_DIR/dm-task.yaml
    sed -i "s/binlog-pos-placeholder-2/4/g" $WORK_DIR/dm-task.yaml
    dmctl_start_task $WORK_DIR/dm-task.yaml

    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    dmctl_stop_task $TASK_NAME

    # $worker1_run_source_1 > 0 means source1 is operated to worker1
    worker1_run_source_1=$(sed "s/$SOURCE_ID1/$SOURCE_ID1\n/g" $WORK_DIR/worker1/log/dm-worker.log | grep -c "$SOURCE_ID1") || true
    if [ $worker1_run_source_1 -gt 0 ]
    then
        name1=$(grep "Log: " $WORK_DIR/worker1/dumped_data.$TASK_NAME/metadata|awk -F: '{print $2}'|tr -d ' ')
        pos1=$(grep "Pos: " $WORK_DIR/worker1/dumped_data.$TASK_NAME/metadata|awk -F: '{print $2}'|tr -d ' ')
        gtid1=$(grep "GTID:" $WORK_DIR/worker1/dumped_data.$TASK_NAME/metadata|awk -F: '{print $2,":",$3}'|tr -d ' ')
        name2=$(grep "Log: " $WORK_DIR/worker2/dumped_data.$TASK_NAME/metadata|awk -F: '{print $2}'|tr -d ' ')
        pos2=$(grep "Pos: " $WORK_DIR/worker2/dumped_data.$TASK_NAME/metadata|awk -F: '{print $2}'|tr -d ' ')
        gtid2=$(grep "GTID:" $WORK_DIR/worker2/dumped_data.$TASK_NAME/metadata|awk -F: '{print $2,":",$3}'|tr -d ' ')
    else
        name2=$(grep "Log: " $WORK_DIR/worker1/dumped_data.$TASK_NAME/metadata|awk -F: '{print $2}'|tr -d ' ')
        pos2=$(grep "Pos: " $WORK_DIR/worker1/dumped_data.$TASK_NAME/metadata|awk -F: '{print $2}'|tr -d ' ')
        gtid2=$(grep "GTID:" $WORK_DIR/worker1/dumped_data.$TASK_NAME/metadata|awk -F: '{print $2,":",$3}'|tr -d ' ')
        name1=$(grep "Log: " $WORK_DIR/worker2/dumped_data.$TASK_NAME/metadata|awk -F: '{print $2}'|tr -d ' ')
        pos1=$(grep "Pos: " $WORK_DIR/worker2/dumped_data.$TASK_NAME/metadata|awk -F: '{print $2}'|tr -d ' ')
        gtid1=$(grep "GTID:" $WORK_DIR/worker2/dumped_data.$TASK_NAME/metadata|awk -F: '{print $2,":",$3}'|tr -d ' ')
    fi
    # kill worker1 and worker2
    kill_dm_worker
    check_port_offline $WORKER1_PORT 20
    check_port_offline $WORKER2_PORT 20

    run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
    run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

    # start a task in `incremental` mode
    # using account with limited privileges
    run_sql_file $cur/data/db1.prepare.user.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
    check_count 'Query OK, 0 rows affected' 7
    run_sql_file $cur/data/db2.prepare.user.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
    check_count 'Query OK, 0 rows affected' 7

    # update mysql config
    sed -i "s/root/dm_incremental/g" $WORK_DIR/source1.toml
    sed -i "s/root/dm_incremental/g" $WORK_DIR/source2.toml
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "operate-source update $WORK_DIR/source1.toml" \
        "Update worker config is not supported by dm-ha now" 1
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "operate-source update $WORK_DIR/source2.toml" \
        "Update worker config is not supported by dm-ha now" 1
    # update mysql config is not supported by dm-ha now, so we stop and start source again to update source config
    dmctl_operate_source stop $WORK_DIR/source1.toml $SOURCE_ID1
    dmctl_operate_source stop $WORK_DIR/source2.toml $SOURCE_ID2
    dmctl_operate_source create $WORK_DIR/source1.toml $SOURCE_ID1
    dmctl_operate_source create $WORK_DIR/source2.toml $SOURCE_ID2

    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

    echo "start task in incremental mode"
    cat $cur/conf/dm-task.yaml > $WORK_DIR/dm-task.yaml
    sed -i "s/task-mode-placeholder/incremental/g" $WORK_DIR/dm-task.yaml
    sed -i "s/binlog-name-placeholder-1/$name1/g" $WORK_DIR/dm-task.yaml
    sed -i "s/binlog-pos-placeholder-1/$pos1/g" $WORK_DIR/dm-task.yaml
    sed -i "s/binlog-gtid-placeholder-1/$gtid1/g" $WORK_DIR/dm-task.yaml

    sed -i "s/binlog-name-placeholder-2/$name2/g" $WORK_DIR/dm-task.yaml
    sed -i "s/binlog-pos-placeholder-2/$pos2/g" $WORK_DIR/dm-task.yaml
    sed -i "s/binlog-gtid-placeholder-2/$gtid2/g" $WORK_DIR/dm-task.yaml

    sleep 3
    dmctl_start_task $WORK_DIR/dm-task.yaml

    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

cleanup_data $TEST_NAME
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
