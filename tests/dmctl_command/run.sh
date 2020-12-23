#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

help_cnt=34

function run() {
    # check dmctl output with help flag
    # it should usage for root command
    $PWD/bin/dmctl.test DEVEL --help > $WORK_DIR/help.log
    help_msg=$(cat $WORK_DIR/help.log)
    help_msg_cnt=$(echo "${help_msg}" | wc -l |xargs)
    if [ "$help_msg_cnt" != $help_cnt ]; then
        echo "dmctl case 1 help failed: $help_msg"
        exit 1
    fi

    run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
    # check dmctl command start-task output with master-addr
    # it should usage for start-task
    $PWD/bin/dmctl.test DEVEL --master-addr=:$MASTER_PORT start-task > $WORK_DIR/help.log 2>&1 && exit 1 || echo "exit code should be not zero"
    help_msg=$(cat $WORK_DIR/help.log)

    echo $help_msg | grep -q "dmctl start-task"
    if [ $? -ne 0 ]; then
        echo "dmctl case 2 help failed: $help_msg"
        exit 1
    fi

    # check dmctl command start-task output with master-addr and unknown flag
    # it should print parse cmd flags err: 'xxxx' is an invalid flag%
    $PWD/bin/dmctl.test DEVEL --master-addr=:$MASTER_PORT xxxx start-task > $WORK_DIR/help.log 2>&1 && exit 1 || echo "exit code should be not zero"
    help_msg=$(cat $WORK_DIR/help.log)
    help_msg_cnt=$(echo "${help_msg}" | wc -l |xargs)
    if [ "$help_msg_cnt" != 1 ]; then
        echo "dmctl case 3 help failed: $help_msg"
        exit 1
    fi
    echo $help_msg | grep -q "parse cmd flags err: 'xxxx' is an invalid flag"
    if [ $? -ne 0 ]; then
        echo "dmctl case 3 help failed: $help_msg"
        exit 1
    fi

    # run normal task with command
    run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
    check_contains 'Query OK, 2 rows affected'
    run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
    check_contains 'Query OK, 3 rows affected'

    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
    # operate mysql config to worker
    cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
    cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
    sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
    dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
    dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

    # start DM task with command mode
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "start-task $cur/conf/dm-task.yaml" \
        "\`remove-meta\` in task config is deprecated, please use \`start-task ... --remove-meta\` instead" 1
    check_log_contains $WORK_DIR/master/log/dm-master.log "\`remove-meta\` in task config is deprecated, please use \`start-task ... --remove-meta\` instead"

    # use sync_diff_inspector to check full dump loader
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    # query status with command mode
    $PWD/bin/dmctl.test DEVEL --master-addr=:$MASTER_PORT query-status > $WORK_DIR/query-status.log

    running_task=$(grep -r Running $WORK_DIR/query-status.log | wc -l | xargs)

    if [ "$running_task" != 1 ]; then
        echo "query status failed with command: $running_task task"
        exit 1
    fi
}

cleanup_data dmctl_command
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
