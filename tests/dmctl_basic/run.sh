#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
TASK_CONF=$cur/conf/dm-task.yaml
TASK_NAME="test"
MYSQL1_CONF=$cur/conf/source1.toml
SQL_RESULT_FILE="$TEST_DIR/sql_res.$TEST_NAME.txt"

# used to coverage wrong usage of dmctl command
function usage_and_arg_test() {
    echo "check_task_wrong_arg"
    check_task_wrong_arg
    check_task_wrong_config_file
    check_task_while_master_down $TASK_CONF

    echo "pause_relay_wrong_arg"
    pause_relay_wrong_arg
    pause_relay_wihout_worker
    pause_relay_while_master_down

    echo "resume_relay_wrong_arg"
    resume_relay_wrong_arg
    resume_relay_wihout_worker
    resume_relay_while_master_down

    echo "pause_task_wrong_arg"
    pause_task_wrong_arg
    pause_task_while_master_down

    echo "resume_task_wrong_arg"
    resume_task_wrong_arg
    resume_task_while_master_down

    echo "query_status_wrong_arg"
    query_status_wrong_arg
    query_status_wrong_params

    echo "start_task_wrong_arg"
    start_task_wrong_arg
    start_task_wrong_config_file
    start_task_while_master_down $TASK_CONF

    echo "stop_task_wrong_arg"
    stop_task_wrong_arg
    stop_task_while_master_down

    echo "show_ddl_locks_wrong_arg"
    show_ddl_locks_wrong_arg
    show_ddl_locks_while_master_down

    echo "update_relay_wrong_arg"
    update_relay_wrong_arg
    update_relay_wrong_config_file
    update_relay_should_specify_one_dm_worker $MYSQL1_CONF
    update_relay_while_master_down $MYSQL1_CONF

    # echo "update_task_wrong_arg"
    # update_task_wrong_arg
    # update_task_wrong_config_file
    # update_task_while_master_down $TASK_CONF

    echo "update_master_config_wrong_arg"
    update_master_config_wrong_arg
    update_master_config_wrong_config_file
    update_master_config_while_master_down $cur/conf/dm-master.toml

    echo "purge_relay_wrong_arg"
    purge_relay_wrong_arg
    purge_relay_wihout_worker
    purge_relay_filename_with_multi_workers
    purge_relay_while_master_down

    echo "operate_source_empty_arg"
    operate_source_empty_arg
    operate_source_wrong_config_file
    operate_source_while_master_down $MYSQL1_CONF
}

function recover_max_binlog_size() {
    run_sql "set @@global.max_binlog_size = $1" $MYSQL_PORT1 $MYSQL_PASSWORD1
    run_sql "set @@global.max_binlog_size = $2" $MYSQL_PORT2 $MYSQL_PASSWORD2
}

function run() {
    inject_points=("github.com/pingcap/dm/syncer/SyncerEventTimeout=return(1)")
    export GO_FAILPOINTS="$(join_string \; ${inject_points[@]})"

    run_sql "show variables like 'max_binlog_size'\G" $MYSQL_PORT1 $MYSQL_PASSWORD1
    max_binlog_size1=$(tail -n 1 "$TEST_DIR/sql_res.$TEST_NAME.txt" | awk '{print $NF}')
    run_sql "show variables like 'max_binlog_size'\G" $MYSQL_PORT2 $MYSQL_PASSWORD2
    max_binlog_size2=$(tail -n 1 "$TEST_DIR/sql_res.$TEST_NAME.txt" | awk '{print $NF}')
    run_sql "set @@global.max_binlog_size = 12288" $MYSQL_PORT1 $MYSQL_PASSWORD1
    run_sql "set @@global.max_binlog_size = 12288" $MYSQL_PORT2 $MYSQL_PASSWORD2
    trap "recover_max_binlog_size $max_binlog_size1 $max_binlog_size2" EXIT

    run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
    run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

    cd $cur
    for file in "check_list"/*; do
        source $file
    done
    cd -

    usage_and_arg_test

    mkdir -p $WORK_DIR/master $WORK_DIR/worker1 $WORK_DIR/worker2
    dm_master_conf="$WORK_DIR/master/dm-master.toml"
    dm_worker1_conf="$WORK_DIR/worker1/dm-worker.toml"
    dm_worker2_conf="$WORK_DIR/worker2/dm-worker.toml"
    cp $cur/conf/dm-worker1.toml $dm_worker1_conf
    cp $cur/conf/dm-worker2.toml $dm_worker2_conf
    cp $cur/conf/dm-master.toml $dm_master_conf

    run_dm_master $WORK_DIR/master $MASTER_PORT $dm_master_conf
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $dm_worker1_conf
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $dm_worker2_conf
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

    operate_source_stop_not_created_config $MYSQL1_CONF

    # operate mysql config to worker
    cp $cur/conf/source1.toml $WORK_DIR/source1.toml
    cp $cur/conf/source2.toml $WORK_DIR/source2.toml
    sed -i "/relay-binlog-name/i\relay-dir = \"$WORK_DIR/worker1/relay_log\"" $WORK_DIR/source1.toml
    sed -i "/relay-binlog-name/i\relay-dir = \"$WORK_DIR/worker2/relay_log\"" $WORK_DIR/source2.toml
    
    # operate with invalid op type
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "operate-source invalid $WORK_DIR/source1.toml" \
        "invalid operate" 1

    dmctl_operate_source create $WORK_DIR/source1.toml $SOURCE_ID1
    dmctl_operate_source create $WORK_DIR/source2.toml $SOURCE_ID2

    echo "pause_relay_success"
    pause_relay_success
    query_status_stopped_relay
    # pause twice won't receive an error now
    # pause_relay_fail
    resume_relay_success
    query_status_with_no_tasks

    echo "dmctl_check_task"
    check_task_pass $TASK_CONF
    check_task_not_pass $cur/conf/dm-task2.yaml

    echo "dmctl_start_task"
    dmctl_start_task
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status -s $SOURCE_ID1,$SOURCE_ID2" \
        "\"result\": true" 3 \
        "\"source\": \"$SOURCE_ID1\"" 1 \
        "\"source\": \"$SOURCE_ID2\"" 1 \
        "\"stage\": \"Running\"" 4
    # update_task_not_paused $TASK_CONF

    echo "show_ddl_locks_no_locks"
    show_ddl_locks_no_locks $TASK_NAME
    query_status_with_tasks
    pause_task_success $TASK_NAME
    query_status_paused_tasks

    # echo "update_task_worker_not_found"
    # update_task_worker_not_found $TASK_CONF 127.0.0.1:9999
    # update_task_success_single_worker $TASK_CONF $SOURCE_ID1
    # update_task_success $TASK_CONF

    run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
    run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
    resume_task_success $TASK_NAME
    query_status_running_tasks
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 20

    update_relay_success $cur/conf/source1.toml $SOURCE_ID1
    update_relay_success $cur/conf/source2.toml $SOURCE_ID2
    # check worker config backup file is correct
    [ -f $WORK_DIR/worker1/dm-worker-config.bak ] && cmp $WORK_DIR/worker1/dm-worker-config.bak $cur/conf/dm-worker1.toml
    [ -f $WORK_DIR/worker2/dm-worker-config.bak ] && cmp $WORK_DIR/worker2/dm-worker-config.bak $cur/conf/dm-worker2.toml
    # check worker config has been changed
    md5_new_worker1=$(md5sum $dm_worker1_conf | awk '{print $1}')
    md5_new_worker2=$(md5sum $dm_worker2_conf | awk '{print $1}')
    md5_old_worker1=$(md5sum $cur/conf/dm-worker1.toml | awk '{print $1}')
    md5_old_worker2=$(md5sum $cur/conf/dm-worker2.toml | awk '{print $1}')
    [ "md5_new_worker1" != "md5_old_worker1" ]
    [ "md5_new_worker2" != "md5_old_worker2" ]

    update_master_config_success $dm_master_conf
    cmp $dm_master_conf $cur/conf/dm-master.toml

#   TODO: The ddl sharding part for DM-HA still has some problem. This should be uncommented when it's fixed.
#    run_sql_file $cur/data/db1.increment2.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
#    set +e
#    i=0
#    while [ $i -lt 10 ]
#    do
#        show_ddl_locks_with_locks "$TASK_NAME-\`dmctl\`.\`t_target\`" "ALTER TABLE \`dmctl\`.\`t_target\` DROP COLUMN \`d\`"
#        ((i++))
#        if [ "$?" != 0 ]; then
#            echo "wait 1s and check for the $i-th time"
#            sleep 1
#        else
#            break
#        fi
#    done
#    set -e
#    if [ $i -ge 10 ]; then
#        echo "show_ddl_locks_with_locks check timeout"
#        exit 1
#    fi
#    run_sql_file $cur/data/db2.increment2.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
#    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 10
#    show_ddl_locks_no_locks $TASK_NAME

    # sleep 1s to ensure syncer unit has flushed global checkpoint and updates
    # updated ActiveRelayLog
    sleep 1
    server_uuid=$(tail -n 1 $WORK_DIR/worker1/relay_log/server-uuid.index)
    run_sql "show binary logs\G" $MYSQL_PORT1 $MYSQL_PASSWORD1
    max_binlog_name=$(grep Log_name "$SQL_RESULT_FILE"| tail -n 1 | awk -F":" '{print $NF}')
    binlog_count=$(grep Log_name "$SQL_RESULT_FILE" | wc -l)
    relay_log_count=$(($(ls $WORK_DIR/worker1/relay_log/$server_uuid | wc -l) - 1))
    [ "$binlog_count" -eq "$relay_log_count" ]
    purge_relay_success $max_binlog_name $SOURCE_ID1
    new_relay_log_count=$(($(ls $WORK_DIR/worker1/relay_log/$server_uuid | wc -l) - 1))
    [ "$new_relay_log_count" -eq 1 ]
}

cleanup_data dmctl
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
