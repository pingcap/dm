#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
API_VERSION="v1alpha1"
# import helper functions
source $cur/lib.sh


function test_running() {
    echo "[$(date)] <<<<<< start test_running >>>>>>"
    cleanup
    prepare_sql
    start_cluster

    # make sure task to step in "Sync" stage
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT3" \
        "query-status test" \
        "\"stage\": \"Running\"" 4 \
        "\"unit\": \"Sync\"" 2

    echo "use sync_diff_inspector to check full dump loader"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    echo "flush logs to force rotate binlog file"
    run_sql "flush logs;" $MYSQL_PORT1 $MYSQL_PASSWORD1
    run_sql "flush logs;" $MYSQL_PORT2 $MYSQL_PASSWORD2

    echo "apply increment data before restart dm-worker to ensure entering increment phase"
    run_sql_file_withdb $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1 $ha_test
    run_sql_file_withdb $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2 $ha_test

    sleep 3 # wait for flush checkpoint
    echo "use sync_diff_inspector to check increment data"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
    echo "[$(date)] <<<<<< finish test_running >>>>>>"
}


function test_multi_task_running() {
    echo "[$(date)] <<<<<< start test_multi_task_running >>>>>>"
    cleanup
    prepare_sql_multi_task
    start_multi_tasks_cluster

    # make sure task to step in "Sync" stage
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT3" \
        "query-status test" \
        "\"stage\": \"Running\"" 4 \
        "\"unit\": \"Sync\"" 2
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT3" \
        "query-status test2" \
        "\"stage\": \"Running\"" 4 \
        "\"unit\": \"Sync\"" 2

    echo "use sync_diff_inspector to check full dump loader"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
    check_sync_diff $WORK_DIR $cur/conf/diff_config_multi_task.toml

    echo "flush logs to force rotate binlog file"
    run_sql "flush logs;" $MYSQL_PORT1 $MYSQL_PASSWORD1
    run_sql "flush logs;" $MYSQL_PORT2 $MYSQL_PASSWORD2

    echo "apply increment data before restart dm-worker to ensure entering increment phase"
    run_sql_file_withdb $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1 $ha_test
    run_sql_file_withdb $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2 $ha_test
    run_sql_file_withdb $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1 $ha_test2
    run_sql_file_withdb $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2 $ha_test2

    sleep 5 # wait for flush checkpoint
    echo "use sync_diff_inspector to check increment data"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 50 || print_debug_status
    check_sync_diff $WORK_DIR $cur/conf/diff_config_multi_task.toml 50 || print_debug_status
    echo "[$(date)] <<<<<< finish test_multi_task_running >>>>>>"
}

function print_debug_status() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT3" \
        "query-status test" \
        "fail me!" 1 && \
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT3" \
        "query-status test2" \
        "fail me!" 1 && exit 1
}


function test_join_masters_and_worker {
    echo "[$(date)] <<<<<< start test_join_masters_and_worker >>>>>>"
    cleanup

    run_dm_master $WORK_DIR/master-join1 $MASTER_PORT1 $cur/conf/dm-master-join1.toml
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT1

    echo "query-status from unique master"
    run_dm_ctl_with_retry $WORK_DIR 127.0.0.1:$MASTER_PORT1 "query-status" '"result": true' 1

    run_dm_master $WORK_DIR/master-join2 $MASTER_PORT2 $cur/conf/dm-master-join2.toml
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT2
    sleep 5
    run_dm_master $WORK_DIR/master-join3 $MASTER_PORT3 $cur/conf/dm-master-join3.toml
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT3
    sleep 5
    run_dm_master $WORK_DIR/master-join4 $MASTER_PORT4 $cur/conf/dm-master-join4.toml
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT4
    sleep 5
    run_dm_master $WORK_DIR/master-join5 $MASTER_PORT5 $cur/conf/dm-master-join5.toml
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT5

    run_dm_ctl_with_retry $WORK_DIR 127.0.0.1:$MASTER_PORT2 "query-status" '"result": true' 1
    run_dm_ctl_with_retry $WORK_DIR 127.0.0.1:$MASTER_PORT3 "query-status" '"result": true' 1
    run_dm_ctl_with_retry $WORK_DIR 127.0.0.1:$MASTER_PORT4 "query-status" '"result": true' 1
    run_dm_ctl_with_retry $WORK_DIR 127.0.0.1:$MASTER_PORT5 "query-status" '"result": true' 1

    echo "join worker with dm-master1 endpoint"
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker-join2.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

    run_dm_ctl_with_retry $WORK_DIR 127.0.0.1:$MASTER_PORT2 "list-member --worker --name=worker2" '"stage": "free",' 1

    echo "kill dm-master-join1"
    ps aux | grep dm-master-join1 | awk '{print $2}' | xargs kill || true
    check_port_offline $MASTER_PORT1 20
    rm -rf $WORK_DIR/master1/default.master1

    run_dm_ctl_with_retry $WORK_DIR 127.0.0.1:$MASTER_PORT2 "list-member --worker --name=worker2" '"stage": "free",' 1

    echo "join worker with 5 masters endpoint"
    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker-join1.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    
    echo "query-status from master2"
    run_dm_ctl_with_retry $WORK_DIR 127.0.0.1:$MASTER_PORT2 "query-status" '"result": true' 1

    echo "[$(date)] <<<<<< finish test_join_masters_and_worker >>>>>>"
}


function test_kill_master() {
    echo "[$(date)] <<<<<< start test_kill_master >>>>>>"
    test_running

    echo "kill dm-master1"
    ps aux | grep dm-master1 | awk '{print $2}' | xargs kill || true
    check_port_offline $MASTER_PORT1 20
    rm -rf $WORK_DIR/master1/default.master1

    echo "waiting 5 seconds"
    sleep 5
    echo "check task is running"
    check_http_alive 127.0.0.1:$MASTER_PORT2/apis/${API_VERSION}/status/test '"stage": "Running"' 10

    echo "check master2,3 are running"
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT2" \
        "query-status test" \
        "\"stage\": \"Running\"" 4

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT3" \
        "query-status test" \
        "\"stage\": \"Running\"" 4

    run_sql_file_withdb $cur/data/db1.increment2.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1 $ha_test
    run_sql_file_withdb $cur/data/db2.increment2.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2 $ha_test
    sleep 2

    echo "use sync_diff_inspector to check increment2 data now!"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
    echo "[$(date)] <<<<<< finish test_kill_master >>>>>>"
}


function test_kill_and_isolate_worker() {
    echo "[$(date)] <<<<<< start test_kill_and_isolate_worker >>>>>>"
    test_running


    echo "kill dm-worker2"
    ps aux | grep dm-worker2 |awk '{print $2}'|xargs kill || true
    check_port_offline $WORKER2_PORT 20
    rm -rf $WORK_DIR/worker2/relay_log
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT1" \
    "query-status test" \
    "\"result\": false" 1

    run_dm_worker $WORK_DIR/worker3 $WORKER3_PORT $cur/conf/dm-worker3.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER3_PORT

    echo "wait and check task running"
    check_http_alive 127.0.0.1:$MASTER_PORT/apis/${API_VERSION}/status/test '"stage": "Running"' 10

    run_dm_worker $WORK_DIR/worker4 $WORKER4_PORT $cur/conf/dm-worker4.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER4_PORT

    echo "restart dm-worker3"
    ps aux | grep dm-worker3 |awk '{print $2}'|xargs kill || true
    check_port_offline $WORKER3_PORT 20
    rm -rf $WORK_DIR/worker3/relay_log

    echo "wait and check task running"
    check_http_alive 127.0.0.1:$MASTER_PORT/apis/${API_VERSION}/status/test '"stage": "Running"' 10

    run_dm_worker $WORK_DIR/worker3 $WORKER3_PORT $cur/conf/dm-worker3.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER3_PORT

    echo "isolate dm-worker4"
    isolate_worker 4 "isolate"
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT1" \
        "query-status test" \
        "\"stage\": \"Running\"" 4

    echo "isolate dm-worker3"
    isolate_worker 3 "isolate"
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT1" \
        "query-status test" \
        "\"stage\": \"Running\"" 2 \
        "\"result\": false" 1
    
    echo "disable isolate dm-worker4"
    isolate_worker 4 "disable_isolate"
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT1" \
        "query-status test" \
        "\"stage\": \"Running\"" 4

    echo "query-status from all dm-master"
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT1" \
        "query-status test" \
        "\"stage\": \"Running\"" 4

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT2" \
        "query-status test" \
        "\"stage\": \"Running\"" 4

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT3" \
        "query-status test" \
        "\"stage\": \"Running\"" 4
    
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "pause-task test"\
        "\"result\": true" 3
    
    echo "restart worker4"
    ps aux | grep dm-worker4 |awk '{print $2}'|xargs kill || true
    check_port_offline $WORKER4_PORT 20
    rm -rf $WORK_DIR/worker4/relay_log
    run_dm_worker $WORK_DIR/worker4 $WORKER4_PORT $cur/conf/dm-worker4.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER4_PORT

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "resume-task test"\
        "\"result\": true" 3

    run_sql_file_withdb $cur/data/db1.increment2.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1 $ha_test
    run_sql_file_withdb $cur/data/db2.increment2.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2 $ha_test
    sleep 2

    echo "use sync_diff_inspector to check increment2 data now!"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
    echo "[$(date)] <<<<<< finish test_kill_and_isolate_worker >>>>>>"
}

# usage: test_kill_master_in_sync leader
# or: test_kill_master_in_sync follower (default)
function test_kill_master_in_sync() {
    echo "[$(date)] <<<<<< start test_kill_master_in_sync >>>>>>"
    test_running

    echo "start dumping SQLs into source"
    load_data $MYSQL_PORT1 $MYSQL_PASSWORD1 "a" &
    load_data $MYSQL_PORT2 $MYSQL_PASSWORD2 "b" &

    ps aux | grep dm-master1 |awk '{print $2}'|xargs kill || true
    check_port_offline $MASTER_PORT1 20

    echo "wait and check task running"
    sleep 1
    check_http_alive 127.0.0.1:$MASTER_PORT2/apis/${API_VERSION}/status/test '"stage": "Running"' 10
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT2" \
        "query-status test" \
        "\"stage\": \"Running\"" 4

    # waiting for syncing
    wait

    echo "wait for dm to sync"
    sleep 1
    echo "use sync_diff_inspector to check data now!"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
    echo "[$(date)] <<<<<< finish test_kill_master_in_sync >>>>>>"
}

function test_kill_worker_in_sync() {
    echo "[$(date)] <<<<<< start test_kill_worker_in_sync >>>>>>"
    test_running

    echo "start dumping SQLs into source"
    load_data $MYSQL_PORT1 $MYSQL_PASSWORD1 "a" &
    load_data $MYSQL_PORT2 $MYSQL_PASSWORD2 "b" &

    echo "kill dm-worker1"
    ps aux | grep dm-worker1 |awk '{print $2}'|xargs kill || true
    echo "kill dm-worker2"
    ps aux | grep dm-worker2 |awk '{print $2}'|xargs kill || true
    echo "start worker3"
    run_dm_worker $WORK_DIR/worker3 $WORKER3_PORT $cur/conf/dm-worker3.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER3_PORT
    echo "start worker4"
    run_dm_worker $WORK_DIR/worker4 $WORKER4_PORT $cur/conf/dm-worker4.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER4_PORT


    echo "wait and check task running"
    check_http_alive 127.0.0.1:$MASTER_PORT/apis/${API_VERSION}/status/test '"stage": "Running"' 10

    echo "query-status from all dm-master"
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT1" \
        "query-status test" \
        "\"stage\": \"Running\"" 4

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT2" \
        "query-status test" \
        "\"stage\": \"Running\"" 4

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT3" \
        "query-status test" \
        "\"stage\": \"Running\"" 4

    # waiting for syncing
    wait

    echo "wait for dm to sync"
    sleep 1

    echo "use sync_diff_inspector to check data now!"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
    echo "[$(date)] <<<<<< finish test_kill_worker_in_sync >>>>>>"
}

function test_standalone_running() {
    echo "[$(date)] <<<<<< start test_standalone_running >>>>>>"
    cleanup
    prepare_sql
    start_standalone_cluster

    echo "use sync_diff_inspector to check full dump loader"
    check_sync_diff $WORK_DIR $cur/conf/diff-standalone-config.toml

    echo "flush logs to force rotate binlog file"
    run_sql "flush logs;" $MYSQL_PORT1 $MYSQL_PASSWORD1

    echo "apply increment data before restart dm-worker to ensure entering increment phase"
    run_sql_file_withdb $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1 $ha_test

    echo "use sync_diff_inspector to check increment data"
    check_sync_diff $WORK_DIR $cur/conf/diff-standalone-config.toml

    cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
    sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker2/relay_log" $WORK_DIR/source2.yaml
    dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "start-task $cur/conf/standalone-task2.yaml" \
        "\"result\": false" 1

    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "start-task $cur/conf/standalone-task2.yaml" \
        "\"result\": true" 2 \
        "\"source\": \"$SOURCE_ID2\"" 1

    worker=$($PWD/bin/dmctl.test DEVEL --master-addr "127.0.0.1:$MASTER_PORT" query-status test2 \
        | grep 'worker' | awk -F: '{print $2}')
    worker_name=${worker:0-9:7}
    worker_idx=${worker_name:0-1:1}
    worker_ports=(0 WORKER1_PORT WORKER2_PORT)

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status"\
        "\"taskStatus\": \"Running\"" 2
    
    echo "kill $worker_name"
    ps aux | grep dm-worker${worker_idx} |awk '{print $2}'|xargs kill || true
    check_port_offline ${worker_ports[$worker_idx]} 20
    rm -rf $WORK_DIR/worker${worker_idx}/relay_log

    # test running, test2 fail
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status"\
        "\"taskStatus\": \"Running\"" 1
    
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "stop-task test2"\
        "\"result\": true" 1
    
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "start-task $cur/conf/standalone-task2.yaml"\
        "\"result\": false" 1

    # test should still running
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status test"\
        "\"stage\": \"Running\"" 2

    echo "[$(date)] <<<<<< finish test_standalone_running >>>>>>"
}


function test_pause_task() {
    echo "[$(date)] <<<<<< start test_pause_task >>>>>>"
    test_multi_task_running

    echo "start dumping SQLs into source"
    load_data $MYSQL_PORT1 $MYSQL_PASSWORD1 "a" &
    load_data $MYSQL_PORT2 $MYSQL_PASSWORD2 "b" &

    task_name=(test test2)
    for name in ${task_name[@]}; do
        echo "pause tasks $name"

        # because some SQL may running (often remove checkpoint record), pause will cause that SQL failed
        # thus `result` is not true
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "pause-task $name"

        # pause twice, just used to test pause by the way
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "pause-task $name"
        
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status $name"\
            "\"stage\": \"Paused\"" 2
    done

    sleep 1

    for name in ${task_name[@]}; do
        echo "resume tasks $name"
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "resume-task $name"\
            "\"result\": true" 3

        # resume twice, just used to test resume by the way
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "resume-task $name"\
            "\"result\": true" 3
        
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status $name"\
            "\"stage\": \"Running\"" 4
    done

    # waiting for syncing
    wait
    sleep 1

    echo "use sync_diff_inspector to check increment data"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
    check_sync_diff $WORK_DIR $cur/conf/diff_config_multi_task.toml
    echo "[$(date)] <<<<<< finish test_pause_task >>>>>>"
}

function test_stop_task() {
    echo "[$(date)] <<<<<< start test_stop_task >>>>>>"
    test_multi_task_running

    echo "start dumping SQLs into source"
    load_data $MYSQL_PORT1 $MYSQL_PASSWORD1 "a" &
    load_data $MYSQL_PORT2 $MYSQL_PASSWORD2 "b" &

    task_name=(test test2)
    task_config=(dm-task.yaml dm-task2.yaml)
    for name in ${task_name[@]}; do
        echo "stop tasks $name"
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task $name"\
            "\"result\": true" 3
        
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status $name"\
            "\"result\": false" 1
    done

    sleep 1

    run_dm_ctl_with_retry $WORK_DIR 127.0.0.1:$MASTER_PORT2 "list-member --worker" '"stage": "bound",' 2

    for idx in $(seq 0 1); do
        echo "start tasks $cur/conf/${task_config[$idx]}"
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/${task_config[$idx]}"\
            "\"result\": true" 3 \
            "\"source\": \"$SOURCE_ID1\"" 1 \
            "\"source\": \"$SOURCE_ID2\"" 1

        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status ${task_name[$idx]}"\
        "\"stage\": \"Running\"" 4
    done

    # waiting for syncing
    wait
    sleep 1

    echo "use sync_diff_inspector to check increment data"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
    check_sync_diff $WORK_DIR $cur/conf/diff_config_multi_task.toml
    echo "[$(date)] <<<<<< finish test_stop_task >>>>>>"
}


function test_multi_task_reduce_and_restart_worker() {
    echo "[$(date)] <<<<<< start test_multi_task_reduce_and_restart_worker >>>>>>"
    test_multi_task_running

    echo "start dumping SQLs into source"
    load_data $MYSQL_PORT1 $MYSQL_PASSWORD1 "a" &
    load_data $MYSQL_PORT2 $MYSQL_PASSWORD2 "b" &
    load_data $MYSQL_PORT1 $MYSQL_PASSWORD1 "a" $ha_test2 &
    load_data $MYSQL_PORT2 $MYSQL_PASSWORD2 "b" $ha_test2 &
    worker_ports=($WORKER1_PORT $WORKER2_PORT $WORKER3_PORT $WORKER4_PORT $WORKER5_PORT)

    # find which worker is in use
    task_name=(test test2)
    worker_inuse=("")     # such as ("worker1" "worker4")
    status=$($PWD/bin/dmctl.test DEVEL --master-addr "127.0.0.1:$MASTER_PORT" query-status test \
        | grep 'worker' | awk -F: '{print $2}')
    echo $status
    for w in ${status[@]}; do
        worker_inuse=(${worker_inuse[*]} ${w:0-9:7})
        echo "find workers: ${w:0-9:7} for task: test"
    done
    echo "find all workers: ${worker_inuse[@]} (total: ${#worker_inuse[@]})"

    for idx in $(seq 1 5); do
        if [[ ! " ${worker_inuse[@]} " =~ " worker${idx} " ]]; then
            echo "restart unuse worker${idx}"

            echo "try to kill worker port ${worker_ports[$[ $idx - 1 ] ]}"
            ps aux | grep dm-worker${idx} |awk '{print $2}'|xargs kill || true
            run_dm_ctl_with_retry $WORK_DIR 127.0.0.1:$MASTER_PORT2 "list-member --worker --name=worker$idx" '"stage": "offline"' 1

            echo "start dm-worker${idx}"
            run_dm_worker $WORK_DIR/worker${idx} ${worker_ports[$[ $idx - 1] ]} $cur/conf/dm-worker${idx}.toml
            check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:${worker_ports[$[ $idx - 1] ]}
        fi
    done

    for ((i=0; i < ${#worker_inuse[@]}; i++)); do
        wk=${worker_inuse[$i]:0-1:1} # get worker id, such as ("1", "4")
        echo "try to kill worker port ${worker_ports[$[ $wk - 1 ] ]}" # get relative worker port 
        ps aux | grep dm-${worker_inuse[$i]} |awk '{print $2}'|xargs kill || true
        check_port_offline ${worker_ports[$[ $wk - 1] ]} 20
        # just one worker was killed should be safe
        echo "${worker_inuse[$i]} was killed"
        if [ $i = 0 ]; then
            for name in ${task_name[@]}; do
                run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
                    "query-status $name"\
                    "\"stage\": \"Running\"" 4
            done

            # waiting for syncing
            wait
            sleep 2
            echo "use sync_diff_inspector to check increment data"
            check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
            check_sync_diff $WORK_DIR $cur/conf/diff_config_multi_task.toml
            echo "data checked after one worker was killed"
        else
            status_str=""
            for name in ${task_name[@]}; do
                status_str=$status_str$($PWD/bin/dmctl.test DEVEL --master-addr "127.0.0.1":$MASTER_PORT query-status $name)
            done
            search_str="\"stage\": \"Running\""
            running_count=$(echo $status_str | sed "s/$search_str/$search_str\n/g" | grep -c "$search_str")
            if [ $running_count != 8 ]; then
                echo "error running worker"
                echo $status_str
                exit 1
            fi
        fi
    done
    echo "[$(date)] <<<<<< finish test_multi_task_reduce_and_restart_worker >>>>>>"
}


function test_isolate_master_and_worker() {
    echo "[$(date)] <<<<<< start test_isolate_master_and_worker >>>>>>"

    test_multi_task_running

    # join master4 and master5
    run_dm_master $WORK_DIR/master-join4 $MASTER_PORT4 $cur/conf/dm-master-join4.toml
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT4
    sleep 5
    run_dm_master $WORK_DIR/master-join5 $MASTER_PORT5 $cur/conf/dm-master-join5.toml
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT5
    sleep 5

    master_ports=($MASTER_PORT1 $MASTER_PORT2 $MASTER_PORT3 $MASTER_PORT4 $MASTER_PORT5)
    alive=(1 2 3 4 5)

    leader=($(get_leader $WORK_DIR 127.0.0.1:${master_ports[${alive[0]}-1]}))
    leader_idx=${leader:6}
    echo "try to isolate leader dm-master$leader_idx"
    isolate_master $leader_idx "isolate"
    for idx in "${!alive[@]}"; do
        if [[ ${alive[idx]} = $leader_idx ]]; then
            unset 'alive[idx]'
        fi
    done
    alive=("${alive[@]}")

    new_leader=($(get_leader $WORK_DIR 127.0.0.1:${master_ports[${alive[0]}-1]}))
    new_leader_idx=${new_leader:6}
    new_leader_port=${master_ports[$new_leader_idx-1]}
    follower_idx=${alive[0]}
    if [[ $follower_idx = $new_leader_idx ]]; then
        follower_idx=${alive[1]}
    fi

    echo "try to isolate follower dm-master$follower_idx"
    isolate_master $follower_idx "isolate"
    for idx in "${!alive[@]}"; do
        if [[ ${alive[idx]} = $follower_idx ]]; then
            unset 'alive[idx]'
        fi
    done
    alive=("${alive[@]}")

    # find which worker is in use
    task_name=(test test2)
    worker_inuse=("")     # such as ("worker1" "worker4")
    status=$($PWD/bin/dmctl.test DEVEL --master-addr "127.0.0.1:$new_leader_port" query-status test \
        | grep 'worker' | awk -F: '{print $2}')
    echo $status
    for w in ${status[@]}; do
        worker_inuse=(${worker_inuse[*]} ${w:0-9:7})
        echo "find workers: ${w:0-9:7} for task: test"
    done
    echo "find all workers: ${worker_inuse[@]} (total: ${#worker_inuse[@]})"

    for ((i=0; i < ${#worker_inuse[@]}; i++)); do
        wk=${worker_inuse[$i]:0-1:1} # get worker id, such as ("1", "4")
        echo "try to isolate dm-worker$wk"
        isolate_worker $wk "isolate"
    done

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$new_leader_port" \
            "pause-task test"\
            "\"result\": true" 3

    run_sql "DROP TABLE if exists $ha_test.ta;" $TIDB_PORT $TIDB_PASSWORD
    run_sql "DROP TABLE if exists $ha_test.tb;" $TIDB_PORT $TIDB_PASSWORD
    load_data $MYSQL_PORT1 $MYSQL_PASSWORD1 "a" &
    load_data $MYSQL_PORT2 $MYSQL_PASSWORD2 "b" &

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$new_leader_port" \
        "resume-task test"\
        "\"result\": true" 3 # wait for load data wait
    wait
    sleep 3

    echo "use sync_diff_inspector to check increment data"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    echo "[$(date)] <<<<<< finish test_isolate_master_and_worker >>>>>>"
}

function test_config_name() {
    echo "[$(date)] <<<<<< start test_config_name >>>>>>"

    cp $cur/conf/dm-master-join2.toml $WORK_DIR/dm-master-join2.toml
    sed -i "s/name = \"master2\"/name = \"master1\"/g" $WORK_DIR/dm-master-join2.toml
    run_dm_master $WORK_DIR/master-join1 $MASTER_PORT1 $cur/conf/dm-master-join1.toml
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT1
    run_dm_master $WORK_DIR/master-join2 $MASTER_PORT2 $WORK_DIR/dm-master-join2.toml
    check_log_contain_with_retry "missing data or joining a duplicate member master1" $WORK_DIR/master-join2/log/dm-master.log

    TEST_CHAR="!@#$%^\&*()_+¥"
    cp $cur/conf/dm-master-join2.toml $WORK_DIR/dm-master-join2.toml
    sed -i "s/name = \"master2\"/name = \"test$TEST_CHAR\"/g" $WORK_DIR/dm-master-join2.toml
    run_dm_master $WORK_DIR/master-join2 $MASTER_PORT2 $WORK_DIR/dm-master-join2.toml
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT2

    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

    cp $cur/conf/dm-worker2.toml $WORK_DIR/dm-worker2.toml
    sed -i "s/name = \"worker2\"/name = \"worker1\"/g" $WORK_DIR/dm-worker2.toml
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $WORK_DIR/dm-worker2.toml
    sleep 2

    check_log_contain_with_retry "[dm-worker with name {\"name\":\"worker1\",\"addr\":\"127.0.0.1:8262\"} already exists]" $WORK_DIR/worker2/log/dm-worker.log

    cp $cur/conf/dm-worker2.toml $WORK_DIR/dm-worker2.toml
    sed -i "s/name = \"worker2\"/name = \"master1\"/g" $WORK_DIR/dm-worker2.toml
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $WORK_DIR/dm-worker2.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

    echo "[$(date)] <<<<<< finish test_config_name >>>>>>"
}

function test_last_bound() {
    echo "[$(date)] <<<<<< start test_last_bound >>>>>>"
    test_running

    worker1bound=$($PWD/bin/dmctl.test DEVEL --master-addr "127.0.0.1:$MASTER_PORT1" list-member --name worker1 \
        | grep 'source' | awk -F: '{print $2}')
    echo "worker1bound $worker1bound"
    worker2bound=$($PWD/bin/dmctl.test DEVEL --master-addr "127.0.0.1:$MASTER_PORT1" list-member --name worker2 \
        | grep 'source' | awk -F: '{print $2}')
    echo "worker2bound $worker2bound"

    kill_2_worker_ensure_unbound 1 2

    # start 1 then 2
    start_2_worker_ensure_bound 1 2

    check_bound
    # only contains 1 "will try purge ..." which is printed the first time dm worker start
    check_log_contains $WORK_DIR/worker1/log/dm-worker.log "will try purge whole relay dir for new relay log" 1
    check_log_contains $WORK_DIR/worker2/log/dm-worker.log "will try purge whole relay dir for new relay log" 1

    kill_2_worker_ensure_unbound 1 2

    # start 2 then 1
    start_2_worker_ensure_bound 2 1

    check_bound
    check_log_contains $WORK_DIR/worker1/log/dm-worker.log "will try purge whole relay dir for new relay log" 1
    check_log_contains $WORK_DIR/worker2/log/dm-worker.log "will try purge whole relay dir for new relay log" 1

    # kill 12, start 34, kill 34
    kill_2_worker_ensure_unbound 1 2
    start_2_worker_ensure_bound 3 4
    kill_2_worker_ensure_unbound 3 4

    # start 1 then 2
    start_2_worker_ensure_bound 1 2

    # check
    check_bound
    check_log_contains $WORK_DIR/worker1/log/dm-worker.log "will try purge whole relay dir for new relay log" 1
    check_log_contains $WORK_DIR/worker2/log/dm-worker.log "will try purge whole relay dir for new relay log" 1

    echo "[$(date)] <<<<<< finish test_last_bound >>>>>>"
}

function run() {
    test_last_bound
    test_config_name                           # TICASE-915, 916, 954, 955
    test_join_masters_and_worker               # TICASE-928, 930, 931, 961, 932, 957
    test_kill_master                           # TICASE-996, 958
    test_kill_and_isolate_worker               # TICASE-968, 973, 1002, 975, 969, 972, 974, 970, 971, 976, 978, 988
    test_kill_master_in_sync
    test_kill_worker_in_sync
    test_standalone_running                    # TICASE-929, 959, 960, 967, 977, 980, 983
    test_pause_task                            # TICASE-990
    test_multi_task_reduce_and_restart_worker  # TICASE-968, 994, 995, 964, 966, 979, 981, 982, 985, 986, 989, 993
    test_isolate_master_and_worker             # TICASE-934, 935, 936, 987, 992, 998, 999
    test_stop_task                             # TICASE-991, 984
}


cleanup_data $ha_test
cleanup_data $ha_test2
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
