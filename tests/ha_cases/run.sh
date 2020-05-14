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
        "\"stage\": \"Running\"" 2 \
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
        "\"stage\": \"Running\"" 2 \
        "\"unit\": \"Sync\"" 2
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT3" \
        "query-status test2" \
        "\"stage\": \"Running\"" 2 \
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

    sleep 3 # wait for flush checkpoint
    echo "use sync_diff_inspector to check increment data"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 3
    check_sync_diff $WORK_DIR $cur/conf/diff_config_multi_task.toml 3
    echo "[$(date)] <<<<<< finish test_multi_task_running >>>>>>"
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

    echo "kill dm-master-join1"
    ps aux | grep dm-master-join1 | awk '{print $2}' | xargs kill || true
    check_port_offline $MASTER_PORT1 20
    rm -rf $WORK_DIR/master1/default.master1

    sleep 5

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
    check_http_alive 127.0.0.1:$MASTER_PORT2/apis/${API_VERSION}/status/test '"name":"test","stage":"Running"' 10

    echo "check master2,3 are running"
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT2" \
        "query-status test" \
        "\"stage\": \"Running\"" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT3" \
        "query-status test" \
        "\"stage\": \"Running\"" 2

    run_sql_file_withdb $cur/data/db1.increment2.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1 $ha_test
    run_sql_file_withdb $cur/data/db2.increment2.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2 $ha_test
    sleep 2

    echo "use sync_diff_inspector to check increment2 data now!"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
    echo "[$(date)] <<<<<< finish test_kill_master >>>>>>"
}


function test_kill_worker() {
    echo "[$(date)] <<<<<< start test_kill_worker >>>>>>"
    test_running


    echo "kill dm-worker2"
    ps aux | grep dm-worker2 |awk '{print $2}'|xargs kill || true
    check_port_offline $WORKER2_PORT 20
    rm -rf $WORK_DIR/worker2/relay_log

    run_dm_worker $WORK_DIR/worker3 $WORKER3_PORT $cur/conf/dm-worker3.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER3_PORT

    echo "wait and check task running"
    check_http_alive 127.0.0.1:$MASTER_PORT/apis/${API_VERSION}/status/test '"name":"test","stage":"Running"' 10

    run_dm_worker $WORK_DIR/worker4 $WORKER4_PORT $cur/conf/dm-worker4.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER4_PORT

    echo "kill dm-worker3"
    ps aux | grep dm-worker3 |awk '{print $2}'|xargs kill || true
    check_port_offline $WORKER3_PORT 20
    rm -rf $WORK_DIR/worker3/relay_log

    echo "wait and check task running"
    check_http_alive 127.0.0.1:$MASTER_PORT/apis/${API_VERSION}/status/test '"name":"test","stage":"Running"' 10

    echo "query-status from all dm-master"
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT1" \
        "query-status test" \
        "\"stage\": \"Running\"" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT2" \
        "query-status test" \
        "\"stage\": \"Running\"" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT3" \
        "query-status test" \
        "\"stage\": \"Running\"" 2

    run_sql_file_withdb $cur/data/db1.increment2.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1 $ha_test
    run_sql_file_withdb $cur/data/db2.increment2.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2 $ha_test
    sleep 2

    echo "use sync_diff_inspector to check increment2 data now!"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
    echo "[$(date)] <<<<<< finish test_kill_worker >>>>>>"
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
    check_http_alive 127.0.0.1:$MASTER_PORT2/apis/${API_VERSION}/status/test '"name":"test","stage":"Running"' 10
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT2" \
        "query-status test" \
        "\"stage\": \"Running\"" 2

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
    check_http_alive 127.0.0.1:$MASTER_PORT/apis/${API_VERSION}/status/test '"name":"test","stage":"Running"' 10

    echo "query-status from all dm-master"
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT1" \
        "query-status test" \
        "\"stage\": \"Running\"" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT2" \
        "query-status test" \
        "\"stage\": \"Running\"" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT3" \
        "query-status test" \
        "\"stage\": \"Running\"" 2

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
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "pause-task $name"\
            "\"result\": true" 3
        
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
        
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status $name"\
            "\"stage\": \"Running\"" 2
    done

    # waiting for syncing
    wait
    sleep 1

    echo "use sync_diff_inspector to check increment data"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 3
    check_sync_diff $WORK_DIR $cur/conf/diff_config_multi_task.toml 3
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

    for idx in $(seq 0 1); do
        echo "start tasks $cur/conf/${task_config[$idx]}"
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/${task_config[$idx]}"\
            "\"result\": true" 3 \
            "\"source\": \"$SOURCE_ID1\"" 1 \
            "\"source\": \"$SOURCE_ID2\"" 1

        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status ${task_name[$idx]}"\
        "\"stage\": \"Running\"" 2
    done

    # waiting for syncing
    wait
    sleep 1

    echo "use sync_diff_inspector to check increment data"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 3
    check_sync_diff $WORK_DIR $cur/conf/diff_config_multi_task.toml 3
    echo "[$(date)] <<<<<< finish test_stop_task >>>>>>"
}


function test_multi_task_reduce_and_restart_worker() {
    echo "[$(date)] <<<<<< start test_multi_task_reduce_worker >>>>>>"
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
            check_port_offline ${worker_ports[$[ $idx - 1] ]} 20

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
                    "\"stage\": \"Running\"" 2
            done

            # waiting for syncing
            wait
            sleep 2
            echo "use sync_diff_inspector to check increment data"
            check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 3
            check_sync_diff $WORK_DIR $cur/conf/diff_config_multi_task.toml 3
            echo "data checked after one worker was killed"
        else
            status_str=""
            for name in ${task_name[@]}; do
                status_str=$status_str$($PWD/bin/dmctl.test DEVEL --master-addr "127.0.0.1":$MASTER_PORT query-status $name)
            done
            search_str="\"stage\": \"Running\""
            running_count=$(echo $status_str | sed "s/$search_str/$search_str\n/g" | grep -c "$search_str")
            if [ $running_count != 4 ]; then
                echo "error running worker"
                exit 1
            fi
        fi
    done
    echo "[$(date)] <<<<<< finish test_multi_task_reduce_worker >>>>>>"
}


function test_isolate_master() {
    echo "[$(date)] <<<<<< start test_isolate_master >>>>>>"

    test_running

    for idx in $(seq 1 3); do
        echo "try to isolate dm-master$idx"
        isolate_master $idx "isolate"

        port=$MASTER_PORT3
        if [ $idx != 1 ]; then
            port=${master_ports[$[ $idx - 2 ]]}
        fi

        run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$port" \
            "pause-task test"\
            "\"result\": true" 3

        run_sql "DROP TABLE if exists $ha_test.ta;" $TIDB_PORT $TIDB_PASSWORD
        run_sql "DROP TABLE if exists $ha_test.tb;" $TIDB_PORT $TIDB_PASSWORD
        load_data $MYSQL_PORT1 $MYSQL_PASSWORD1 "a" &
        load_data $MYSQL_PORT2 $MYSQL_PASSWORD2 "b" &

        run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$port" \
            "resume-task test"\
            "\"result\": true" 3

        # wait for load data
        wait
        sleep 3

        echo "use sync_diff_inspector to check increment data"
        check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 10

        isolate_master $idx "disable_isolate"
        check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$port
    done


    echo "[$(date)] <<<<<< finish test_isolate_master >>>>>>"
}


function run() {
    test_join_masters_and_worker               # TICASE-928, 930, 931, 961
    test_kill_master                           # TICASE-996
    test_kill_worker                           # TICASE-968, 973, 1002
    test_kill_master_in_sync
    test_kill_worker_in_sync
    test_standalone_running                    # TICASE-929, 959, 960, 967
    test_pause_task                            # TICASE-990
    test_multi_task_reduce_and_restart_worker  # TICASE-968, 994, 995
    test_isolate_master                        # TICASE-934, 935
    test_stop_task                             # TICASE-991
}


cleanup_data $ha_test
cleanup_data $ha_test2
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
