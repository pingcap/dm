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

    echo "use sync_diff_inspector to check full dump loader"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    echo "flush logs to force rotate binlog file"
    run_sql "flush logs;" $MYSQL_PORT1 $MYSQL_PASSWORD1
    run_sql "flush logs;" $MYSQL_PORT2 $MYSQL_PASSWORD2

    echo "apply increment data before restart dm-worker to ensure entering increment phase"
    run_sql_file_withdb $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1 $ha_test
    run_sql_file_withdb $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2 $ha_test

    echo "use sync_diff_inspector to check increment data"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}


function test_multi_task_running() {
    echo "[$(date)] <<<<<< start test_multi_task_running >>>>>>"
    cleanup
    prepare_sql_multi_task
    start_multi_tasks_cluster

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

    echo "use sync_diff_inspector to check increment data"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 3
    check_sync_diff $WORK_DIR $cur/conf/diff_config_multi_task.toml 3
}


function test_join_masters {
    echo "[$(date)] <<<<<< start test_join_multi_masters >>>>>>"
    cleanup

    run_dm_master $WORK_DIR/master-join1 $MASTER_PORT1 $cur/conf/dm-master-join1.toml
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT1
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

    run_dm_ctl_with_retry $WORK_DIR 127.0.0.1:$MASTER_PORT1 "query-status" '"result": true' 1
    run_dm_ctl_with_retry $WORK_DIR 127.0.0.1:$MASTER_PORT2 "query-status" '"result": true' 1
    run_dm_ctl_with_retry $WORK_DIR 127.0.0.1:$MASTER_PORT3 "query-status" '"result": true' 1
    run_dm_ctl_with_retry $WORK_DIR 127.0.0.1:$MASTER_PORT4 "query-status" '"result": true' 1
    run_dm_ctl_with_retry $WORK_DIR 127.0.0.1:$MASTER_PORT5 "query-status" '"result": true' 1
}


function test_kill_master() {
    test_running
    echo "[$(date)] <<<<<< start test_kill_master >>>>>>"

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
}


function test_kill_worker() {
    test_running
    echo "[$(date)] <<<<<< start test_kill_worker >>>>>>"


    echo "kill dm-worker2"
    ps aux | grep dm-worker2 |awk '{print $2}'|xargs kill || true
    check_port_offline $WORKER2_PORT 20
    rm -rf $WORK_DIR/worker2/relay_log

    run_dm_worker $WORK_DIR/worker3 $WORKER3_PORT $cur/conf/dm-worker3.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER3_PORT

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
}

# usage: test_kill_master_in_sync leader
# or: test_kill_master_in_sync follower (default)
function test_kill_master_in_sync() {
    role=false
    test_running
    echo "[$(date)] <<<<<< start test_kill_master_in_sync >>>>>>"

    echo "start dumping random SQLs into source"
    pocket_pid1=$(start_random_sql_to "root:123456@tcp(127.0.0.1:3306)/ha_test" 1 0)
    pocket_pid2=$(start_random_sql_to "root:123456@tcp(127.0.0.1:3307)/ha_test" 1 1)

    sleep 1

    ps aux | grep dm-master2 |awk '{print $2}'|xargs kill || true
    check_port_offline $MASTER_PORT2 20

    run_dm_worker $WORK_DIR/worker4 $WORKER4_PORT $cur/conf/dm-worker4.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER4_PORT

    echo "wait and check task running"
    sleep 1
    check_http_alive 127.0.0.1:$MASTER_PORT1/apis/${API_VERSION}/status/test '"name":"test","stage":"Running"' 10
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT1" \
        "query-status test" \
        "\"stage\": \"Running\"" 2

    sleep 1
    echo "kill tipocket"
    kill $pocket_pid1 $pocket_pid2

    # waiting for syncing
    sleep 1

    # WARN: run ddl sqls spent so long
    sleep 300
    echo $(dmctl --master-addr "127.0.0.1:$MASTER_PORT1" query-status test)

    echo "use sync_diff_inspector to check increment2 data now!"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function test_kill_worker_in_sync() {
    test_running
    echo "[$(date)] <<<<<< start test_kill_worker_in_sync >>>>>>"

    echo "start dumping random SQLs into source"
    pocket_pid1=$(start_random_sql_to "root:123456@tcp(127.0.0.1:3306)/ha_test" 0 0)
    pocket_pid2=$(start_random_sql_to "root:123456@tcp(127.0.0.1:3307)/ha_test" 0 1)

    sleep 1

    echo "start worker3"
    run_dm_worker $WORK_DIR/worker3 $WORKER3_PORT $cur/conf/dm-worker3.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER3_PORT

    echo "kill dm-worker2"
    ps aux | grep dm-worker2 |awk '{print $2}'|xargs kill || true

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

    sleep 1

    echo "kill tipocket"
    kill $pocket_pid1 $pocket_pid2 # if kill fails, means tipocket exited unexceptly

    # waiting for syncing
    sleep 100

    echo "use sync_diff_inspector to check increment2 data now!"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function test_standalone_running() {
    echo "[$(date)] <<<<<< start test_running >>>>>>"
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
}


function test_pause_task() {
    test_multi_task_running
    echo "[$(date)] <<<<<< start test_pause_task >>>>>>"

    echo "start dumping random SQLs into source"
    pocket_pid1=$(start_random_sql_to "root:123456@tcp(127.0.0.1:3306)/ha_test" 0 0)
    pocket_pid2=$(start_random_sql_to "root:123456@tcp(127.0.0.1:3307)/ha_test" 0 1)

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

    sleep 1
    # stop
    kill $pocket_pid1 $pocket_pid2
    sleep 200

    echo "use sync_diff_inspector to check increment data"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 3
    check_sync_diff $WORK_DIR $cur/conf/diff_config_multi_task.toml 3
    echo $(dmctl --master-addr "127.0.0.1":$MASTER_PORT query-status test)
    echo $(dmctl --master-addr "127.0.0.1":$MASTER_PORT query-status test2)
}


function test_multi_task_reduce_worker() {
    test_multi_task_running
    echo "[$(date)] <<<<<< start test_multi_task_reduce_worker >>>>>>"

    echo "start dumping random SQLs into source"
    pocket_pid1=$(start_random_sql_to "root:123456@tcp(127.0.0.1:3306)/ha_test" 0 0)
    pocket_pid2=$(start_random_sql_to "root:123456@tcp(127.0.0.1:3307)/ha_test" 0 1)

    # find which worker is in use
    task_name=(test test2)
    worker_inuse=("")     # such as ("worker1" "worker4")
    for name in ${task_name[@]}; do
        status=$(dmctl --master-addr "127.0.0.1":$MASTER_PORT query-status $name \
            | grep 'worker' | awk -F 'u0007' '{print $2}')
        for w in ${status[@]}; do
            worker_inuse=(${worker_inuse[*]} ${w:0-9:7})
            echo "find workers: ${w:0-9:7} for task: $name"
        done
    done
    echo "find all workers: ${worker_inuse[@]} (total: ${#worker_inuse[@]})"

    for  ((i=0; i < ${#worker_inuse[@]}; i++)); do
        ps aux | grep dm-${worker_inuse[$i]} |awk '{print $2}'|xargs kill || true
        check_port_offline ${WORKER$[$i+1]_PORT} 20
        # just one worker was killed should be safe
        echo "$[$i+1] worker(s) were killed"
        if [ $i = 0 ]; then
            for name in ${task_name[@]}; do
                run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
                    "query-status $name"\
                    "\"stage\": \"Running\"" 2
            done

            # stop
            kill $pocket_pid1 $pocket_pid2

            sleep 10
            echo "use sync_diff_inspector to check increment data"
            check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 3
            check_sync_diff $WORK_DIR $cur/conf/diff_config_multi_task.toml 3
            echo "data checked after one worker was killed"
        else
            status_str=""
            for name in ${task_name[@]}; do
                status_str=$status_str$(dmctl --master-addr "127.0.0.1":$MASTER_PORT query-status $name)
            done
            search_str="\"stage\": \"Running\""
            running_count=$(echo $status_str | sed "s/$search_str/$search_str\n/g" | grep -c "$search_str")
            if [ $running_count != $[4-$i] ]; then
                echo "error running worker"
                exit 1
            fi
        fi
    done

}


function test_isolate_master() {
    role=false
    if [ $1 = "leader" ]; then
        role=true
    fi
    test_running

    echo "[$(date)] <<<<<< start test_isolate_master >>>>>>"

    master_ports=($MASTER_PORT1 $MASTER_PORT2 $MASTER_PORT3)
    leader_index=""         # in general, only one leader
    follower_indeces=( "" )
    for idx in ${!master_ports[@]}; do
        master=$(etcdctl --endpoints='127.0.0.1:'${master_ports[$idx]}'' endpoint status | awk -F ', ' '{print $5}')
        if [ $master = "true" ]; then
            leader_index=$idx
        else
            follower_indeces=(${follower_indeces[*]} $idx)
        fi
    done
    echo "leader idx: $leader_index ; followers idx: $follower_indeces"

    if [ leader_index = "" ]; then
        echo "no leader has been elected"
        exit 1
    fi

    if [ role = true ]; then
        isolate_port ${master_ports[$leader_index]}
        sleep 1
        # check new leader was elected
        elected="no"
        for idx in ${follower_indeces[@]}; do
            master=$(etcdctl --endpoints='127.0.0.1:'${master_ports[$idx]}'' endpoint status | awk -F ', ' '{print $5}')
            if [ $master = "true" ]; then
                elected="yes"
                break
            fi
        done
        if [ elected = "no" ]; then
            echo "no new leader was elected"
            disable_isolate_port ${master_ports[$leader_index]}
            exit 1
        fi
        disable_isolate_port ${master_ports[$leader_index]}
    else
        if [ ${#follower_indeces[@]} = 0 ]; then
            echo "there is not any follower"
        fi
        isolate_port ${master_ports[${follower_indeces[0]}]}
        sleep 1
        # check serve normally
        run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:${master_ports[$leader_index]}" \
        "query-status test" \
        "\"stage\": \"Running\"" 2

        disable_isolate_port ${master_ports[${follower_indeces[0]}]}
    fi
}


function run() {
    # test_join_masters
    # test_kill_master
    # test_kill_worker
    test_kill_master_in_sync
    test_kill_worker_in_sync
    test_standalone_running
    test_pause_task
    test_multi_task_reduce_worker
    test_isolate_master leader
    test_isolate_master follower
}


cleanup_data ha_test
cleanup_data ha_test2
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
