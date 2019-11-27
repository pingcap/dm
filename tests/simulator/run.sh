#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

function run() {
    run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1
    run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2

    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
    run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT

    task_conf="$cur/conf/dm-task.yaml"
    # table-route simulator test
    run-dm-ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "table-route $task_conf" \
        "\"result\": true" 1 \
        "\"routes\"" 1 \
        "\"`simulator`.`t`\"": 1 \
        "127.0.0.1:3306": 1 \
        "127.0.0.1:3307": 1 \
        "simulator_1": 3 \
        "simulator_2": 3 \
        "t_1": 2 \

    run-dm-ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "table-route -w 127.0.0.1:$WORKER1_PORT -T `A`.`B` $task_conf" \
        "\"result\": true" 1 \
        "\"will-be-filtered\": \"yes\"" 1

    run-dm-ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "table-route -w 127.0.0.1:$WORKER1_PORT -T `simulator_5`.`A` $task_conf" \
        "\"result\": true" 1 \
        "\"match-route\": \"user-route-rules-schema\"," 1 \
        "\"target-schema\": \"simulator\"," 1 \
        "\"target-table\": \"A\"" 1

    run-dm-ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "table-route -w 127.0.0.1:$WORKER1_PORT -T `simulator_5`.`A` $task_conf" \
        "\"result\": true" 1 \
        "\"match-route\": \"user-route-rules\"," 1 \
        "\"target-schema\": \"simulator\"," 1 \
        "\"target-table\": \"t\"" 1

    # black-white-list simulator test
    run-dm-ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "bw-list $task_conf" \
        "\"do-tables\":" 1 \
        "\"ignore-tables\":" 1 \
        "\"`simulator`.`t`\"": 2 \
        "127.0.0.1:3306": 2 \
        "127.0.0.1:3307": 2 \
        "simulator_1": 3 \
        "simulator_2": 3 \
        "t_1": 2 \

    run-dm-ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "bw-list -w 127.0.0.1:$WORKER1_PORT -T `simulator_5`.`A` $task_conf" \
        "\"result\": true" 1 \
        "\"will-be-filtered\": \"no\"" 1

    run-dm-ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "bw-list -w 127.0.0.1:$WORKER1_PORT -T `simulator`.`t` $task_conf" \
        "\"result\": true" 1 \
        "\"will-be-filtered\": \"yes\"" 1

    # binlog-event-filter simulator test
    run-dm-ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "event-filter -w 127.0.0.1:$WORKER1_PORT $task_conf create table simulate_1_1.t_1(id int);" \
        "\"result\": true" 1 \
        "\"will-be-filtered\": \"no\"" 1

    run-dm-ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "event-filter -w 127.0.0.1:$WORKER1_PORT $task_conf drop table simulate_1_1.t_1;" \
        "\"result\": true" 1 \
        "\"will-be-filtered\": \"yes\"" 1 \
        "\"filter-name\": \"user-filter-1\"" 1

    run-dm-ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "event-filter -w 127.0.0.1:$WORKER1_PORT $task_conf truncate table simulate_1_1.t_1;" \
        "\"result\": true" 1 \
        "\"will-be-filtered\": \"yes\"" 1 \
        "\"filter-name\": \"user-filter-1\"" 1

    run-dm-ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "event-filter -w 127.0.0.1:$WORKER1_PORT $task_conf create table simulate_2_1.t_1;" \
        "\"result\": true" 1 \
        "\"will-be-filtered\": \"yes\"" 1 \
        "\"filter-name\": \"user-filter-2\"" 1

    run-dm-ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "event-filter -w 127.0.0.1:$WORKER2_PORT $task_conf create table simulate_2_1.t_1;" \
        "\"result\": true" 1 \
        "\"will-be-filtered\": \"no\"" 1
}

cleanup_data simulator
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
