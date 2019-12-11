#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

run_dm_ctl_contain() {
    workdir=$1
    master_addr=$2
    cmd=$3
    content=$4

    shift 3

    PWD=$(pwd)
    binary=$PWD/bin/dmctl.test
    ts=$(date +"%s")
    dmctl_log=$workdir/dmctl.$ts.log
    pid=$$
    echo "dmctl test cmd: \"$cmd\""
    echo "$cmd" | $binary -test.coverprofile="$TEST_DIR/cov.$TEST_NAME.dmctl.$ts.$pid.out" DEVEL -master-addr=$master_addr > $dmctl_log 2>&1
    dmctl_log_content=`cat $dmctl_log`

    if [[ !($dmctl_log_content =~ $content) ]]; then
        echo "command: $cmd expected to contain: "
        echo $content
        echo "but is: "
        cat $dmctl_log
        exit 1
    fi
}

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
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "table-route $task_conf" \
        "\"routes\"" 1 \
        "\"\`simulator\`.\`t\`\"" 1 \
        "127.0.0.1:3306" 1 \
        "127.0.0.1:3307" 1 \
        "simulator_1" 3 \
        "simulator_2" 3 \
        "simulate_1" 2

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "table-route $task_conf -w 127.0.0.1:$WORKER1_PORT" \
        "\"routes\"" 1 \
        "\"\`simulator\`.\`t\`\"" 1 \
        "127.0.0.1:3306" 1 \
        "127.0.0.1:3307" 0 \
        "simulator_1" 3 \
        "simulator_2" 0 \
        "simulate_1" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "table-route -w 127.0.0.1:$WORKER1_PORT -T \`A\`.\`B\` -T \`A\`.\`C\` $task_conf" \
        "\"ignore-tables\"" 1 \
        "\"routes\"" 0 \
        "\"\`A\`.\`B\`\"" 1 \
        "\"\`A\`.\`C\`\"" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "table-route -w 127.0.0.1:$WORKER1_PORT -T \`simulator_5\`.\`A\` $task_conf" \
        "\"routes\"" 1 \
        "\"reason\": \"user-route-rules-schema\"" 1 \
        "\"table\": \"\`simulator_5\`.\`A\`\"," 1 \
        "\"\`simulator\`.\`A\`\": {" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "table-route -w 127.0.0.1:$WORKER1_PORT -T \`simulator_5\`.\`simulate_3\` -T \`simulator_5\`.\`simulate_4\` $task_conf" \
        "\"routes\"" 1 \
        "\"reason\": \"user-route-rules\"" 2 \
        "\"table\": \"\`simulator_5\`.\`simulate_3\`\"," 1 \
        "\"table\": \"\`simulator_5\`.\`simulate_4\`\"," 1 \
        "\"\`simulator\`.\`t\`\": {" 1

    # black-white-list simulator test
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "bw-list $task_conf" \
        "\"do-tables\":" 1 \
        "\"ignore-tables\":" 1 \
        "127.0.0.1:3306" 2 \
        "127.0.0.1:3307" 2 \
        "simulator_1" 3 \
        "simulator_2" 3 \
        "simulate_1" 2

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "bw-list $task_conf -w 127.0.0.1:$WORKER1_PORT" \
        "\"do-tables\":" 1 \
        "\"ignore-tables\":" 1 \
        "127.0.0.1:3306" 2 \
        "127.0.0.1:3307" 0 \
        "simulator_1" 3 \
        "simulator_2" 0 \
        "simulate_1" 1

    run_dm_ctl_contain $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "bw-list -w 127.0.0.1:$WORKER1_PORT -T \`simulator_5\`.\`A\` -T \`simulator\`.\`t\` $task_conf" \
        .*"\"do-tables\"".*"\"127.0.0.1:3306\"".*"\"\`simulator_5\`.\`A\`\"".*"\"ignore-tables\"".*"\"127.0.0.1:3306\"".*"\"\`simulator\`.\`t\`\"".*

    # binlog-event-filter simulator test
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "event-filter -w 127.0.0.1:$WORKER1_PORT $task_conf create table eventFilter1_1.simulate_1(id int);" \
        "\"will-be-filtered\": \"no\"" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "event-filter -w 127.0.0.1:$WORKER1_PORT $task_conf drop table eventFilter1_1.simulate_1;" \
        "\"will-be-filtered\": \"yes\"" 1 \
        "\"filter-name\": \"user-filter-1\"" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "event-filter -w 127.0.0.1:$WORKER1_PORT $task_conf truncate table eventFilter1_1.simulate_1;" \
        "\"will-be-filtered\": \"yes\"" 1 \
        "\"filter-name\": \"user-filter-1\"" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "event-filter -w 127.0.0.1:$WORKER1_PORT $task_conf create table eventFilter2_1.simulate_1;" \
        "\"will-be-filtered\": \"yes\"" 1 \
        "\"filter-name\": \"user-filter-2\"" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "event-filter -w 127.0.0.1:$WORKER2_PORT $task_conf create table simulate_2_1.simulate_1;" \
        "\"will-be-filtered\": \"no\"" 1
}

cleanup_data simulator
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
