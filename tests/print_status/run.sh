#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

function run() {
    run_sql_file $cur/data/db.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1

    # in load stage, the dumped file split into 14 insert segments, we slow down 14 * 100 ms
    # in sync stage, there are 92 group of binlog events, including an XIDEvent,
    # TableMapEvent, QueryEvent, GTIDEvent, and a specific Event in each group.
    # so we slow down 460 * 4 ms. Besides the log may be not flushed to disk asap,
    # we need to add some retry mechanism
    inject_points=("github.com/pingcap/dm/loader/PrintStatusCheckSeconds=return(1)"
                   "github.com/pingcap/dm/syncer/PrintStatusCheckSeconds=return(1)"
                   "github.com/pingcap/dm/loader/LoadDataSlowDown=sleep(100)"
                   "github.com/pingcap/dm/syncer/ProcessBinlogSlowDown=sleep(4)")
    export GO_FAILPOINTS="$(join_string \; ${inject_points[@]})"

    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT

    # start DM task only
    dmctl_start_task_standalone

    # use sync_diff_inspector to check full dump loader
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    run_sql_file $cur/data/db.increment.sql $MYSQL_HOST1 $MYSQL_PORT1
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function check_print_status() {
    # wait for all dm-worker's log flushed to disk
    i=0
    while [ $i -lt 3 ]
    do
        exit_log=$(grep "dm-worker exit" $WORK_DIR/worker1/log/dm-worker.log || echo "not found")
        if [ "$exit_log" == "not found" ]; then
            echo "wait for dm-worker exit log for the $i-th time"
            sleep 1
        else
            break
        fi
    done
    if [ $i -ge 3 ]; then
        echo "wait for dm-worker exit log timeout"
        exit 1
    fi

    # check load unit print status
    status_file=$WORK_DIR/worker1/log/loader_status.log
    grep -oP "loader.*\Kfinished_bytes = [0-9]+, total_bytes = [0-9]+, total_file_count = [0-9]+, progress = .*" $WORK_DIR/worker1/log/dm-worker.log > $status_file
    status_count=$(wc -l $status_file|awk '{print $1}')
    [ $status_count -ge 2 ]
    count=0
    cat $status_file|while read -r line; do
        total_file_count=$(echo "$line"|awk '{print $(NF-4)}'|tr -d ",")
        [ $total_file_count -eq 3 ]
        count=$((count+1))
        if [ $count -eq $status_count ]; then
            finished_bytes=$(echo "$line"|awk '{print $3}'|tr -d ",")
            total_bytes=$(echo "$line"|awk '{print $6}'|tr -d ",")
            [[ "$finished_bytes" -eq "$total_bytes" ]]
        fi
    done
    echo "check load unit print status success"

    # check sync unit print status
    status_file2=$WORK_DIR/worker1/log/syncer_status.log
    grep -oP "syncer.*\Ktotal events = [0-9]+, total tps = [0-9]+, recent tps = [0-9]+, master-binlog = .*" $WORK_DIR/worker1/log/dm-worker.log > $status_file2
    status_count2=$(wc -l $status_file2|awk '{print $1}')
    [ $status_count2 -ge 1 ]
    echo "check sync unit print status success"
}

cleanup1 $TEST_NAME
# also cleanup dm processes in case of last run failed
cleanup2 $*
run $*
cleanup2 $*

wait_process_exit dm-master.test
wait_process_exit dm-worker.test

check_print_status $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
