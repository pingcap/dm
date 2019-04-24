#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

function run() {
    run_sql_file $cur/data/db.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1

    # in load stage, the dumped file split into 14 insert segments, we slow down 14 * 100 ms
    # in sync stage, there are approximately 500~500 binlog events, we slow down 3 * 500 ms
    inject_points=("github.com/pingcap/dm/loader/PrintStatusCheckSeconds=return(1)"
                   "github.com/pingcap/dm/syncer/PrintStatusCheckSeconds=return(1)"
                   "github.com/pingcap/dm/loader/LoadDataSlowDown=return(100)"
                   "github.com/pingcap/dm/syncer/ProcessBinlogSlowDown=return(3)")
    export GO_FAILPOINTS="$(join_string \; ${inject_points[@]})"

    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml

    check_port_alive $MASTER_PORT
    check_port_alive $WORKER1_PORT

    # start DM task only
    $cur/../bin/dmctl_start_task "$cur/conf/dm-task.yaml"

    # use sync_diff_inspector to check full dump loader
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    status_file=$WORK_DIR/worker1/log/loader_status.log
    grep -oP "loader.*\Kfinished_bytes = [0-9]+, total_bytes = [0-9]+, total_file_count = [0-9]+, progress = .*" $WORK_DIR/worker1/log/dm-worker.log > $status_file
    status_count=$(wc -l $status_file|awk '{print $1}')
    [ $status_count -ge 2 ]
    count=0
    cat $status_file|while read -r line; do
        total_file_count=$(echo "$line"|awk '{print $(NF-4)}'|tr -d ",")
        [ $total_file_count -eq 3 ]
        ((count+=1))
        if [ $count -eq $status_count ]; then
            finished_bytes=$(echo "$line"|awk '{print $3}'|tr -d ",")
            total_bytes=$(echo "$line"|awk '{print $6}'|tr -d ",")
            [[ "$finished_bytes" -eq "$total_bytes" ]]
        fi
    done

    run_sql_file $cur/data/db.increment.sql $MYSQL_HOST1 $MYSQL_PORT1

    # use sync_diff_inspector to check data now!
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
