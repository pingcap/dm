#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
PWD=$(pwd)
DB1_PORT=3306
DB2_PORT=3307
TIDB_PORT=4000
MASTER_PORT=8261
WORKER1_PORT=8262
WORKER2_PORT=8263
WORK_DIR=$TEST_DIR/safe_mode

# we do clean staff at beginning of each run, so we can keep logs of the latset run
function cleanup1() {
    rm -rf $WORK_DIR
    mkdir $WORK_DIR
    run_sql "drop database if exists db_target" $TIDB_PORT
    run_sql "drop database if exists dm_meta" $TIDB_PORT
}

function cleanup2() {
    killall dm-worker.test 2>/dev/null || true
    killall dm-master.test 2>/dev/null || true
}

function run() {
    run_sql_file $cur/data/db1.prepare.sql $DB1_PORT
    check_contains 'Query OK, 2 rows affected'
    run_sql_file $cur/data/db2.prepare.sql $DB2_PORT
    check_contains 'Query OK, 3 rows affected'

    cd $cur && GO111MODULE=on go build -o bin/dmctl && cd -

    export GOFAIL_FAILPOINTS='github.com/pingcap/dm/syncer/ReSyncExit=return(true);github.com/pingcap/dm/syncer/WaitShardingSyncExit=return(false)'
    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
    check_port_alive $MASTER_PORT
    check_port_alive $WORKER1_PORT
    check_port_alive $WORKER2_PORT

    $cur/bin/dmctl "$cur/conf/dm-task.yaml"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    # DM-worker exit during re-sync after sharding group synced
    run_sql_file $cur/data/db1.increment.sql $DB1_PORT
    run_sql_file $cur/data/db2.increment.sql $DB2_PORT

    check_port_offline $WORKER1_PORT 20
    check_port_offline $WORKER2_PORT 20

    export GOFAIL_FAILPOINTS='github.com/pingcap/dm/syncer/ReSyncExit=return(false);github.com/pingcap/dm/syncer/WaitShardingSyncExit=return(true)'

    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    check_port_alive $WORKER1_PORT
    check_port_alive $WORKER2_PORT
    $cur/bin/dmctl "$cur/conf/dm-task.yaml"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    # DM-worker exit when waiting for sharding group synced
    run_sql_file $cur/data/db1.increment2.sql $DB1_PORT
    run_sql_file $cur/data/db2.increment2.sql $DB2_PORT

    export GOFAIL_FAILPOINTS='github.com/pingcap/dm/syncer/ReSyncExit=return(false);github.com/pingcap/dm/syncer/WaitShardingSyncExit=return(false)'

    i=0
    while [ $i -lt 10 ]; do
        # we can't determine which DM-worker is the sharding lock owner, so we try both of them
        # DM-worker1 is sharding lock owner and exits
        if [ "$(check_port_return $WORKER1_PORT)" == "0" ]; then
            run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
            check_port_alive $WORKER1_PORT
            break
        fi
        # DM-worker2 is sharding lock owner and exits
        if [ "$(check_port_return $WORKER2_PORT)" == "0" ]; then
            run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
            check_port_alive $WORKER2_PORT
            break
        fi
    done
    if [ $i -ge 10 ]; then
        echo "wait DM-worker offline timeout"
        exit 1
    fi

    $cur/bin/dmctl "$cur/conf/dm-task.yaml"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

cleanup1 $*
# also cleanup dm processes in case of last run failed
cleanup2 $*
run $*
cleanup2 $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
