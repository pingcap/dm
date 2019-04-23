#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

function run() {
    run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1
    check_contains 'Query OK, 2 rows affected'
    run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2
    check_contains 'Query OK, 3 rows affected'

    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml

    check_port_alive $MASTER_PORT
    check_port_alive $WORKER1_PORT
    check_port_alive $WORKER2_PORT

    # start DM task only
    $cur/../bin/dmctl_start_task "$cur/conf/dm-task.yaml"

    # TODO: check sharding partition id
    # use sync_diff_inspector to check full dump loader
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1
    run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2

    # TODO: check sharding partition id
    # use sync_diff_inspector to check data now!
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    # test create database, create table in sharding mode
    run_sql_file $cur/data/db1.increment2.sql $MYSQL_HOST1 $MYSQL_PORT1
    run_sql_file $cur/data/db2.increment2.sql $MYSQL_HOST2 $MYSQL_PORT2
    cp $cur/conf/diff_config.toml $WORK_DIR/diff_config.toml
    printf "\n[[table-config.source-tables]]\ninstance-id = \"source-1\"\nschema = \"sharding2\"\ntable  = \"~t.*\"" >> $WORK_DIR/diff_config.toml
    printf "\n[[table-config.source-tables]]\ninstance-id = \"source-2\"\nschema = \"sharding2\"\ntable  = \"~t.*\"" >> $WORK_DIR/diff_config.toml
    check_sync_diff $WORK_DIR $WORK_DIR/diff_config.toml

    # test drop table, drop database in sharding mode
    run_sql_file $cur/data/db1.increment3.sql $MYSQL_HOST1 $MYSQL_PORT1
    run_sql_file $cur/data/db2.increment3.sql $MYSQL_HOST2 $MYSQL_PORT2
    cp $cur/conf/diff_config.toml $WORK_DIR/diff_config.toml
    printf "\n[[table-config.source-tables]]\ninstance-id = \"source-1\"\nschema = \"sharding2\"\ntable  = \"~t.*\"" >> $WORK_DIR/diff_config.toml
    sed -i "s/^# range-placeholder/range = \"uid < 70000\"/g" $WORK_DIR/diff_config.toml
    check_sync_diff $WORK_DIR $WORK_DIR/diff_config.toml
}

cleanup1 db_target
# also cleanup dm processes in case of last run failed
cleanup2 $*
run $*
cleanup2 $*

wait_process_exit dm-master.test
wait_process_exit dm-worker.test

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
