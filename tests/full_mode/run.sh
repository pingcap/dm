#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

function fail_acquire_global_lock() {
    export GO_FAILPOINTS="github.com/pingcap/dm/dm/worker/TaskCheckInterval=return(\"500ms\")"

    run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
    check_contains 'Query OK, 2 rows affected'
    run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
    check_contains 'Query OK, 3 rows affected'

    cp $cur/data/db1.prepare.user.sql $WORK_DIR/db1.prepare.user.sql
    sed -i "/revoke create temporary/i\revoke reload on *.* from 'dm_full'@'%';" $WORK_DIR/db1.prepare.user.sql
    run_sql_file $WORK_DIR/db1.prepare.user.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
    check_count 'Query OK, 0 rows affected' 8
    cp $cur/data/db2.prepare.user.sql $WORK_DIR/db2.prepare.user.sql
    sed -i "/revoke create temporary/i\revoke reload on *.* from 'dm_full'@'%';" $WORK_DIR/db2.prepare.user.sql
    run_sql_file $WORK_DIR/db2.prepare.user.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
    check_count 'Query OK, 0 rows affected' 8

    run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

    cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
    cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
    sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
    sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker2/relay_log" $WORK_DIR/source2.yaml
    dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
    dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

    cp $cur/conf/dm-task.yaml $WORK_DIR/dm-task.yaml
    sed -i '/timezone/i\ignore-checking-items: ["dump_privilege"]' $WORK_DIR/dm-task.yaml
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "start-task $WORK_DIR/dm-task.yaml --remove-meta"

    # TaskCheckInterval set to 500ms
    sleep 1

    check_log_contains $WORK_DIR/worker1/log/dm-worker.log "you need (at least one of) the RELOAD privilege(s) for this operation"
    check_log_contains $WORK_DIR/worker1/log/dm-worker.log "error is not resumable"
    check_log_contains $WORK_DIR/worker2/log/dm-worker.log "you need (at least one of) the RELOAD privilege(s) for this operation"
    check_log_contains $WORK_DIR/worker2/log/dm-worker.log "error is not resumable"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status test" \
        "\"stage\": \"Paused\"" 2 \
        "you need (at least one of) the RELOAD privilege(s) for this operation" 2

    cleanup_data full_mode
    cleanup_process $*
}

function escape_schema() {
    cp $cur/data/db1.prepare.sql $WORK_DIR/db1.prepare.sql
    cp $cur/data/db2.prepare.sql $WORK_DIR/db2.prepare.sql
    cp $cur/conf/dm-task.yaml $WORK_DIR/dm-task.yaml
    cp $cur/conf/diff_config.toml $WORK_DIR/diff_config.toml
    sed -i "s/full_mode/full\/mode/g" $WORK_DIR/db1.prepare.sql $WORK_DIR/db2.prepare.sql $WORK_DIR/dm-task.yaml $WORK_DIR/diff_config.toml

    run_sql_file $WORK_DIR/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
    check_contains 'Query OK, 2 rows affected'
    run_sql_file $WORK_DIR/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
    check_contains 'Query OK, 3 rows affected'

    # test load data with `/` in the table name
    run_sql_source1 "create table \`full/mode\`.\`tb\/1\` (id int, name varchar(10), primary key(\`id\`));"
    run_sql_source1 "insert into \`full/mode\`.\`tb\/1\` values(1,'haha');"
    run_sql_source1 "insert into \`full/mode\`.\`tb\/1\` values(2,'hihi');"

    run_sql_file $cur/data/db1.prepare.user.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
    check_count 'Query OK, 0 rows affected' 7
    run_sql_file $cur/data/db2.prepare.user.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
    check_count 'Query OK, 0 rows affected' 7

    run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
    # operate mysql config to worker
    cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
    cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
    sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
    sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker2/relay_log" $WORK_DIR/source2.yaml
    dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
    dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

    # start DM task only
    dmctl_start_task "$WORK_DIR/dm-task.yaml" "--remove-meta"
    check_sync_diff $WORK_DIR $WORK_DIR/diff_config.toml

    echo "check dump files have been cleaned"
    ls $WORK_DIR/worker1/dumped_data.test && exit 1 || echo "worker1 auto removed dump files"
    ls $WORK_DIR/worker2/dumped_data.test && exit 1 || echo "worker2 auto removed dump files"

    cleanup_data full/mode
    cleanup_process $*
}

function run() {
    fail_acquire_global_lock

    run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
    check_contains 'Query OK, 2 rows affected'
    run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
    check_contains 'Query OK, 3 rows affected'

    # test load data with `"` in the table name
    run_sql_source1 "create table full_mode.\`tb\"1\` (id int,name varchar(10), primary key(\`id\`));"
    run_sql_source1 "insert into full_mode.\`tb\"1\` values(1,'haha');"
    run_sql_source1 "insert into full_mode.\`tb\"1\` values(2,'hihi');"

    run_sql_file $cur/data/db1.prepare.user.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
    check_count 'Query OK, 0 rows affected' 7
    run_sql_file $cur/data/db2.prepare.user.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
    check_count 'Query OK, 0 rows affected' 7

    run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
    # operate mysql config to worker
    cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
    cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
    sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
    sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker2/relay_log" $WORK_DIR/source2.yaml
    dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
    dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

    # start DM task only
    dmctl_start_task "$cur/conf/dm-task.yaml" "--remove-meta"

    # use sync_diff_inspector to check full dump loader
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    echo "check dump files have been cleaned"
    ls $WORK_DIR/worker1/dumped_data.test && exit 1 || echo "worker1 auto removed dump files"
    ls $WORK_DIR/worker2/dumped_data.test && exit 1 || echo "worker2 auto removed dump files"
}

cleanup_data full_mode
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
