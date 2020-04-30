#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

function checksum() {
    read -d '' sql << EOF
SELECT BIT_XOR(CAST(CRC32(CONCAT_WS(',', uid, name, info, age, id_gen,
    CONCAT(ISNULL(uid), ISNULL(name), ISNULL(info), ISNULL(age), ISNULL(id_gen)))) AS UNSIGNED)) AS checksum
    FROM db_target.t_target WHERE (uid > 70000);
EOF
    run_sql "$sql" $TIDB_PORT $TIDB_PASSWORD
    echo $(tail -n 1 "$TEST_DIR/sql_res.$TEST_NAME.txt")
}

function run() {
    run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
    check_contains 'Query OK, 2 rows affected'
    run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
    check_contains 'Query OK, 3 rows affected'

    run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT

    # now, for pessimistic shard DDL, if interrupted after executed DDL but before flush checkpoint,
    # re-sync this DDL will cause the source try to sync the DDL of the previous lock again,
    # this will need to recover the replication manually,
    # so we do not interrupt the replication after executed DDL for this test case.
    #
    # now, for pessimistic shard DDL, owner and non-owner will reach a stage often not at the same time,
    # in order to simply the check and resume flow, only enable the failpoint for one DM-worker.
    export GO_FAILPOINTS="github.com/pingcap/dm/syncer/FlushCheckpointStage=return(2)"
    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    export GO_FAILPOINTS=''

    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
    # operate mysql config to worker
    cp $cur/conf/source1.toml $WORK_DIR/source1.toml
    cp $cur/conf/source2.toml $WORK_DIR/source2.toml
    sed -i "/relay-binlog-name/i\relay-dir = \"$WORK_DIR/worker1/relay_log\"" $WORK_DIR/source1.toml
    sed -i "/relay-binlog-name/i\relay-dir = \"$WORK_DIR/worker2/relay_log\"" $WORK_DIR/source2.toml
    dmctl_operate_source create $WORK_DIR/source1.toml $SOURCE_ID1
    dmctl_operate_source create $WORK_DIR/source2.toml $SOURCE_ID2

    # start DM task only
    dmctl_start_task

    # TODO: check sharding partition id
    # use sync_diff_inspector to check full dump loader
    echo "check sync diff for full dump and load"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
    run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

    # the task should paused by `FlushCheckpointStage` failpont before flush old checkpoint.
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status test" \
        "failpoint error for FlushCheckpointStage before flush old checkpoint" 1

    # resume-task to next stage
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "resume-task test"\
        "\"result\": true" 3

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status test" \
        "failpoint error for FlushCheckpointStage before track DDL" 1

    # resume-task to next stage
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "resume-task test"\
        "\"result\": true" 3

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status test" \
        "failpoint error for FlushCheckpointStage before execute DDL" 1

    # resume-task to next stage
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "resume-task test"\
        "\"result\": true" 3

    # NOTE: the lock may be locked for the next DDL, for details please see the following comments in `master/shardll/pessimist.go`,
    # `FIXME: the following case is not supported automatically now, try to support it later`
    # so we try to do this `pause-task` and `resume-task` in the case now.
    sleep 3
    # pause twice, just used to test pause by the way
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "pause-task test"\
        "\"result\": true" 3
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "pause-task test"\
        "\"result\": true" 3
    # wait really paused
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status test" \
        "Paused" 2

    # resume twice, just used to test resume by the way
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "resume-task test"\
        "\"result\": true" 3
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "resume-task test"\
        "\"result\": true" 3

    # TODO: check sharding partition id
    # use sync_diff_inspector to check data now!
    echo "check sync diff for the first increment replication"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    # test create database, create table in sharding mode
    run_sql_file $cur/data/db1.increment2.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
    run_sql_file $cur/data/db2.increment2.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
    cp $cur/conf/diff_config.toml $WORK_DIR/diff_config.toml
    printf "\n[[table-config.source-tables]]\ninstance-id = \"source-1\"\nschema = \"sharding2\"\ntable  = \"~t.*\"" >> $WORK_DIR/diff_config.toml
    printf "\n[[table-config.source-tables]]\ninstance-id = \"source-2\"\nschema = \"sharding2\"\ntable  = \"~t.*\"" >> $WORK_DIR/diff_config.toml
    echo "check sync diff for the second increment replication"
    check_sync_diff $WORK_DIR $WORK_DIR/diff_config.toml

    old_checksum=$(checksum)

    # test drop table, drop database, truncate table in sharding mode
    run_sql_file $cur/data/db1.increment3.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
    run_sql_file $cur/data/db2.increment3.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
    cp $cur/conf/diff_config.toml $WORK_DIR/diff_config.toml
    printf "\n[[table-config.source-tables]]\ninstance-id = \"source-1\"\nschema = \"sharding2\"\ntable  = \"~t.*\"" >> $WORK_DIR/diff_config.toml
    sed -i "s/^# range-placeholder/range = \"uid < 70000\"/g" $WORK_DIR/diff_config.toml
    echo "check sync diff for the third increment replication"
    check_sync_diff $WORK_DIR $WORK_DIR/diff_config.toml

    new_checksum=$(checksum)
    echo "checksum before drop/truncate: $old_checksum, checksum after drop/truncate: $new_checksum"
    [ "$old_checksum" == "$new_checksum" ]

    # stop twice, just used to test stop by the way
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "stop-task test"\
        "\"result\": true" 3
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "stop-task test"\
        "task test has no source or not exist" 1
}

cleanup_data db_target
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
