#!/bin/bash

set -eux

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
task_name="sequence_sharding_optimistic"

BASE_URL="127.0.0.1:${MASTER_PORT}/apis/v1alpha1/schema/"

run() {
    run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
    run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

    run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

    # try to get schema for the table, but the worker instance not started.
    curl -X PUT ${BASE_URL}1 -d '{"task":"sequence_sharding_optimistic", "sources": ["mysql-replica-01"], "database":"sharding_seq_opt", "table":"t1"}' > ${WORK_DIR}/get_schema.log
    check_log_contains ${WORK_DIR}/get_schema.log "mysql-replica-01 relevant worker-client not found" 1

    # operate mysql config to worker
    cp $cur/conf/source1.toml $WORK_DIR/source1.toml
    cp $cur/conf/source2.toml $WORK_DIR/source2.toml
    sed -i "/relay-binlog-name/i\relay-dir = \"$WORK_DIR/worker1/relay_log\"" $WORK_DIR/source1.toml
    sed -i "/relay-binlog-name/i\relay-dir = \"$WORK_DIR/worker2/relay_log\"" $WORK_DIR/source2.toml
    dmctl_operate_source create $WORK_DIR/source1.toml $SOURCE_ID1
    dmctl_operate_source create $WORK_DIR/source2.toml $SOURCE_ID2

    # try to get schema for the table, the subtask has not started.
    curl -X PUT ${BASE_URL}1 -d '{"task":"sequence_sharding_optimistic", "sources": ["mysql-replica-01"], "database":"sharding_seq_opt", "table":"t1"}' > ${WORK_DIR}/get_schema.log
    check_log_contains ${WORK_DIR}/get_schema.log "sub task with name sequence_sharding_optimistic not found" 1

    # start DM task only
    dmctl_start_task "$cur/conf/dm-task.yaml" "--remove-meta"

    # use sync_diff_inspector to check full dump loader
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    # test create and alter database ddl
    run_sql_file $cur/data/db1.increment0.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
    run_sql_file $cur/data/db2.increment0.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

    # check database `sharding_seq_tmp` exists
    sleep 2
    run_sql "select count(*) from sharding_seq_tmp.t1;" $TIDB_PORT $TIDB_PASSWORD
    check_contains "count(*): 1"

    # try to get schema for the table, but the stage is not paused.
    curl -X PUT ${BASE_URL}1 -d '{"task":"sequence_sharding_optimistic", "sources": ["mysql-replica-01"], "database":"sharding_seq_opt", "table":"t1"}' > ${WORK_DIR}/get_schema.log
    check_log_contains ${WORK_DIR}/get_schema.log "current stage is Running but not paused, invalid" 1

    # pause task manually.
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "pause-task $task_name"\
        "\"result\": true" 3
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status $task_name" \
        "\"stage\": \"Paused\"" 2

    # try to get schema for the table, but can't get because no DDL/DML replicated yet.
    curl -X PUT ${BASE_URL}1 -d '{"task":"sequence_sharding_optimistic", "sources": ["mysql-replica-01"], "database":"sharding_seq_opt", "table":"t1"}' > ${WORK_DIR}/get_schema.log
    check_log_contains ${WORK_DIR}/get_schema.log "Table 'sharding_seq_opt.t1' doesn't exist" 1

    # resume task manually.
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "resume-task $task_name"\
        "\"result\": true" 3
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status $task_name" \
        "\"stage\": \"Running\"" 2

    # now, for optimistic shard DDL, different sources will reach a stage often not at the same time,
    # in order to simply the check and resume flow, only enable the failpoint for one DM-worker.
    export GO_FAILPOINTS="github.com/pingcap/dm/syncer/FlushCheckpointStage=return(100)" # for all stages
    echo "restart dm-worker1"
    ps aux | grep dm-worker1 |awk '{print $2}'|xargs kill || true
    check_port_offline $WORKER1_PORT 20
    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    export GO_FAILPOINTS=''

    run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
    run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

     # the task should paused by `FlushCheckpointStage` failpont before flush old checkpoint.
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status $task_name" \
        "failpoint error for FlushCheckpointStage before flush old checkpoint" 1

    # resume-task to next stage
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "resume-task $task_name"\
        "\"result\": true" 3

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status $task_name" \
        "failpoint error for FlushCheckpointStage before track DDL" 1

    # resume-task to next stage
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "resume-task $task_name"\
        "\"result\": true" 3

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status $task_name" \
        "failpoint error for FlushCheckpointStage before execute DDL" 1

    # resume-task to next stage
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "resume-task $task_name"\
        "\"result\": true" 3

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status $task_name" \
        "failpoint error for FlushCheckpointStage before save checkpoint" 1

    # resume-task to next stage
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "resume-task $task_name"\
        "\"result\": true" 3

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status $task_name" \
        "failpoint error for FlushCheckpointStage before flush checkpoint" 1

    # resume-task to continue the sync
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "resume-task $task_name"\
        "\"result\": true" 3

    # use sync_diff_inspector to check data now!
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    # pause task manually again.
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "pause-task $task_name"\
        "\"result\": true" 3
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status $task_name" \
        "\"stage\": \"Paused\"" 2

    # try to get schema for the table, the latest schema got.
    curl -X PUT ${BASE_URL}1 -d '{"task":"sequence_sharding_optimistic", "sources": ["mysql-replica-01"], "database":"sharding_seq_opt", "table":"t1"}' > ${WORK_DIR}/get_schema.log
    check_log_contains ${WORK_DIR}/get_schema.log 'CREATE TABLE `t1` ( `id` bigint(20) NOT NULL, `c2` varchar(20) DEFAULT NULL, `c3` int(11) DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin' 1

    # drop the schema.
    curl -X PUT ${BASE_URL}3 -d '{"task":"sequence_sharding_optimistic", "sources": ["mysql-replica-01"], "database":"sharding_seq_opt", "table":"t1"}'

    # try to get schema again, but can't get.
    curl -X PUT ${BASE_URL}1 -d '{"task":"sequence_sharding_optimistic", "sources": ["mysql-replica-01"], "database":"sharding_seq_opt", "table":"t1"}' > ${WORK_DIR}/get_schema.log
    check_log_contains ${WORK_DIR}/get_schema.log "Table 'sharding_seq_opt.t1' doesn't exist" 1
}

cleanup_data sharding_target_opt
cleanup_data sharding_seq_tmp
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
