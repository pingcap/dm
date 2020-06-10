#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
source $cur/lib.sh

function DM_001() {
    echo "[$(date)] <<<<<< start DM-001 >>>>>>"

    # create table
    init_table 111 112

    # start task
    cp $cur/conf/dm-task-single-source.yaml $WORK_DIR/task.yaml
    sed -i "s/is-sharding: true/is-sharding: false/g" $WORK_DIR/task.yaml
    sed -i '/^shard-mode/d' $WORK_DIR/task.yaml
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $WORK_DIR/task.yaml --remove-meta" \
            "\"result\": true" 2

    # CASE
    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
    sleep 2
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Duplicate column name 'new_col1'" 1

    # stop task
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 2

    # clean data
    clean_table

    echo "[$(date)] <<<<<< finish DM-001 >>>>>>"
}

function DM_002() {
    echo "[$(date)] <<<<<< start DM-002 >>>>>>"

    init_table 111 112

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-single-source.yaml --remove-meta" \
            "\"result\": true" 2

    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 2

    clean_table

    echo "[$(date)] <<<<<< finish DM-002 >>>>>>"
}

function DM_003() {
    echo "[$(date)] <<<<<< start DM-003 >>>>>>"

    init_table 111 112

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-single-source.yaml --remove-meta" \
            "\"result\": true" 2

    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 5 "fail"
    run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 2

    clean_table

    echo "[$(date)] <<<<<< finish DM-003 >>>>>>"
}

function DM_004() {
    echo "[$(date)] <<<<<< start DM-004 >>>>>>"

    init_table 111 112

    cp $cur/conf/dm-task-single-source.yaml $WORK_DIR/task.yaml
    sed -i "s/shard-mode: \"pessimistic\"/shard-mode: \"optimistic\"/g" $WORK_DIR/task.yaml
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $WORK_DIR/task.yaml --remove-meta" \
            "\"result\": true" 2

    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (1);"
    run_sql_source1 "insert into ${shardddl1}.${tb2} values (2);"

    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
    sleep 2
    run_sql_tidb "select count(1) from ${shardddl}.${tb};"
    check_contains 'count(1): 3'

    run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb2} values (2,2)"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 2

    clean_table

    echo "[$(date)] <<<<<< finish DM-004 >>>>>>"
}

function DM_005() {
    echo "[$(date)] <<<<<< start DM-005 >>>>>>"

    init_table 111 112

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-single-source.yaml --remove-meta" \
            "\"result\": true" 2

    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
    run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb2} values (2,2)"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 2

    clean_table

    echo "[$(date)] <<<<<< finish DM-005 >>>>>>"
}

function DM_006() {
    echo "[$(date)] <<<<<< start DM-006 >>>>>>"

    init_table 111 211

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-double-source.yaml --remove-meta" \
            "\"result\": true" 3

    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values (2,2)"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-006 >>>>>>"
}

function DM_007() {
    echo "[$(date)] <<<<<< start DM-007 >>>>>>"

    init_table 111 212

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-double-source.yaml --remove-meta" \
            "\"result\": true" 3

    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
    run_sql_source2 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values (2,2)"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-007 >>>>>>"
}

function DM_008() {
    echo "[$(date)] <<<<<< start DM-008 >>>>>>"

    init_table 111 221

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-double-source.yaml --remove-meta" \
            "\"result\": true" 3

    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
    run_sql_source2 "alter table ${shardddl2}.${tb1} add column new_col1 int;"
    run_sql_source2 "insert into ${shardddl2}.${tb1} values (2,2)"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-008 >>>>>>"
}

function DM_009() {
    echo "[$(date)] <<<<<< start DM-009 >>>>>>"

    init_table 111 222

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-double-source.yaml --remove-meta" \
            "\"result\": true" 3

    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
    run_sql_source2 "alter table ${shardddl2}.${tb2} add column new_col1 int;"
    run_sql_source2 "insert into ${shardddl2}.${tb2} values (2,2)"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-009 >>>>>>"
}

function DM_010() {
    echo "[$(date)] <<<<<< start DM-010 >>>>>>"

    init_table 111 112

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-single-source.yaml --remove-meta" \
            "\"result\": true" 2

    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col2 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (2,2,2)"
    run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb2} values (3,3)"
    run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col2 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb2} values (4,4,4)"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 2

    clean_table

    echo "[$(date)] <<<<<< finish DM-010 >>>>>>"
}

function DM_011() {
    echo "[$(date)] <<<<<< start DM-011 >>>>>>"

    init_table 111 211

    # we should change to single source after https://github.com/pingcap/dm/pull/722 merged
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-double-source.yaml --remove-meta" \
            "\"result\": true" 3

    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col2 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (2,2,2)"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 float;"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values (3,3.0)"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col2 float;"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values (4,4.0,4.0)"

    sleep 2
    check_log_contains $WORK_DIR/master/log/dm-master.log "is different with"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-011 >>>>>>"
}

function DM_012() {
    echo "[$(date)] <<<<<< start DM-012 >>>>>>"

    init_table 111 211

    # we should change to single source after https://github.com/pingcap/dm/pull/722 merged
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-double-source.yaml --remove-meta" \
            "\"result\": true" 3

    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col2 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (2,2,2)"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col2 int;"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values (3,3)"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values (4,4,4)"

    sleep 2
    check_log_contains $WORK_DIR/master/log/dm-master.log "is different with"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-012 >>>>>>"
}

function DM_013() {
    echo "[$(date)] <<<<<< start DM-013 >>>>>>"

    init_table 111 112

    cp $cur/conf/dm-task-single-source.yaml $WORK_DIR/task.yaml
    sed -i "s/shard-mode: \"pessimistic\"/shard-mode: \"optimistic\"/g" $WORK_DIR/task.yaml
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $WORK_DIR/task.yaml --remove-meta" \
            "\"result\": true" 2

    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (1);"
    run_sql_source1 "insert into ${shardddl1}.${tb2} values (2);"

    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col2 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (2,2,2)"
    run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb2} values (3,3)"
    run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col2 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb2} values (4,4,4)"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 2

    clean_table

    echo "[$(date)] <<<<<< finish DM-013 >>>>>>"
}

function DM_014() {
    echo "[$(date)] <<<<<< start DM-014 >>>>>>"

    init_table 111 112

    cp $cur/conf/dm-task-single-source.yaml $WORK_DIR/task.yaml
    sed -i "s/shard-mode: \"pessimistic\"/shard-mode: \"optimistic\"/g" $WORK_DIR/task.yaml
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $WORK_DIR/task.yaml --remove-meta" \
            "\"result\": true" 2

    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (1);"
    run_sql_source1 "insert into ${shardddl1}.${tb2} values (2);"

    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col2 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (2,2,2)"
    run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col2 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb2} values (3,3)"
    run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb2} values (4,4,4)"

    sleep 2
    run_sql_tidb "select count(1) from ${shardddl}.${tb};"
    check_contains 'count(1): 6'

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 2

    clean_table

    echo "[$(date)] <<<<<< finish DM-014 >>>>>>"
}

function DM_015() {
    echo "[$(date)] <<<<<< start DM-015 >>>>>>"

    init_table 111
    
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-single-source.yaml --remove-meta" \
            "\"result\": true" 2

    run_sql_source1 "drop database ${shardddl1};"
    sleep 2
    check_log_contains $WORK_DIR/worker1/log/dm-worker.log "skip event, need handled ddls is empty"

    run_sql_source1 "create database ${shardddl1};"
    sleep 2
    check_log_contains $WORK_DIR/worker1/log/dm-worker.log "CREATE DATABASE IF NOT EXISTS \`${shardddl1}\`"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 2

    clean_table

    echo "[$(date)] <<<<<< finish DM-015 >>>>>>"
}

function DM_016() {
    echo "[$(date)] <<<<<< start DM-016 >>>>>>"

    init_table 111
    
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-single-source.yaml --remove-meta" \
            "\"result\": true" 2

    run_sql_source1 "drop database ${shardddl1};"
    sleep 2
    check_log_contains $WORK_DIR/worker1/log/dm-worker.log "skip event, need handled ddls is empty"

    run_sql_source1 "create database if not exists ${shardddl1};"
    sleep 2
    check_log_contains $WORK_DIR/worker1/log/dm-worker.log "CREATE DATABASE IF NOT EXISTS \`${shardddl1}\`"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 2

    clean_table

    echo "[$(date)] <<<<<< finish DM-016 >>>>>>"
}

function DM_017() {
    echo "[$(date)] <<<<<< start DM-017 >>>>>>"

    init_table 111
    
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-single-source.yaml --remove-meta" \
            "\"result\": true" 2

    run_sql_source1 "drop table ${shardddl1}.${tb1};"
    sleep 2
    check_log_contains $WORK_DIR/worker1/log/dm-worker.log "skip event, need handled ddls is empty"

    run_sql_source1 "create table ${shardddl1}.${tb1}(id int);"
    sleep 2
    check_log_contains $WORK_DIR/worker1/log/dm-worker.log "CREATE TABLE IF NOT EXISTS \`${shardddl1}\`.\`${tb1}\` (\`id\` INT)"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 2

    clean_table

    echo "[$(date)] <<<<<< finish DM-017 >>>>>>"
}

function DM_018() {
    echo "[$(date)] <<<<<< start DM-018 >>>>>>"

    init_table 111
    
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-single-source.yaml --remove-meta" \
            "\"result\": true" 2

    run_sql_source1 "drop table ${shardddl1}.${tb1};"
    sleep 2
    check_log_contains $WORK_DIR/worker1/log/dm-worker.log "skip event, need handled ddls is empty"

    run_sql_source1 "create table if not exists ${shardddl1}.${tb1}(id int);"
    sleep 2
    check_log_contains $WORK_DIR/worker1/log/dm-worker.log "CREATE TABLE IF NOT EXISTS \`${shardddl1}\`.\`${tb1}\` (\`id\` INT)"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 2

    clean_table

    echo "[$(date)] <<<<<< finish DM-018 >>>>>>"
}

function DM_019() {
    echo "[$(date)] <<<<<< start DM-019 >>>>>>"

    init_table 111
    
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-single-source.yaml --remove-meta" \
            "\"result\": true" 2

    run_sql_source1 "truncate table ${shardddl1}.${tb1};"
    sleep 2
    check_log_contains $WORK_DIR/worker1/log/dm-worker.log "skip event, need handled ddls is empty"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 2

    clean_table

    echo "[$(date)] <<<<<< finish DM-019 >>>>>>"
}

function DM_020() {
    echo "[$(date)] <<<<<< start DM-020 >>>>>>"

    init_table 111
    
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-no-shard.yaml --remove-meta" \
            "\"result\": true" 2

    run_sql_source1 "rename table ${shardddl1}.${tb1} to ${shardddl1}.${tb2};"
    run_sql_source1 "insert into ${shardddl1}.${tb2} values (1);"

    sleep 2
    run_sql_tidb "select count(1) from ${shardddl1}.${tb2};"
    check_contains 'count(1): 1'

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 2

    run_sql_tidb "drop database ${shardddl1};"
    clean_table

    echo "[$(date)] <<<<<< finish DM-020 >>>>>>"
}

function DM_021() {
    echo "[$(date)] <<<<<< start DM-021 >>>>>>"

    init_table 111
    
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-single-source.yaml --remove-meta" \
            "\"result\": true" 2

    # same as "rename ${shardddl}.${tb} to ${shardddl}.${tb};"
    run_sql_source1 "rename table ${shardddl1}.${tb1} to ${shardddl1}.${tb2};"
    sleep 2
    check_log_contains $WORK_DIR/worker1/log/dm-worker.log "Table '${shardddl}.${tb}' already exist"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 2

    clean_table

    echo "[$(date)] <<<<<< finish DM-021 >>>>>>"
}

function DM_022() {
    echo "[$(date)] <<<<<< start DM-022 >>>>>>"

    init_table 111
    
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-no-shard.yaml --remove-meta" \
            "\"result\": true" 2

    sleep 2
    run_sql_tidb "create table ${shardddl1}.${tb2} (id int);"

    run_sql_source1 "rename table ${shardddl1}.${tb1} to ${shardddl1}.${tb2};"
    sleep 2
    check_log_contains $WORK_DIR/worker1/log/dm-worker.log "Table '${shardddl1}.${tb2}' already exists"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 2

    run_sql_tidb "drop database ${shardddl1};"
    clean_table

    echo "[$(date)] <<<<<< finish DM-022 >>>>>>"
}

function DM_023() {
    echo "[$(date)] <<<<<< start DM-023 >>>>>>"

    init_table 111 112
    
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-double-source.yaml --remove-meta" \
            "\"result\": true" 3

    run_sql_source1 "rename table ${shardddl1}.${tb1} to ${shardddl1}.${tb3}, ${shardddl1}.${tb2} to ${shardddl1}.${tb4};"
    sleep 2
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "rename table .* not supported" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    run_sql_source1 "drop table ${shardddl1}.${tb3};"
    run_sql_source1 "drop table ${shardddl1}.${tb4};"
    clean_table

    echo "[$(date)] <<<<<< finish DM-023 >>>>>>"
}

function DM_026() {
    echo "[$(date)] <<<<<< start DM-026 >>>>>>"

    init_table 111 112

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-single-source.yaml --remove-meta" \
            "\"result\": true" 2

    run_sql_source1 "insert into ${shardddl1}.${tb1} values (1)"
    run_sql_source1 "insert into ${shardddl1}.${tb2} values (2)"
    run_sql_source1 "create table ${shardddl1}.${tb3}(id int);"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (3)"
    run_sql_source1 "insert into ${shardddl1}.${tb2} values (4)"
    run_sql_source1 "insert into ${shardddl1}.${tb3} values (5)"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 2

    run_sql_source1 "drop table ${shardddl1}.${tb3};"
    clean_table

    echo "[$(date)] <<<<<< finish DM-026 >>>>>>"
}

function DM_027() {
    echo "[$(date)] <<<<<< start DM-027 >>>>>>"

    init_table 111 112

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-single-source.yaml --remove-meta" \
            "\"result\": true" 2

    run_sql_source1 "insert into ${shardddl1}.${tb1} values (1)"
    run_sql_source1 "insert into ${shardddl1}.${tb2} values (2)"
    run_sql_source1 "create table ${shardddl1}.${tb3}(id int,val int);"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (3)"
    run_sql_source1 "insert into ${shardddl1}.${tb2} values (4)"
    run_sql_source1 "insert into ${shardddl1}.${tb3} values (5,6)"

    # we now haven't checked table struct when create sharding table
    sleep 2
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Unknown column 'val' in 'field list'" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 2

    run_sql_source1 "drop table ${shardddl1}.${tb3};"
    clean_table

    echo "[$(date)] <<<<<< finish DM-027 >>>>>>"
}

function DM_028() {
    echo "[$(date)] <<<<<< start DM-028 >>>>>>"

    run_sql_source1 "create table ${shardddl1}.${tb1} (id int primary key);"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-single-source.yaml --remove-meta" \
            "\"result\": true" 2

    run_sql_source1 "alter table ${shardddl1}.${tb1} drop primary key;"

    sleep 2
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Unsupported drop primary key when alter-primary-key is false" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 2

    clean_table

    echo "[$(date)] <<<<<< finish DM-028 >>>>>>"
}

function DM_030_CASE {
    run_sql_source1 "alter table ${shardddl1}.${tb1} add new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(3,3);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(4,4);"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_030_PESSIMISTIC() {
    echo "[$(date)] <<<<<< start DM-030 pessimistic >>>>>>"

    init_table 111 211

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-double-source.yaml --remove-meta" \
            "\"result\": true" 3

    DM_030_CASE

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-030 pessimistic >>>>>>"
}

function DM_030_OPTIMISTIC() {
    echo "[$(date)] <<<<<< start DM-030 optimistic >>>>>>"

    init_table 111 211

    cp $cur/conf/dm-task-double-source.yaml $WORK_DIR/task.yaml
    sed -i "s/shard-mode: \"pessimistic\"/shard-mode: \"optimistic\"/g" $WORK_DIR/task.yaml
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $WORK_DIR/task.yaml --remove-meta" \
            "\"result\": true" 3

    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"

    DM_030_CASE

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-030 optimistic >>>>>>"        
}

function DM_030() {
    echo "[$(date)] <<<<<< start DM-030 >>>>>>"

    DM_030_PESSIMISTIC
    DM_030_OPTIMISTIC

    echo "[$(date)] <<<<<< finish DM-030 >>>>>>"
}

function DM_031_CASE() {
    run_sql_source1 "alter table ${shardddl1}.${tb1} add new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add new_col1 varchar(10);"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(3,3);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(4,'dkfj');"
    sleep 2
    check_log_contains $WORK_DIR/master/log/dm-master.log "is different with"
}

function DM_031_PESSIMISTIC() {
    echo "[$(date)] <<<<<< start DM-031 pessimistic >>>>>>"

    init_table 111 211

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-double-source.yaml --remove-meta" \
            "\"result\": true" 3

    DM_031_CASE

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-031 pessimistic >>>>>>"
}

function DM_031_OPTIMISTIC() {
    echo "[$(date)] <<<<<< start DM-031 optimistic >>>>>>"

    init_table 111 211

    cp $cur/conf/dm-task-double-source.yaml $WORK_DIR/task.yaml
    sed -i "s/shard-mode: \"pessimistic\"/shard-mode: \"optimistic\"/g" $WORK_DIR/task.yaml
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $WORK_DIR/task.yaml --remove-meta" \
            "\"result\": true" 3

    DM_031_CASE

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-031 optimistic >>>>>>"
}

function DM_031() {
    echo "[$(date)] <<<<<< start DM-031 >>>>>>"

    DM_031_PESSIMISTIC
    DM_031_OPTIMISTIC

    echo "[$(date)] <<<<<< finish DM-031 >>>>>>"
}

function DM_032_CASE {
    run_sql_source1 "alter table ${shardddl1}.${tb1} add new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
    run_sql_source1 "alter table ${shardddl1}.${tb1} drop column new_col1;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source1 "alter table ${shardddl1}.${tb1} add new_col2 float;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(3,3.0);"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add new_col2 float;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4.0);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5.0);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(6);"
    run_sql_source2 "alter table ${shardddl1}.${tb2} add new_col2 float;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7.0);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8.0);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9.0);"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_032_PESSIMISTIC() {
    echo "[$(date)] <<<<<< start DM-032 pessimistic >>>>>>"

    init_table 111 211 212

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-double-source.yaml --remove-meta" \
            "\"result\": true" 3

    DM_032_CASE

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-032 pessimistic >>>>>>"
}

function DM_032_OPTIMISTIC() {
    echo "[$(date)] <<<<<< start DM-032 optimistic >>>>>>"

    init_table 111 211 212

    cp $cur/conf/dm-task-double-source.yaml $WORK_DIR/task.yaml
    sed -i "s/shard-mode: \"pessimistic\"/shard-mode: \"optimistic\"/g" $WORK_DIR/task.yaml
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $WORK_DIR/task.yaml --remove-meta" \
            "\"result\": true" 3

    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

    DM_032_CASE

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-032 optimistic >>>>>>"        
}

function DM_032() {
    echo "[$(date)] <<<<<< start DM-032 >>>>>>"

    # currently not support pessimistic
    # DM_032_PESSIMISTIC
    DM_032_OPTIMISTIC

    echo "[$(date)] <<<<<< finish DM-032 >>>>>>"
}

function DM_033_CASE {
    run_sql_source1 "alter table ${shardddl1}.${tb1} add new_col1 int not null;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(null);"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add new_col1 int not null;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(2,2);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(3,3);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(null);"
    run_sql_source2 "alter table ${shardddl1}.${tb2} add new_col1 int not null;"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_033_PESSIMISTIC() {
    echo "[$(date)] <<<<<< start DM-033 pessimistic >>>>>>"

    init_table 111 211 212

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-double-source.yaml --remove-meta" \
            "\"result\": true" 3

    DM_033_CASE

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-033 pessimistic >>>>>>"
}

function DM_033_OPTIMISTIC() {
    echo "[$(date)] <<<<<< start DM-033 optimistic >>>>>>"

    init_table 111 211 212

    cp $cur/conf/dm-task-double-source.yaml $WORK_DIR/task.yaml
    sed -i "s/shard-mode: \"pessimistic\"/shard-mode: \"optimistic\"/g" $WORK_DIR/task.yaml
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $WORK_DIR/task.yaml --remove-meta" \
            "\"result\": true" 3

    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

    DM_033_CASE

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-033 optimistic >>>>>>"        
}

function DM_033() {
    echo "[$(date)] <<<<<< start DM-033 >>>>>>"

    DM_033_PESSIMISTIC
    # currently not support optimistic
    # DM_033_OPTIMISTIC

    echo "[$(date)] <<<<<< finish DM-033 >>>>>>"
}

function DM_034_CASE {
    run_sql_source1 "alter table ${shardddl1}.${tb1} add new_col1 int unique auto_increment;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,0);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add new_col1 int unique auto_increment;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(3,0);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(4,0);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(5);"
    run_sql_source2 "alter table ${shardddl1}.${tb2} add new_col1 int unique auto_increment;"
    sleep 2
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "unsupported add column 'new_col1' constraint UNIQUE KEY when altering" 2
}

function DM_034_PESSIMISTIC() {
    echo "[$(date)] <<<<<< start DM-034 pessimistic >>>>>>"

    init_table 111 211 212

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-double-source.yaml --remove-meta" \
            "\"result\": true" 3

    DM_034_CASE

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-034 pessimistic >>>>>>"
}

function DM_034_OPTIMISTIC() {
    echo "[$(date)] <<<<<< start DM-034 optimistic >>>>>>"

    init_table 111 211 212

    cp $cur/conf/dm-task-double-source.yaml $WORK_DIR/task.yaml
    sed -i "s/shard-mode: \"pessimistic\"/shard-mode: \"optimistic\"/g" $WORK_DIR/task.yaml
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $WORK_DIR/task.yaml --remove-meta" \
            "\"result\": true" 3

    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

    DM_034_CASE

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-034 optimistic >>>>>>"        
}

function DM_034() {
    echo "[$(date)] <<<<<< start DM-034 >>>>>>"

    DM_034_PESSIMISTIC
    DM_034_OPTIMISTIC

    echo "[$(date)] <<<<<< finish DM-034 >>>>>>"
}

function DM_035_CASE {
    run_sql_source1 "alter table ${shardddl1}.${tb1} add new_col1 int;"
    run_sql_source1 "alter table ${shardddl1}.${tb1} add new_col2 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add new_col2 int;"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,4);"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(5,5,5);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(6);"
    run_sql_source2 "alter table ${shardddl1}.${tb2} add new_col2 int;"
    run_sql_source2 "alter table ${shardddl1}.${tb2} add new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,7);"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(8,8,8);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9,9);"
}

function DM_035_PESSIMISTIC() {
    echo "[$(date)] <<<<<< start DM-035 pessimistic >>>>>>"

    init_table 111 211 212

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-double-source.yaml --remove-meta" \
            "\"result\": true" 3

    DM_035_CASE

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-035 pessimistic >>>>>>"
}

function DM_035_OPTIMISTIC() {
    echo "[$(date)] <<<<<< start DM-035 optimistic >>>>>>"

    init_table 111 211 212

    cp $cur/conf/dm-task-double-source.yaml $WORK_DIR/task.yaml
    sed -i "s/shard-mode: \"pessimistic\"/shard-mode: \"optimistic\"/g" $WORK_DIR/task.yaml
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $WORK_DIR/task.yaml --remove-meta" \
            "\"result\": true" 3

    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

    DM_035_CASE
    sleep 5
    run_sql_tidb "select count(1) from ${shardddl}.${tb};"
    check_contains 'count(1): 12'

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-035 optimistic >>>>>>"        
}

function DM_035() {
    echo "[$(date)] <<<<<< start DM-035 >>>>>>"

    # currently not support pessimistic
    # DM_035_PESSIMISTIC
    DM_035_OPTIMISTIC

    echo "[$(date)] <<<<<< finish DM-035 >>>>>>"
}

function DM_036_CASE {
    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int first;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,'aaa');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'ccc');"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 int after a;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,'ddd');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,'eee');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,'fff');"
    run_sql_source2 "alter table ${shardddl1}.${tb2} add column new_col1 int after b;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,'ggg');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,'hhh');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,'iii',9);"
}

function DM_036_PESSIMISTIC() {
    echo "[$(date)] <<<<<< start DM-036 pessimistic >>>>>>"

    run_sql_source1 "create table ${shardddl1}.${tb1} (a int, b varchar(10));"
    run_sql_source2 "create table ${shardddl1}.${tb1} (a int, b varchar(10));"
    run_sql_source2 "create table ${shardddl1}.${tb2} (a int, b varchar(10));"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-double-source.yaml --remove-meta" \
            "\"result\": true" 3

    DM_036_CASE
    sleep 2
    run_sql_tidb "select count(1) from ${shardddl}.${tb};"
    check_contains 'count(1): 9'

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-036 pessimistic >>>>>>"
}

function DM_036_OPTIMISTIC() {
    echo "[$(date)] <<<<<< start DM-036 optimistic >>>>>>"

    run_sql_source1 "create table ${shardddl1}.${tb1} (a int, b varchar(10));"
    run_sql_source2 "create table ${shardddl1}.${tb1} (a int, b varchar(10));"
    run_sql_source2 "create table ${shardddl1}.${tb2} (a int, b varchar(10));"

    cp $cur/conf/dm-task-double-source.yaml $WORK_DIR/task.yaml
    sed -i "s/shard-mode: \"pessimistic\"/shard-mode: \"optimistic\"/g" $WORK_DIR/task.yaml
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $WORK_DIR/task.yaml --remove-meta" \
            "\"result\": true" 3

    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'ccc');"

    DM_036_CASE
    sleep 2
    run_sql_tidb "select count(1) from ${shardddl}.${tb};"
    check_contains 'count(1): 12'

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-036 optimistic >>>>>>"        
}

function DM_036() {
    echo "[$(date)] <<<<<< start DM-036 >>>>>>"

    # currently not support int pessimistic
    # DM_036_PESSIMISTIC
    DM_036_OPTIMISTIC

    echo "[$(date)] <<<<<< finish DM-036 >>>>>>"
}

function DM_037_CASE {
    run_sql_source1 "alter table ${shardddl1}.${tb1} add new_col1 int default 0;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add new_col1 int default -1;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(3,3);"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4);"
    run_sql_source2 "alter table ${shardddl1}.${tb2} add new_col1 int default 10;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(5,5);"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(6,6);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(7,7);"
}

function DM_037_PESSIMISTIC() {
    echo "[$(date)] <<<<<< start DM-037 pessimistic >>>>>>"

    init_table 111 211 212

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-double-source.yaml --remove-meta" \
            "\"result\": true" 3

    DM_037_CASE
    sleep 2
    check_log_contains $WORK_DIR/master/log/dm-master.log "is different with"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-037 pessimistic >>>>>>"
}

function DM_037_OPTIMISTIC() {
    echo "[$(date)] <<<<<< start DM-037 optimistic >>>>>>"

    init_table 111 211 212

    cp $cur/conf/dm-task-double-source.yaml $WORK_DIR/task.yaml
    sed -i "s/shard-mode: \"pessimistic\"/shard-mode: \"optimistic\"/g" $WORK_DIR/task.yaml
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $WORK_DIR/task.yaml --remove-meta" \
            "\"result\": true" 3

    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

    DM_037_CASE
    sleep 2
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "fail to handle shard ddl .* in optimistic mode," 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-037 optimistic >>>>>>"        
}

function DM_037() {
    echo "[$(date)] <<<<<< start DM-037 >>>>>>"

    DM_037_PESSIMISTIC
    DM_037_OPTIMISTIC

    echo "[$(date)] <<<<<< finish DM-037 >>>>>>"
}

function DM_038_CASE {
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"
    run_sql_source1 "alter table ${shardddl1}.${tb1} add column col1 datetime default now();"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add column col1 datetime default now();"
    run_sql_source2 "alter table ${shardddl1}.${tb2} add column col1 datetime default now();"
    run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values (1);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} (id) values (1);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} (id) values (1);"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 5 'fail'
}

function DM_038_PESSIMISTIC() {
    echo "[$(date)] <<<<<< start DM-038 pessimistic >>>>>>"

    init_table 111 211 212

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-double-source.yaml --remove-meta" \
            "\"result\": true" 3

    DM_038_CASE

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-038 pessimistic >>>>>>"
}

function DM_038_OPTIMISTIC() {
    echo "[$(date)] <<<<<< start DM-038 optimistic >>>>>>"

    init_table 111 211 212

    cp $cur/conf/dm-task-double-source.yaml $WORK_DIR/task.yaml
    sed -i "s/shard-mode: \"pessimistic\"/shard-mode: \"optimistic\"/g" $WORK_DIR/task.yaml
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $WORK_DIR/task.yaml --remove-meta" \
            "\"result\": true" 3

    DM_038_CASE

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-038 optimistic >>>>>>"        
}

function DM_038() {
    echo "[$(date)] <<<<<< start DM-038 >>>>>>"

    DM_038_PESSIMISTIC
    DM_038_OPTIMISTIC

    echo "[$(date)] <<<<<< finish DM-038 >>>>>>"
}

function DM_039_CASE {
    run_sql_source1 "alter table ${shardddl1}.${tb1} add column col1 varchar(10) character set utf8 collate utf8_bin;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add column col1 varchar(10) character set utf8 collate utf8_bin;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,'bbb');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,'ccc');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(6);"
    run_sql_source2 "alter table ${shardddl1}.${tb2} add column col1 varchar(10) character set utf8 collate utf8_bin;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,'ddd');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,'eee');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,'fff');"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_039_PESSIMISTIC() {
    echo "[$(date)] <<<<<< start DM-039 pessimistic >>>>>>"

    init_table 111 211 212

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-double-source.yaml --remove-meta" \
            "\"result\": true" 3

    DM_039_CASE

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-039 pessimistic >>>>>>"
}

function DM_039_OPTIMISTIC() {
    echo "[$(date)] <<<<<< start DM-039 optimistic >>>>>>"

    init_table 111 211 212

    cp $cur/conf/dm-task-double-source.yaml $WORK_DIR/task.yaml
    sed -i "s/shard-mode: \"pessimistic\"/shard-mode: \"optimistic\"/g" $WORK_DIR/task.yaml
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $WORK_DIR/task.yaml --remove-meta" \
            "\"result\": true" 3

    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

    DM_039_CASE

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-039 optimistic >>>>>>"        
}

function DM_039() {
    echo "[$(date)] <<<<<< start DM-039 >>>>>>"

    DM_039_PESSIMISTIC
    DM_039_OPTIMISTIC

    echo "[$(date)] <<<<<< finish DM-039 >>>>>>"
}

function DM_040_CASE {
    run_sql_source1 "alter table ${shardddl1}.${tb1} add column col1 varchar(10) character set utf8 collate utf8_bin;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add column col1 varchar(10) character set utf8mb4 collate utf8mb4_bin;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,'bbb');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,'ccc');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(6);"
    run_sql_source2 "alter table ${shardddl1}.${tb2} add column col1 varchar(10) character set utf8mb4 collate utf8mb4_bin;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,'ddd');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,'eee');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,'fff');"
    sleep 2
    check_log_contains $WORK_DIR/master/log/dm-master.log "is different with"
}

function DM_040_PESSIMISTIC() {
    echo "[$(date)] <<<<<< start DM-040 pessimistic >>>>>>"

    init_table 111 211 212

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-double-source.yaml --remove-meta" \
            "\"result\": true" 3

    DM_040_CASE

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-040 pessimistic >>>>>>"
}

function DM_040_OPTIMISTIC() {
    echo "[$(date)] <<<<<< start DM-040 optimistic >>>>>>"

    init_table 111 211 212

    cp $cur/conf/dm-task-double-source.yaml $WORK_DIR/task.yaml
    sed -i "s/shard-mode: \"pessimistic\"/shard-mode: \"optimistic\"/g" $WORK_DIR/task.yaml
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $WORK_DIR/task.yaml --remove-meta" \
            "\"result\": true" 3

    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

    DM_040_CASE

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-040 optimistic >>>>>>"        
}

function DM_040() {
    echo "[$(date)] <<<<<< start DM-040 >>>>>>"

    DM_040_PESSIMISTIC
    DM_040_OPTIMISTIC

    echo "[$(date)] <<<<<< finish DM-040 >>>>>>"
}

function DM_041_CASE {
    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int as (id+1);"
    run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} (id) values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} (id) values(3);"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 int as (id+1);"
    run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values(4);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} (id) values(5);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} (id) values(6);"
    run_sql_source2 "alter table ${shardddl1}.${tb2} add column new_col1 int as (id+1);"
    run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values(7);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} (id) values(8);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} (id) values(9);"
    sleep 2
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_041_PESSIMISTIC() {
    echo "[$(date)] <<<<<< start DM-041 pessimistic >>>>>>"

    init_table 111 211 212

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-double-source.yaml --remove-meta" \
            "\"result\": true" 3

    DM_041_CASE

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-041 pessimistic >>>>>>"
}

function DM_041_OPTIMISTIC() {
    echo "[$(date)] <<<<<< start DM-041 optimistic >>>>>>"

    init_table 111 211 212

    cp $cur/conf/dm-task-double-source.yaml $WORK_DIR/task.yaml
    sed -i "s/shard-mode: \"pessimistic\"/shard-mode: \"optimistic\"/g" $WORK_DIR/task.yaml
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $WORK_DIR/task.yaml --remove-meta" \
            "\"result\": true" 3

    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

    DM_041_CASE

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-041 optimistic >>>>>>"        
}

function DM_041() {
    echo "[$(date)] <<<<<< start DM-041 >>>>>>"

    DM_041_PESSIMISTIC
    DM_041_OPTIMISTIC

    echo "[$(date)] <<<<<< finish DM-041 >>>>>>"
}

function DM_043_CASE {
    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int as (id+1);"
    run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} (id) values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} (id) values(3);"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 int as (id+2);"
    run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values(4);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} (id) values(5);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} (id) values(6);"
    run_sql_source2 "alter table ${shardddl1}.${tb2} add column new_col1 int as (id+2);"
    run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values(7);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} (id) values(8);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} (id) values(9);"
    sleep 2
    check_log_contains $WORK_DIR/master/log/dm-master.log "is different with"
}

function DM_043_PESSIMISTIC() {
    echo "[$(date)] <<<<<< start DM-043 pessimistic >>>>>>"

    init_table 111 211 212

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-double-source.yaml --remove-meta" \
            "\"result\": true" 3

    DM_043_CASE

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-043 pessimistic >>>>>>"
}

function DM_043_OPTIMISTIC() {
    echo "[$(date)] <<<<<< start DM-043 optimistic >>>>>>"

    init_table 111 211 212

    cp $cur/conf/dm-task-double-source.yaml $WORK_DIR/task.yaml
    sed -i "s/shard-mode: \"pessimistic\"/shard-mode: \"optimistic\"/g" $WORK_DIR/task.yaml
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $WORK_DIR/task.yaml --remove-meta" \
            "\"result\": true" 3

    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

    DM_043_CASE

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-043 optimistic >>>>>>"        
}

function DM_043() {
    echo "[$(date)] <<<<<< start DM-043 >>>>>>"

    DM_043_PESSIMISTIC
    DM_043_OPTIMISTIC

    echo "[$(date)] <<<<<< finish DM-043 >>>>>>"
}

function DM_046_CASE {
    run_sql_source1 "alter table ${shardddl1}.${tb1} drop column b;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'aaa');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'bbb');"
    run_sql_source2 "alter table ${shardddl1}.${tb1} drop column b;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(4);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(5);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,'ccc');"
    run_sql_source2 "alter table ${shardddl1}.${tb2} drop column b;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(7);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(8);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(9);"
    sleep 2
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_046_PESSIMISTIC() {
    echo "[$(date)] <<<<<< start DM-046 pessimistic >>>>>>"

    run_sql_source1 "create table ${shardddl1}.${tb1} (a int, b varchar(10));"
    run_sql_source2 "create table ${shardddl1}.${tb1} (a int, b varchar(10));"
    run_sql_source2 "create table ${shardddl1}.${tb2} (a int, b varchar(10));"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-double-source.yaml --remove-meta" \
            "\"result\": true" 3

    DM_046_CASE

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-046 pessimistic >>>>>>"
}

function DM_046_OPTIMISTIC() {
    echo "[$(date)] <<<<<< start DM-046 optimistic >>>>>>"

    run_sql_source1 "create table ${shardddl1}.${tb1} (a int, b varchar(10));"
    run_sql_source2 "create table ${shardddl1}.${tb1} (a int, b varchar(10));"
    run_sql_source2 "create table ${shardddl1}.${tb2} (a int, b varchar(10));"

    cp $cur/conf/dm-task-double-source.yaml $WORK_DIR/task.yaml
    sed -i "s/shard-mode: \"pessimistic\"/shard-mode: \"optimistic\"/g" $WORK_DIR/task.yaml
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $WORK_DIR/task.yaml --remove-meta" \
            "\"result\": true" 3

    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'ccc');"

    DM_046_CASE

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-046 optimistic >>>>>>"        
}

function DM_046() {
    echo "[$(date)] <<<<<< start DM-046 >>>>>>"

    DM_046_PESSIMISTIC
    DM_046_OPTIMISTIC

    echo "[$(date)] <<<<<< finish DM-046 >>>>>>"
}

function DM_047_CASE {
    run_sql_source1 "alter table ${shardddl1}.${tb1} drop column c;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} (a,b) values(2,'bbb');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} (a,b) values(3,'ccc');"
    run_sql_source2 "alter table ${shardddl1}.${tb1} drop column c;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,'ddd');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,'eee');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} (a,b) values(6,'fff');"
    run_sql_source2 "alter table ${shardddl1}.${tb2} drop column c;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,'ggg');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,'hhh');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,'iii');"
    sleep 2
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_047_PESSIMISTIC() {
    echo "[$(date)] <<<<<< start DM-047 pessimistic >>>>>>"

    run_sql_source1 "create table ${shardddl1}.${tb1} (a int, b varchar(10), c int as (a+1) stored);"
    run_sql_source2 "create table ${shardddl1}.${tb1} (a int, b varchar(10), c int as (a+1) stored);"
    run_sql_source2 "create table ${shardddl1}.${tb2} (a int, b varchar(10), c int as (a+1) stored);"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-double-source.yaml --remove-meta" \
            "\"result\": true" 3

    DM_047_CASE

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-047 pessimistic >>>>>>"
}

function DM_047_OPTIMISTIC() {
    echo "[$(date)] <<<<<< start DM-047 optimistic >>>>>>"

    run_sql_source1 "create table ${shardddl1}.${tb1} (a int, b varchar(10), c int as (a+1) stored);"
    run_sql_source2 "create table ${shardddl1}.${tb1} (a int, b varchar(10), c int as (a+1) stored);"
    run_sql_source2 "create table ${shardddl1}.${tb2} (a int, b varchar(10), c int as (a+1) stored);"

    cp $cur/conf/dm-task-double-source.yaml $WORK_DIR/task.yaml
    sed -i "s/shard-mode: \"pessimistic\"/shard-mode: \"optimistic\"/g" $WORK_DIR/task.yaml
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $WORK_DIR/task.yaml --remove-meta" \
            "\"result\": true" 3

    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} (a,b) values(1,'aaa');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} (a,b) values(2,'bbb');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} (a,b) values(3,'ccc');"

    DM_047_CASE

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-047 optimistic >>>>>>"        
}

function DM_047() {
    echo "[$(date)] <<<<<< start DM-047 >>>>>>"

    DM_047_PESSIMISTIC
    DM_047_OPTIMISTIC

    echo "[$(date)] <<<<<< finish DM-047 >>>>>>"
}

function DM_048_CASE {
    run_sql_source1 "alter table ${shardddl1}.${tb1} drop column c;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} (a,b) values(2,'bbb');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} (a,b) values(3,'ccc');"
    run_sql_source2 "alter table ${shardddl1}.${tb1} drop column c;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,'ddd');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,'eee');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} (a,b) values(6,'fff');"
    run_sql_source2 "alter table ${shardddl1}.${tb2} drop column c;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,'ggg');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,'hhh');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,'iii');"
    sleep 2
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_048_PESSIMISTIC() {
    echo "[$(date)] <<<<<< start DM-048 pessimistic >>>>>>"

    run_sql_source1 "create table ${shardddl1}.${tb1} (a int, b varchar(10), c int as (a+1) virtual);"
    run_sql_source2 "create table ${shardddl1}.${tb1} (a int, b varchar(10), c int as (a+1) virtual);"
    run_sql_source2 "create table ${shardddl1}.${tb2} (a int, b varchar(10), c int as (a+1) virtual);"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-double-source.yaml --remove-meta" \
            "\"result\": true" 3

    DM_048_CASE

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-048 pessimistic >>>>>>"
}

function DM_048_OPTIMISTIC() {
    echo "[$(date)] <<<<<< start DM-048 optimistic >>>>>>"

    run_sql_source1 "create table ${shardddl1}.${tb1} (a int, b varchar(10), c int as (a+1) virtual);"
    run_sql_source2 "create table ${shardddl1}.${tb1} (a int, b varchar(10), c int as (a+1) virtual);"
    run_sql_source2 "create table ${shardddl1}.${tb2} (a int, b varchar(10), c int as (a+1) virtual);"

    cp $cur/conf/dm-task-double-source.yaml $WORK_DIR/task.yaml
    sed -i "s/shard-mode: \"pessimistic\"/shard-mode: \"optimistic\"/g" $WORK_DIR/task.yaml
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $WORK_DIR/task.yaml --remove-meta" \
            "\"result\": true" 3

    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} (a,b) values(1,'aaa');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} (a,b) values(2,'bbb');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} (a,b) values(3,'ccc');"

    DM_048_CASE

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-048 optimistic >>>>>>"        
}

function DM_048() {
    echo "[$(date)] <<<<<< start DM-048 >>>>>>"

    DM_048_PESSIMISTIC
    DM_048_OPTIMISTIC

    echo "[$(date)] <<<<<< finish DM-048 >>>>>>"
}

function DM_049_CASE {
    run_sql_source1 "alter table ${shardddl1}.${tb1} change a c int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'ccc');"
    run_sql_source2 "alter table ${shardddl1}.${tb1} change a c int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,'ddd');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,'eee');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,'fff');"
    run_sql_source2 "alter table ${shardddl1}.${tb2} change a c int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,'ggg');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,'hhh');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,'iii');"
}

function DM_049_PESSIMISTIC() {
    echo "[$(date)] <<<<<< start DM-049 pessimistic >>>>>>"

    run_sql_source1 "create table ${shardddl1}.${tb1} (a int, b varchar(10));"
    run_sql_source2 "create table ${shardddl1}.${tb1} (a int, b varchar(10));"
    run_sql_source2 "create table ${shardddl1}.${tb2} (a int, b varchar(10));"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-double-source.yaml --remove-meta" \
            "\"result\": true" 3

    DM_049_CASE
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-049 pessimistic >>>>>>"
}

function DM_049_OPTIMISTIC() {
    echo "[$(date)] <<<<<< start DM-049 optimistic >>>>>>"

    run_sql_source1 "create table ${shardddl1}.${tb1} (a int, b varchar(10));"
    run_sql_source2 "create table ${shardddl1}.${tb1} (a int, b varchar(10));"
    run_sql_source2 "create table ${shardddl1}.${tb2} (a int, b varchar(10));"

    cp $cur/conf/dm-task-double-source.yaml $WORK_DIR/task.yaml
    sed -i "s/shard-mode: \"pessimistic\"/shard-mode: \"optimistic\"/g" $WORK_DIR/task.yaml
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $WORK_DIR/task.yaml --remove-meta" \
            "\"result\": true" 3

    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'ccc');"

    DM_049_CASE
    sleep 2
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "because schema conflict detected"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-049 optimistic >>>>>>"        
}

function DM_049() {
    echo "[$(date)] <<<<<< start DM-049 >>>>>>"

    DM_049_PESSIMISTIC
    DM_049_OPTIMISTIC

    echo "[$(date)] <<<<<< finish DM-049 >>>>>>"
}

function DM_050_CASE {
    run_sql_source1 "alter table ${shardddl1}.${tb1} change a c int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'ccc');"
    run_sql_source2 "alter table ${shardddl1}.${tb1} change a d int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,'ddd');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,'eee');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,'fff');"
    run_sql_source2 "alter table ${shardddl1}.${tb2} change a d int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,'ggg');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,'hhh');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,'iii');"
}

function DM_050_PESSIMISTIC() {
    echo "[$(date)] <<<<<< start DM-050 pessimistic >>>>>>"

    run_sql_source1 "create table ${shardddl1}.${tb1} (a int, b varchar(10));"
    run_sql_source2 "create table ${shardddl1}.${tb1} (a int, b varchar(10));"
    run_sql_source2 "create table ${shardddl1}.${tb2} (a int, b varchar(10));"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-double-source.yaml --remove-meta" \
            "\"result\": true" 3

    DM_050_CASE
    check_log_contains $WORK_DIR/master/log/dm-master.log "is different with"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-050 pessimistic >>>>>>"
}

function DM_050_OPTIMISTIC() {
    echo "[$(date)] <<<<<< start DM-050 optimistic >>>>>>"

    run_sql_source1 "create table ${shardddl1}.${tb1} (a int, b varchar(10));"
    run_sql_source2 "create table ${shardddl1}.${tb1} (a int, b varchar(10));"
    run_sql_source2 "create table ${shardddl1}.${tb2} (a int, b varchar(10));"

    cp $cur/conf/dm-task-double-source.yaml $WORK_DIR/task.yaml
    sed -i "s/shard-mode: \"pessimistic\"/shard-mode: \"optimistic\"/g" $WORK_DIR/task.yaml
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $WORK_DIR/task.yaml --remove-meta" \
            "\"result\": true" 3

    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'ccc');"

    DM_050_CASE
    sleep 2
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "because schema conflict detected"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-050 optimistic >>>>>>"        
}

function DM_050() {
    echo "[$(date)] <<<<<< start DM-050 >>>>>>"

    DM_050_PESSIMISTIC
    DM_050_OPTIMISTIC

    echo "[$(date)] <<<<<< finish DM-050 >>>>>>"
}

function DM_051_CASE {
    run_sql_source1 "alter table ${shardddl1}.${tb1} change a c int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"
    run_sql_source2 "alter table ${shardddl1}.${tb1} change b c int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6);"
    run_sql_source2 "alter table ${shardddl1}.${tb2} change b c int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9);"
}

function DM_051_PESSIMISTIC() {
    echo "[$(date)] <<<<<< start DM-051 pessimistic >>>>>>"

    run_sql_source1 "create table ${shardddl1}.${tb1} (a int, b int);"
    run_sql_source2 "create table ${shardddl1}.${tb1} (a int, b int);"
    run_sql_source2 "create table ${shardddl1}.${tb2} (a int, b int);"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-double-source.yaml --remove-meta" \
            "\"result\": true" 3

    DM_051_CASE
    sleep 2
    check_log_contains $WORK_DIR/master/log/dm-master.log "is different with"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-051 pessimistic >>>>>>"
}

function DM_051_OPTIMISTIC() {
    echo "[$(date)] <<<<<< start DM-051 optimistic >>>>>>"

    run_sql_source1 "create table ${shardddl1}.${tb1} (a int, b int);"
    run_sql_source2 "create table ${shardddl1}.${tb1} (a int, b int);"
    run_sql_source2 "create table ${shardddl1}.${tb2} (a int, b int);"

    cp $cur/conf/dm-task-double-source.yaml $WORK_DIR/task.yaml
    sed -i "s/shard-mode: \"pessimistic\"/shard-mode: \"optimistic\"/g" $WORK_DIR/task.yaml
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $WORK_DIR/task.yaml --remove-meta" \
            "\"result\": true" 3

    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"

    DM_051_CASE
    sleep 2
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "because schema conflict detected"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-051 optimistic >>>>>>"        
}

function DM_051() {
    echo "[$(date)] <<<<<< start DM-051 >>>>>>"

    DM_051_PESSIMISTIC
    DM_051_OPTIMISTIC

    echo "[$(date)] <<<<<< finish DM-051 >>>>>>"
}

function DM_056_CASE {
    run_sql_source1 "alter table ${shardddl1}.${tb1} change a c int after b;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"
    run_sql_source2 "alter table ${shardddl1}.${tb1} change b c int first;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6);"
    run_sql_source2 "alter table ${shardddl1}.${tb2} change b c int first;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9);"
}

function DM_056_PESSIMISTIC() {
    echo "[$(date)] <<<<<< start DM-056 pessimistic >>>>>>"

    run_sql_source1 "create table ${shardddl1}.${tb1} (a int, b int);"
    run_sql_source2 "create table ${shardddl1}.${tb1} (a int, b int);"
    run_sql_source2 "create table ${shardddl1}.${tb2} (a int, b int);"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-double-source.yaml --remove-meta" \
            "\"result\": true" 3

    DM_056_CASE
    sleep 2
    check_log_contains $WORK_DIR/master/log/dm-master.log "is different with"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-056 pessimistic >>>>>>"
}

function DM_056_OPTIMISTIC() {
    echo "[$(date)] <<<<<< start DM-056 optimistic >>>>>>"

    run_sql_source1 "create table ${shardddl1}.${tb1} (a int, b int);"
    run_sql_source2 "create table ${shardddl1}.${tb1} (a int, b int);"
    run_sql_source2 "create table ${shardddl1}.${tb2} (a int, b int);"

    cp $cur/conf/dm-task-double-source.yaml $WORK_DIR/task.yaml
    sed -i "s/shard-mode: \"pessimistic\"/shard-mode: \"optimistic\"/g" $WORK_DIR/task.yaml
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $WORK_DIR/task.yaml --remove-meta" \
            "\"result\": true" 3

    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"

    DM_056_CASE
    sleep 2
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "because schema conflict detected"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-056 optimistic >>>>>>"        
}

function DM_056() {
    echo "[$(date)] <<<<<< start DM-056 >>>>>>"

    DM_056_PESSIMISTIC
    DM_056_OPTIMISTIC

    echo "[$(date)] <<<<<< finish DM-056 >>>>>>"
}

function DM_057_CASE {
    run_sql_source1 "alter table ${shardddl1}.${tb1} change id new_col int default 1;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"
    run_sql_source2 "alter table ${shardddl1}.${tb1} change id new_col int default 1;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(4);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(5);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(6);"
    run_sql_source2 "alter table ${shardddl1}.${tb2} change id new_col int default 1;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(7);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(8);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(9);"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_057_PESSIMISTIC() {
    echo "[$(date)] <<<<<< start DM-057 pessimistic >>>>>>"

    init_table 111 211 212

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-double-source.yaml --remove-meta" \
            "\"result\": true" 3

    DM_057_CASE

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-057 pessimistic >>>>>>"
}

function DM_057_OPTIMISTIC() {
    echo "[$(date)] <<<<<< start DM-057 optimistic >>>>>>"

    init_table 111 211 212

    cp $cur/conf/dm-task-double-source.yaml $WORK_DIR/task.yaml
    sed -i "s/shard-mode: \"pessimistic\"/shard-mode: \"optimistic\"/g" $WORK_DIR/task.yaml
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $WORK_DIR/task.yaml --remove-meta" \
            "\"result\": true" 3

    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

    DM_057_CASE

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-057 optimistic >>>>>>"        
}

function DM_057() {
    echo "[$(date)] <<<<<< start DM-057 >>>>>>"

    DM_057_PESSIMISTIC
    # currently not support optimistic
    # DM_057_OPTIMISTIC

    echo "[$(date)] <<<<<< finish DM-057 >>>>>>"
}

function DM_058_CASE {
    run_sql_source1 "alter table ${shardddl1}.${tb1} change id new_col int default 1;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"
    run_sql_source2 "alter table ${shardddl1}.${tb1} change id new_col int default 2;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(4);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(5);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(6);"
    run_sql_source2 "alter table ${shardddl1}.${tb2} change id new_col int default 2;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(7);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(8);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(9);"
}

function DM_058_PESSIMISTIC() {
    echo "[$(date)] <<<<<< start DM-058 pessimistic >>>>>>"

    init_table 111 211 212

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-double-source.yaml --remove-meta" \
            "\"result\": true" 3

    DM_058_CASE
    sleep 2
    check_log_contains $WORK_DIR/master/log/dm-master.log "is different with"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-058 pessimistic >>>>>>"
}

function DM_058_OPTIMISTIC() {
    echo "[$(date)] <<<<<< start DM-058 optimistic >>>>>>"

    init_table 111 211 212

    cp $cur/conf/dm-task-double-source.yaml $WORK_DIR/task.yaml
    sed -i "s/shard-mode: \"pessimistic\"/shard-mode: \"optimistic\"/g" $WORK_DIR/task.yaml
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $WORK_DIR/task.yaml --remove-meta" \
            "\"result\": true" 3

    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

    DM_058_CASE
    sleep 2
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "because schema conflict detected"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-058 optimistic >>>>>>"        
}

function DM_058() {
    echo "[$(date)] <<<<<< start DM-058 >>>>>>"

    DM_058_PESSIMISTIC
    DM_058_OPTIMISTIC

    echo "[$(date)] <<<<<< finish DM-058 >>>>>>"
}

function DM_059_CASE {
    run_sql_source1 "alter table ${shardddl1}.${tb1} change id new_col datetime default now();"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(now());"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(now());"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(now());"
    run_sql_source2 "alter table ${shardddl1}.${tb1} change id new_col datetime default now();"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(now());"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(now());"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(now());"
    run_sql_source2 "alter table ${shardddl1}.${tb2} change id new_col datetime default now();"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(now());"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(now());"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(now());"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_059_PESSIMISTIC() {
    echo "[$(date)] <<<<<< start DM-059 pessimistic >>>>>>"

    run_sql_source1 "create table ${shardddl1}.${tb1} (id datetime);"
    run_sql_source2 "create table ${shardddl1}.${tb1} (id datetime);"
    run_sql_source2 "create table ${shardddl1}.${tb2} (id datetime);"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-double-source.yaml --remove-meta" \
            "\"result\": true" 3

    DM_059_CASE

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-059 pessimistic >>>>>>"
}

function DM_059_OPTIMISTIC() {
    echo "[$(date)] <<<<<< start DM-059 optimistic >>>>>>"

    run_sql_source1 "create table ${shardddl1}.${tb1} (id datetime);"
    run_sql_source2 "create table ${shardddl1}.${tb1} (id datetime);"
    run_sql_source2 "create table ${shardddl1}.${tb2} (id datetime);"

    cp $cur/conf/dm-task-double-source.yaml $WORK_DIR/task.yaml
    sed -i "s/shard-mode: \"pessimistic\"/shard-mode: \"optimistic\"/g" $WORK_DIR/task.yaml
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $WORK_DIR/task.yaml --remove-meta" \
            "\"result\": true" 3

    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/593
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(now());"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(now());"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(now());"

    DM_059_CASE
    sleep 2
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "because schema conflict detected"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-059 optimistic >>>>>>"        
}

function DM_059() {
    echo "[$(date)] <<<<<< start DM-059 >>>>>>"

    DM_059_PESSIMISTIC
    # DM_059_OPTIMISTIC

    echo "[$(date)] <<<<<< finish DM-059 >>>>>>"
}

function DM_060_CASE {
    run_sql_source1 "alter table ${shardddl1}.${tb1} change column b b varchar(10) character set utf8 collate utf8_bin"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'ccc');"
    run_sql_source2 "alter table ${shardddl1}.${tb1} change column b b varchar(10) character set utf8 collate utf8_bin"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,'ddd');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,'eee');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,'fff');"
    run_sql_source2 "alter table ${shardddl1}.${tb2} change column b b varchar(10) character set utf8 collate utf8_bin"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,'ggg');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,'hhh');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,'iii');"
}

function DM_060_PESSIMISTIC() {
    echo "[$(date)] <<<<<< start DM-060 pessimistic >>>>>>"

    run_sql_source1 "create table ${shardddl1}.${tb1} (a int, b varchar(10));"
    run_sql_source2 "create table ${shardddl1}.${tb1} (a int, b varchar(10));"
    run_sql_source2 "create table ${shardddl1}.${tb2} (a int, b varchar(10));"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $cur/conf/dm-task-double-source.yaml --remove-meta" \
            "\"result\": true" 3

    DM_060_CASE
    sleep 2
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Unsupported modify charset from latin1 to utf8" 2

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-060 pessimistic >>>>>>"
}

function DM_060_OPTIMISTIC() {
    echo "[$(date)] <<<<<< start DM-060 optimistic >>>>>>"

    run_sql_source1 "create table ${shardddl1}.${tb1} (a int, b varchar(10));"
    run_sql_source2 "create table ${shardddl1}.${tb1} (a int, b varchar(10));"
    run_sql_source2 "create table ${shardddl1}.${tb2} (a int, b varchar(10));"

    cp $cur/conf/dm-task-double-source.yaml $WORK_DIR/task.yaml
    sed -i "s/shard-mode: \"pessimistic\"/shard-mode: \"optimistic\"/g" $WORK_DIR/task.yaml
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $WORK_DIR/task.yaml --remove-meta" \
            "\"result\": true" 3

    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'ccc');"

    DM_060_CASE
    sleep 2
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "because schema conflict detected"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "stop-task test" \
            "\"result\": true" 3

    clean_table

    echo "[$(date)] <<<<<< finish DM-060 optimistic >>>>>>"        
}

function DM_060() {
    echo "[$(date)] <<<<<< start DM-060 >>>>>>"

    DM_060_PESSIMISTIC
    DM_060_OPTIMISTIC

    echo "[$(date)] <<<<<< finish DM-060 >>>>>>"
}

function run() {
    init_cluster
    init_database
    except=(024 025 029 042 044 045)
    for i in $(seq -f "%03g" 60 60); do
        if [[ ${except[@]} =~ $i ]]; then
            continue
        fi
        DM_${i}
        sleep 1
    done
}

cleanup_data $shardddl
cleanup_data $shardddl1
cleanup_data $shardddl2
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
