#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
source $cur/lib.sh

function DM_001_CASE() {
    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "query-status test" \
        "Duplicate column name 'new_col1'" 1
}

function DM_001() {
    run_case 001 "no-sharding" "init_table 111 112" "clean_table" ""
}

function DM_002_CASE() {
    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_002() {
    run_case 002 "single-source-pessimistic" "init_table 111 112" "clean_table" ""
}

function DM_003_CASE() {
    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 3 "fail"
    run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_003() {
    run_case 003 "single-source-pessimistic" "init_table 111 112" "clean_table" "pessimistic"
}

function DM_004_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (1);"
    run_sql_source1 "insert into ${shardddl1}.${tb2} values (2);"

    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
    run_sql_tidb_with_retry "select count(1) from ${shardddl}.${tb};" "count(1): 3"
    run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb2} values (2,2)"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_004() {
    run_case 004 "single-source-optimistic" "init_table 111 112" "clean_table" "optimistic"
}

function DM_005_CASE() {
    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
    run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb2} values (2,2)"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_005() {
    run_case 005 "single-source-pessimistic" "init_table 111 112" "clean_table" ""
}

function DM_006_CASE() {
    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values (2,2)"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_006() {
    run_case 006 "double-source-pessimistic" "init_table 111 211" "clean_table" ""
}

function DM_007_CASE() {
    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
    run_sql_source2 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values (2,2)"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_007() {
    run_case 007 "double-source-pessimistic" "init_table 111 212" "clean_table" ""
}

function DM_008_CASE() {
    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
    run_sql_source2 "alter table ${shardddl2}.${tb1} add column new_col1 int;"
    run_sql_source2 "insert into ${shardddl2}.${tb1} values (2,2)"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_008() {
    run_case 008 "double-source-pessimistic" "init_table 111 221" "clean_table" ""
}

function DM_009_CASE() {
    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
    run_sql_source2 "alter table ${shardddl2}.${tb2} add column new_col1 int;"
    run_sql_source2 "insert into ${shardddl2}.${tb2} values (2,2)"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_009() {
    run_case 009 "double-source-pessimistic" "init_table 111 222" "clean_table" ""
}

function DM_010_CASE() {
    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col2 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (2,2,2)"
    run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb2} values (3,3)"
    run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col2 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb2} values (4,4,4)"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_010() {
    run_case 010 "single-source-pessimistic" "init_table 111 112" "clean_table" ""
}

function DM_011_CASE() {
    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col2 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (2,2,2)"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 float;"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values (3,3.0)"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col2 float;"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values (4,4.0,4.0)"
    check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
}

function DM_011() {
    run_case 011 "double-source-pessimistic" "init_table 111 211" "clean_table" ""
}

function DM_012_CASE() {
    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col2 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (2,2,2)"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col2 int;"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values (3,3)"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values (4,4,4)"
    check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
}

function DM_012() {
    run_case 012 "double-source-pessimistic" "init_table 111 211" "clean_table" ""
}

function DM_013_CASE() {
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
}

function DM_013() {
    run_case 013 "single-source-optimistic" "init_table 111 112" "clean_table" ""
}

function DM_014_CASE() {
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
    run_sql_tidb_with_retry "select count(1) from ${shardddl}.${tb};" "count(1): 6"
}

function DM_014() {
    run_case 014 "single-source-optimistic" "init_table 111 112" "clean_table" ""
}

function DM_015_CASE() {
    run_sql_source1 "drop database ${shardddl1};"
    check_log_contain_with_retry "skip event, need handled ddls is empty" $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log 
    run_sql_source1 "create database ${shardddl1};"
    check_log_contain_with_retry "CREATE DATABASE IF NOT EXISTS \`${shardddl1}\`" $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log 
}

function DM_015() {
    run_case 015 "single-source-pessimistic" "init_table 111" "clean_table 111" ""
}

function DM_016_CASE() {
    run_sql_source1 "drop database ${shardddl1};"
    check_log_contain_with_retry "skip event, need handled ddls is empty" $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log 
    run_sql_source1 "create database if not exists ${shardddl1};"
    check_log_contain_with_retry "CREATE DATABASE IF NOT EXISTS \`${shardddl1}\`" $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log 
}

function DM_016() {
    run_case 016 "single-source-pessimistic" "init_table 111" "clean_table" ""
}

function DM_017_CASE() {
    run_sql_source1 "drop table ${shardddl1}.${tb1};"
    check_log_contain_with_retry "skip event, need handled ddls is empty" $WORK_DIR/worker1/log/dm-worker.log  $WORK_DIR/worker2/log/dm-worker.log
    run_sql_source1 "create table ${shardddl1}.${tb1}(id int);"
    check_log_contain_with_retry "CREATE TABLE IF NOT EXISTS \`${shardddl1}\`.\`${tb1}\` (\`id\` INT)" $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log
}

function DM_017() {
    run_case 017 "single-source-pessimistic" "init_table 111" "clean_table" ""
}

function DM_018_CASE() {
    run_sql_source1 "drop table ${shardddl1}.${tb1};"
    check_log_contain_with_retry "skip event, need handled ddls is empty" $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log 
    run_sql_source1 "create table if not exists ${shardddl1}.${tb1}(id int);"
    check_log_contain_with_retry "CREATE TABLE IF NOT EXISTS \`${shardddl1}\`.\`${tb1}\` (\`id\` INT)" $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log
}

function DM_018() {
    run_case 018 "single-source-pessimistic" "init_table 111" "clean_table" ""
}

function DM_019_CASE() {
    run_sql_source1 "truncate table ${shardddl1}.${tb1};"
    check_log_contain_with_retry "skip event, need handled ddls is empty" $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log
}

function DM_019() {
    run_case 019 "single-source-pessimistic" "init_table 111" "clean_table" ""
}

function DM_020_CASE() {
    run_sql_source1 "rename table ${shardddl1}.${tb1} to ${shardddl1}.${tb2};"
    run_sql_source1 "insert into ${shardddl1}.${tb2} values (1);"
    run_sql_tidb_with_retry "show tables from ${shardddl1};" "${tb2}"
    run_sql_tidb_with_retry "select count(1) from ${shardddl1}.${tb2};" "count(1): 1"
}

function DM_020() {
    run_case 020 "single-source-no-routes" "init_table 111" "clean_table;run_sql_tidb \"drop database ${shardddl1};\"" ""
}

function DM_021_CASE() {
    # same as "rename ${shardddl}.${tb} to ${shardddl}.${tb};"
    run_sql_source1 "rename table ${shardddl1}.${tb1} to ${shardddl1}.${tb2};"
    check_log_contain_with_retry "Table '${shardddl}.${tb}' already exist" $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log
}

function DM_021() {
    run_case 021 "single-source-pessimistic" "init_table 111" "clean_table" ""
}

function DM_022_CASE() {
    sleep 5
    run_sql_tidb "create table ${shardddl1}.${tb2} (id int);"
    run_sql_source1 "rename table ${shardddl1}.${tb1} to ${shardddl1}.${tb2};"
    check_log_contain_with_retry "Table '${shardddl1}.${tb2}' already exists" $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log 
}

function DM_022() {
    run_case 022 "single-source-no-routes" "init_table 111" "clean_table;run_sql_tidb \"drop database ${shardddl1};\"" "" 
}

function DM_023_CASE() {
    run_sql_source1 "rename table ${shardddl1}.${tb1} to ${shardddl1}.${tb3}, ${shardddl1}.${tb2} to ${shardddl1}.${tb4};"
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "rename table .* not supported" 1
}

function DM_023() {
    run_case 023 "double-source-pessimistic" "init_table 111 112" "clean_table;run_sql_source1 \"drop table ${shardddl1}.${tb3};\";run_sql_source1 \"drop table ${shardddl1}.${tb4};\"" ""
}

function DM_026_CASE() {
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (1)"
    run_sql_source1 "insert into ${shardddl1}.${tb2} values (2)"
    run_sql_source1 "create table ${shardddl1}.${tb3}(id int);"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (3)"
    run_sql_source1 "insert into ${shardddl1}.${tb2} values (4)"
    run_sql_source1 "insert into ${shardddl1}.${tb3} values (5)"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_026() {
    run_case 026 "single-source-pessimistic" "init_table 111 112" "clean_table;run_sql_source1 \"drop table ${shardddl1}.${tb3};\"" ""
}

function DM_027_CASE() {
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (1)"
    run_sql_source1 "insert into ${shardddl1}.${tb2} values (2)"
    run_sql_source1 "create table ${shardddl1}.${tb3}(id int,val int);"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (3)"
    run_sql_source1 "insert into ${shardddl1}.${tb2} values (4)"
    run_sql_source1 "insert into ${shardddl1}.${tb3} values (5,6)"
    # we now haven't checked table struct when create sharding table
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Unknown column 'val' in 'field list'" 1
}

function DM_027() {
    run_case 027 "single-source-pessimistic" "init_table 111 112" "clean_table;run_sql_source1 \"drop table ${shardddl1}.${tb3};\"" ""
}

function DM_028_CASE() {
    run_sql_source1 "alter table ${shardddl1}.${tb1} drop primary key;"
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Unsupported drop primary key when alter-primary-key is false" 1
}

function DM_028() {
    run_case 028 "single-source-pessimistic" "run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key);\"" "clean_table" ""
}

function DM_030_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"

    run_sql_source1 "alter table ${shardddl1}.${tb1} add new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(3,3);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(4,4);"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_030() {
    run_case 030 "double-source-pessimistic" "init_table 111 211" "clean_table" "pessimistic"
    run_case 030 "double-source-optimistic" "init_table 111 211" "clean_table" "optimistic"
}

function DM_031_CASE() {
    run_sql_source1 "alter table ${shardddl1}.${tb1} add new_col1 int;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add new_col1 varchar(10);"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(3,3);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(4,'dkfj');"
    check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
}

function DM_031() {
    run_case 031 "double-source-pessimistic" "init_table 111 211" "clean_table" "pessimistic"
    run_case 031 "double-source-optimistic" "init_table 111 211" "clean_table" "optimistic"
}

function DM_032_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

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

function DM_032() {
    # currently not support pessimistic
    # run_case 032 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
    run_case 032 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_033_CASE() {
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

function DM_033() {
    run_case 033 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
    # currently not support optimistic
    # run_case 033 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_034_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

    run_sql_source1 "alter table ${shardddl1}.${tb1} add new_col1 int unique auto_increment;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,0);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add new_col1 int unique auto_increment;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(3,0);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(4,0);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(5);"
    run_sql_source2 "alter table ${shardddl1}.${tb2} add new_col1 int unique auto_increment;"
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "unsupported add column 'new_col1' constraint UNIQUE KEY when altering" 2
}

function DM_034() {
    run_case 034 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
    run_case 034 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_035_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

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
    run_sql_tidb_with_retry "select count(1) from ${shardddl}.${tb};" "count(1): 12"
}

function DM_035() {
    # currently not support pessimistic
    # run_case 035 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
    run_case 035 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_036_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'ccc');"

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
    run_sql_tidb_with_retry "select count(1) from ${shardddl}.${tb};" "count(1): 12"
}

function DM_036() {
    # currently not support pessimistic
    # run_case 036 "double-source-pessimistic" \
    # "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int, b varchar(10));\"; \
    #  run_sql_source2 \"create table ${shardddl1}.${tb1} (a int, b varchar(10));\"; \
    #  run_sql_source2 \"create table ${shardddl1}.${tb2} (a int, b varchar(10));\";" \
    # "clean_table" "pessimistic"

    run_case 036 "double-source-optimistic" \
    "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int, b varchar(10));\";" \
    "clean_table" "optimistic"
}

function DM_037_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

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

    if [[ "$1" = "pessimistic" ]]; then
        run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "detect inconsistent DDL sequence" 1
    else
        run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "fail to handle shard ddl .* in optimistic mode," 1
    fi
}

function DM_037() {
    run_case 037 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
    run_case 037 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_038_CASE() {
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"
    run_sql_source1 "alter table ${shardddl1}.${tb1} add column col1 datetime default now();"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add column col1 datetime default now();"
    run_sql_source2 "alter table ${shardddl1}.${tb2} add column col1 datetime default now();"
    run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values (1);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} (id) values (1);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} (id) values (1);"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 3 'fail'
}

function DM_038() {
    run_case 038 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
    run_case 038 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_039_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

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

function DM_039() {
    run_case 039 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
    run_case 039 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_040_CASE() {
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
    check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
}

function DM_040() {
    run_case 040 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
    run_case 040 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_041_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

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
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_041() {
    run_case 041 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
    run_case 041 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_043_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

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
    check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
}

function DM_043() {
    run_case 043 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
    run_case 043 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_046_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'ccc');"

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
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_046() {
    run_case 046 "double-source-pessimistic" \
    "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int, b varchar(10));\"" \
    "clean_table" "pessimistic"
    run_case 046 "double-source-optimistic" \
    "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int, b varchar(10));\"" \
    "clean_table" "optimistic"
}

function DM_047_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} (a,b) values(1,'aaa');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} (a,b) values(2,'bbb');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} (a,b) values(3,'ccc');"

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
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_047() {
    run_case 047 "double-source-pessimistic" \
    "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int, b varchar(10), c int as (a+1) stored);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int, b varchar(10), c int as (a+1) stored);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int, b varchar(10), c int as (a+1) stored);\"" \
    "clean_table" "pessimistic"
    run_case 047 "double-source-optimistic" \
    "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int, b varchar(10), c int as (a+1) stored);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int, b varchar(10), c int as (a+1) stored);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int, b varchar(10), c int as (a+1) stored);\"" \
    "clean_table" "optimistic"
}

function DM_048_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} (a,b) values(1,'aaa');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} (a,b) values(2,'bbb');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} (a,b) values(3,'ccc');"

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
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_048() {
    run_case 048 "double-source-pessimistic" \
    "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int, b varchar(10), c int as (a+1) virtual);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int, b varchar(10), c int as (a+1) virtual);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int, b varchar(10), c int as (a+1) virtual);\"" \
    "clean_table" "pessimistic"
    run_case 048 "double-source-optimistic" \
    "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int, b varchar(10), c int as (a+1) virtual);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int, b varchar(10), c int as (a+1) virtual);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int, b varchar(10), c int as (a+1) virtual);\"" \
    "clean_table" "optimistic"
}

function DM_049_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'ccc');"

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

    if [[ "$1" = "pessimistic" ]]; then
        check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
    else
        run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "because schema conflict detected" 2
    fi
}

function DM_049() {
    run_case 049 "double-source-pessimistic" \
    "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int, b varchar(10));\"" \
    "clean_table" "pessimistic"
    run_case 049 "double-source-optimistic" \
    "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int, b varchar(10));\"" \
    "clean_table" "optimistic"
}

function DM_050_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'ccc');"

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

    if [[ "$1" = "pessimistic" ]]; then
        check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
    else
        run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "because schema conflict detected" 2
    fi
}

function DM_050() {
    run_case 050 "double-source-pessimistic" \
    "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int, b varchar(10));\"" \
    "clean_table" "pessimistic"
    run_case 050 "double-source-optimistic" \
    "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int, b varchar(10));\"" \
    "clean_table" "optimistic"
}

function DM_051_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"

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

    if [[ "$1" = "pessimistic" ]]; then
        check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
    else
        run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "because schema conflict detected" 2
    fi
}

function DM_051() {
    run_case 051 "double-source-pessimistic" \
    "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int, b int);\"" \
    "clean_table" "pessimistic"
    run_case 051 "double-source-optimistic" \
    "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int, b int);\"" \
    "clean_table" "optimistic"
}

function DM_056_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"

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

    if [[ "$1" = "pessimistic" ]]; then
        check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
    else
        run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "because schema conflict detected" 2
    fi
}

function DM_056() {
    run_case 056 "double-source-pessimistic" \
    "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int, b int);\"" \
    "clean_table" "pessimistic"
    run_case 056 "double-source-optimistic" \
    "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int, b int);\"" \
    "clean_table" "optimistic"
}

function DM_057_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

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

function DM_057() {
    run_case 057 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
    # currently not support optimistic
    # run_case 057 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_058_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

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

    if [[ "$1" = "pessimistic" ]]; then
        check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
    else
        run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "because schema conflict detected" 2
    fi
}

function DM_058() {
    run_case 058 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
    run_case 058 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_059_CASE {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/593
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(now());"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(now());"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(now());"

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

function DM_059() {
    run_case 059 "double-source-pessimistic" \
    "run_sql_source1 \"create table ${shardddl1}.${tb1} (id datetime);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (id datetime);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (id datetime);\"" \
    "clean_table" "optimistic"
    run_case 059 "double-source-pessimistic" \
    "run_sql_source1 \"create table ${shardddl1}.${tb1} (id datetime);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (id datetime);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (id datetime);\"" \
    "clean_table" "optimistic"
}

function DM_062_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

    run_sql_source1 "alter table ${shardddl1}.${tb1} modify id bigint;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"
    run_sql_source2 "alter table ${shardddl1}.${tb1} modify id bigint;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(4);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(5);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(6);"
    run_sql_source2 "alter table ${shardddl1}.${tb2} modify id bigint;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(7);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(8);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(9);"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_062() {
    run_case 062 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
    run_case 062 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_063_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

    run_sql_source1 "alter table ${shardddl1}.${tb1} modify id int(15);"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"
    run_sql_source2 "alter table ${shardddl1}.${tb1} modify id int(20);"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(4);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(5);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(6);"
    run_sql_source2 "alter table ${shardddl1}.${tb2} modify id int(20);"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(7);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(8);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(9);"

    if [[ "$1" = "pessimistic" ]]; then
        check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
    else
        check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
    fi
}

function DM_063() {
    run_case 063 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
    run_case 063 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_064_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

    run_sql_source1 "alter table ${shardddl1}.${tb1} modify id int(30);"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"
    run_sql_source2 "alter table ${shardddl1}.${tb1} modify id int(30);"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(4);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(5);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(6);"
    run_sql_source2 "alter table ${shardddl1}.${tb2} modify id int(30);"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(7);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(8);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(9);"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_064() {
    run_case 064 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
    run_case 064 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_065_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"

    run_sql_source1 "alter table ${shardddl1}.${tb1} modify a bigint after b;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"
    run_sql_source2 "alter table ${shardddl1}.${tb1} modify a bigint first;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6);"
    run_sql_source2 "alter table ${shardddl1}.${tb2} modify a bigint first;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9);"

    if [[ "$1" = "pessimistic" ]]; then
        check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
    else
        run_sql_tidb_with_retry "select count(1) from ${shardddl}.${tb};" "count(1): 12"
    fi
}

function DM_065() {
    run_case 065 "double-source-pessimistic" \
    "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int, b int);\"" \
    "clean_table" "pessimistic"
    run_case 065 "double-source-optimistic" \
    "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int, b int);\"" \
    "clean_table" "optimistic"
}

function DM_066_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

    run_sql_source1 "alter table ${shardddl1}.${tb1} modify id int default 1;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"
    run_sql_source2 "alter table ${shardddl1}.${tb1} modify id int default 1;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(4);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(5);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(6);"
    run_sql_source2 "alter table ${shardddl1}.${tb2} modify id int default 1;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(7);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(8);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(9);"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_066() {
    run_case 066 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
    # currently not support optimistic
    # run_case 066 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_067_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

    run_sql_source1 "alter table ${shardddl1}.${tb1} modify id int default 1;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"
    run_sql_source2 "alter table ${shardddl1}.${tb1} modify id int default 2;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(4);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(5);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(6);"
    run_sql_source2 "alter table ${shardddl1}.${tb2} modify id int default 2;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(7);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(8);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(9);"

    if [[ "$1" = "pessimistic" ]]; then
        check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
    else
        run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "because schema conflict detected" 1
    fi
}

function DM_067() {
    run_case 067 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
    run_case 067 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}


function DM_068_CASE {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/593
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(now());"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(now());"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(now());"

    run_sql_source1 "alter table ${shardddl1}.${tb1} modify id datetime default now();"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(now());"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(now());"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(now());"
    run_sql_source2 "alter table ${shardddl1}.${tb1} modify id datetime default now();"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(now());"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(now());"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(now());"
    run_sql_source2 "alter table ${shardddl1}.${tb2} modify id datetime default now();"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(now());"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(now());"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(now());"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_068() {
    run_case 068 "double-source-pessimistic" \
    "run_sql_source1 \"create table ${shardddl1}.${tb1} (id datetime);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (id datetime);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (id datetime);\"" \
    "clean_table" "optimistic"
    run_case 068 "double-source-pessimistic" \
    "run_sql_source1 \"create table ${shardddl1}.${tb1} (id datetime);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (id datetime);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (id datetime);\"" \
    "clean_table" "optimistic"
}


function DM_076_CASE() {
    run_sql_source1 "alter table ${shardddl1}.${tb1} add primary key(id);"
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Unsupported add primary key" 1
}

function DM_076() {
    run_case 076 "single-source-pessimistic" "init_table 111" "clean_table" ""
}

function DM_077_CASE() {
    run_sql_source1 "alter table ${shardddl1}.${tb1} drop primary key;"
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Unsupported drop primary key when alter-primary-key is false" 1
}

function DM_077() {
    run_case 077 "single-source-pessimistic" "run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key);\"" "clean_table" ""
}

function DM_080_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(0,'a');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(-1,'b');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(-2,'c');"

    run_sql_source1 "alter table ${shardddl1}.${tb1} add unique key idx_a(a);"
    run_sql_source1 "alter table ${shardddl1}.${tb1} add unique key idx_b(b);"
    run_sql_source1 "alter table ${shardddl1}.${tb1} add unique key idx_ab(a,b);"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add unique key idx_a(a);"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add unique key idx_b(b);"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add unique key idx_ab(a,b);"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(2,'bbb');"
    run_sql_source2 "alter table ${shardddl1}.${tb2} add unique key idx_a(a);"
    run_sql_source2 "alter table ${shardddl1}.${tb2} add unique key idx_b(b);"
    run_sql_source2 "alter table ${shardddl1}.${tb2} add unique key idx_ab(a,b);"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(3,'ccc');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(3,'ccc');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'ccc');"
    run_sql_tidb_with_retry "select count(1) from ${shardddl}.${tb};" "count(1): 6"
}

function DM_080() {
    run_case 080 "double-source-pessimistic" \
    "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int, b varchar(10));\"" \
    "clean_table" "pessimistic"

    # currently not support optimistic
    #run_case 080 "double-source-optimistic" \
    #"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int, b varchar(10));\"; \
    # run_sql_source2 \"create table ${shardddl1}.${tb1} (a int, b varchar(10));\"; \
    # run_sql_source2 \"create table ${shardddl1}.${tb2} (a int, b varchar(10));\"" \
    #"clean_table" "optimistic"
}

function DM_081_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(0,'a');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(-1,'b');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(-2,'c');"

    run_sql_source1 "alter table ${shardddl1}.${tb1} add unique key idx_a(a);"
    run_sql_source1 "alter table ${shardddl1}.${tb1} add unique key idx_b(b);"
    run_sql_source1 "alter table ${shardddl1}.${tb1} add unique key idx_ab(a,b);"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add unique key idx_a(a);"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add unique key idx_b(b);"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add unique key idx_ab(a,b);"
    run_sql_source2 "alter table ${shardddl1}.${tb2} add unique key idx_a(a);"
    run_sql_source2 "alter table ${shardddl1}.${tb2} add unique key idx_b(b);"
    run_sql_source2 "alter table ${shardddl1}.${tb2} add unique key idx_ab(a,b);"

    run_sql_source1 "alter table ${shardddl1}.${tb1} drop index idx_a;"
    run_sql_source1 "alter table ${shardddl1}.${tb1} drop index idx_b;"
    run_sql_source1 "alter table ${shardddl1}.${tb1} drop index idx_ab;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
    run_sql_source2 "alter table ${shardddl1}.${tb1} drop index idx_a;"
    run_sql_source2 "alter table ${shardddl1}.${tb1} drop index idx_b;"
    run_sql_source2 "alter table ${shardddl1}.${tb1} drop index idx_ab;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(2,'bbb');"
    run_sql_source2 "alter table ${shardddl1}.${tb2} drop index idx_a;"
    run_sql_source2 "alter table ${shardddl1}.${tb2} drop index idx_b;"
    run_sql_source2 "alter table ${shardddl1}.${tb2} drop index idx_ab;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(3,'ccc');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(3,'ccc');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'ccc');"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_081() {
    run_case 081 "double-source-pessimistic" \
    "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int, b varchar(10));\"" \
    "clean_table" "pessimistic"

    # currently not support optimistic
    # run_case 081 "double-source-optimistic" \
    #"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int, b varchar(10));\"; \
    # run_sql_source2 \"create table ${shardddl1}.${tb1} (a int, b varchar(10));\"; \
    # run_sql_source2 \"create table ${shardddl1}.${tb2} (a int, b varchar(10));\"" \
    #"clean_table" "optimistic"
}


function DM_082_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(0,'a');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(-1,'b');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(-2,'c');"

    run_sql_source1 "alter table ${shardddl1}.${tb1} rename index a to c;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'ccc');"
    run_sql_source2 "alter table ${shardddl1}.${tb1} rename index a to c;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,'ddd');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,'eee');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,'fff');"
    run_sql_source2 "alter table ${shardddl1}.${tb2} rename index a to c;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,'ggg');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,'hhh');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,'iii');"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_082() {
    run_case 082 "double-source-pessimistic" \
    "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int unique key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int unique key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int unique key, b varchar(10));\"" \
    "clean_table" "pessimistic"

    # currently not support optimistic
    # run_case 082 "double-source-optimistic" \
    #"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int unique key, b varchar(10));\"; \
    # run_sql_source2 \"create table ${shardddl1}.${tb1} (a int unique key, b varchar(10));\"; \
    # run_sql_source2 \"create table ${shardddl1}.${tb2} (a int unique key, b varchar(10));\"" \
    #"clean_table" "optimistic"
}

function DM_094_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 int;"

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "stop-task test" \
        "\"result\": true" 3
    
    run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
    run_sql_source2 "alter table ${shardddl1}.${tb2} add column new_col1 int;"

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "start-task $cur/conf/double-source-$1.yaml" \
        "\"result\": true" 3
    
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_094() {
    run_case 094 "double-source-pessimistic" "init_table 111 112 211 212" "clean_table" "pessimistic"
    run_case 094 "double-source-optimistic" "init_table 111 112 211 212" "clean_table" "optimistic"
}

function DM_095_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 int;"

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "pause-task test" \
        "\"result\": true" 3
    
    run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
    run_sql_source2 "alter table ${shardddl1}.${tb2} add column new_col1 int;"

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "resume-task test" \
        "\"result\": true" 3
    
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_095() {
    run_case 095 "double-source-pessimistic" "init_table 111 112 211 212" "clean_table" "pessimistic"
    run_case 095 "double-source-optimistic" "init_table 111 112 211 212" "clean_table" "optimistic"
}

function DM_096_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 int;"

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "pause-task test" \
        "\"result\": true" 3
    
    run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
    run_sql_source2 "alter table ${shardddl1}.${tb2} add column new_col1 int;"

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "update-task $cur/conf/double-source-$1.yaml" \
        "\"result\": true" 1

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "resume-task test" \
        "\"result\": true" 3
    
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_096() {
    run_case 096 "double-source-pessimistic" "init_table 111 112 211 212" "clean_table" "pessimistic"
    run_case 096 "double-source-optimistic" "init_table 111 112 211 212" "clean_table" "optimistic"
}

function DM_097_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 int;"

    ps aux | grep dm-master |awk '{print $2}'|xargs kill || true
    check_port_offline $MASTER_PORT1 20
    
    run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
    run_sql_source2 "alter table ${shardddl1}.${tb2} add column new_col1 int;"

    run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT

    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_097() {
    run_case 097 "double-source-pessimistic" "init_table 111 112 211 212" "clean_table" "pessimistic"
    run_case 097 "double-source-optimistic" "init_table 111 112 211 212" "clean_table" "optimistic"
}

function DM_098_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"

    ps aux | grep dm-worker1 |awk '{print $2}'|xargs kill || true
    check_port_offline $WORKER1_PORT 20

    run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"

    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

    run_sql_source2 "alter table ${shardddl1}.${tb2} add column new_col1 int;"

    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_098() {
    run_case 098 "double-source-pessimistic" "init_table 111 112 211 212" "clean_table" "pessimistic"
}

function DM_099_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 int;"

    ps aux | grep dm-worker2 |awk '{print $2}'|xargs kill || true
    check_port_offline $WORKER2_PORT 20

    run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"

    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

    run_sql_source2 "alter table ${shardddl1}.${tb2} add column new_col1 int;"

    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_099() {
    run_case 099 "double-source-pessimistic" "init_table 111 112 211 212" "clean_table" "pessimistic"
}

function DM_100_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"

    ps aux | grep dm-worker1 |awk '{print $2}'|xargs kill || true
    check_port_offline $WORKER1_PORT 20

    run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"

    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

    run_sql_source2 "alter table ${shardddl1}.${tb2} add column new_col1 int;"

    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_100() {
    run_case 100 "double-source-optimistic" "init_table 111 112 211 212" "clean_table" "optimistic"
}

function DM_101_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 int;"

    ps aux | grep dm-worker2 |awk '{print $2}'|xargs kill || true
    check_port_offline $WORKER2_PORT 20

    run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"

    run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

    run_sql_source2 "alter table ${shardddl1}.${tb2} add column new_col1 int;"

    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_101() {
    run_case 101 "double-source-optimistic" "init_table 111 112 211 212" "clean_table" "optimistic"
}

function DM_102_CASE() {
    run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int default 0;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1);"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 int default -1;"
    
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "unlock-ddl-lock test-\`shardddl\`.\`tb\`" \
        "\"result\": true" 1

    run_sql_source2 "insert into ${shardddl1}.${tb1} values (2,2);"

    run_sql_tidb_with_retry "select count(1) from ${shardddl}.${tb};" "count(1): 2"
}

function DM_102() {
    run_case 102 "double-source-pessimistic" "init_table 111 211" "clean_table" ""
}

function DM_103_CASE() {
    # we should remove this two line after support feature https://github.com/pingcap/dm/issues/583
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'ccc');"

    run_sql_source1 "alter table ${shardddl1}.${tb1} add column c double;"
    run_sql_source1 "alter table ${shardddl1}.${tb1} drop column b;"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add column c double;"
    run_sql_source2 "alter table ${shardddl1}.${tb2} add column c double;"
    run_sql_source1 "alter table ${shardddl1}.${tb1} change a a bigint default 10;"
    run_sql_source2 "alter table ${shardddl1}.${tb1} drop column b;"
    run_sql_source2 "alter table ${shardddl1}.${tb1} change a a bigint default 10;"
    run_sql_source2 "alter table ${shardddl1}.${tb2} drop column b;"
    run_sql_source2 "alter table ${shardddl1}.${tb2} change a a bigint default 10;"

    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_103() {
    run_case 103 "double-source-pessimistic" \
    "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int, b varchar(10));\"" \
    "clean_table" "pessimistic"
    run_case 103 "double-source-optimistic" \
    "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int, b varchar(10));\"" \
    "clean_table" "optimistic"
}

function run() {
    init_cluster
    init_database
    except=(024 025 029 042 044 045 052 053 054 055 060 061 069 070 071 072 073 074 075 078 079 083 084 085 086 087 088 089 090 091 092 093)
    for i in $(seq -f "%03g" 1 103); do
        # we should remove this lines after fix memory leak of schemaTracker
        case="$i"
        if [[ ${case:2:1} -eq "5"  ]]; then
            cleanup_data $shardddl
            cleanup_process $*
            init_cluster
            init_database
        fi

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
