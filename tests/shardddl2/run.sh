#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
source $cur/../_utils/shardddl_lib.sh

function DM_036_CASE() {
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
    run_sql_tidb_with_retry "select count(1) from ${shardddl}.${tb};" "count(1): 9"
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
    run_sql_source1 "alter table ${shardddl1}.${tb1} add new_col1 int default 0;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
    run_sql_source2 "alter table ${shardddl1}.${tb1} add new_col1 int default -1;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(3,3);"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4);"
    run_sql_source2 "alter table ${shardddl1}.${tb2} add new_col1 int default -1;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(5,5);"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(6,6);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(7,7);"
    if [[ "$1" = "pessimistic" ]]; then
        check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
    else
        run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "because schema conflict detected" 1
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

    # TODO: remove sleep after we support detect ASAP in optimistic mode
    sleep 1
    run_sql_source1 "alter table ${shardddl1}.${tb1} add column col1 datetime default now();"
    run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values (1);"
    sleep 1
    run_sql_source2 "alter table ${shardddl1}.${tb1} add column col1 datetime default now();"
    run_sql_source2 "insert into ${shardddl1}.${tb1} (id) values (2);"
    sleep 1
    run_sql_source2 "alter table ${shardddl1}.${tb2} add column col1 datetime default now();"
    run_sql_source2 "insert into ${shardddl1}.${tb2} (id) values (3);"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 3 'fail'
}

function DM_038() {
    run_case 038 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
    run_case 038 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_039_CASE() {
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
    if [[ "$1" = "pessimistic" ]]; then
        check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
    else
        run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "because schema conflict detected" 1
    fi
}

function DM_040() {
    run_case 040 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
    run_case 040 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_041_CASE() {
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
    if [[ "$1" = "pessimistic" ]]; then
        check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
    else
        run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "because schema conflict detected" 1
    fi
}

function DM_043() {
    run_case 043 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
    run_case 043 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_046_CASE() {
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
    run_sql_source1 "alter table ${shardddl1}.${tb1} change a c int after b;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"
    run_sql_source2 "alter table ${shardddl1}.${tb1} change a c int first;"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6);"
    run_sql_source2 "alter table ${shardddl1}.${tb2} change a c int first;"
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
    run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values(1);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} (id) values(2);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} (id) values(3);"

    run_sql_source1 "alter table ${shardddl1}.${tb1} change a new_col datetime default now();"
    run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values(4);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} (id) values(5);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} (id) values(6);"
    sleep 1
    run_sql_source2 "alter table ${shardddl1}.${tb1} change a new_col datetime default now();"
    run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values(7);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} (id) values(8);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} (id) values(9);"
    sleep 1
    run_sql_source2 "alter table ${shardddl1}.${tb2} change a new_col datetime default now();"
    run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values(10);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} (id) values(11);"
    run_sql_source2 "insert into ${shardddl1}.${tb2} (id) values(12);"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_059() {
    run_case 059 "double-source-pessimistic" \
    "run_sql_source1 \"create table ${shardddl1}.${tb1} (id int, a datetime);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (id int, a datetime);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (id int, a datetime);\"" \
    "clean_table" "pessimistic"
    run_case 059 "double-source-pessimistic" \
    "run_sql_source1 \"create table ${shardddl1}.${tb1} (id int, a datetime);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (id int, a datetime);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (id int, a datetime);\"" \
    "clean_table" "optimistic"
}

function DM_062_CASE() {
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
    run_sql_source1 "alter table ${shardddl1}.${tb1} modify id mediumint;"
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

    if [[ "$1" = "pessimistic" ]]; then
        check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
    else
        # TODO: should detect schema conflict in optimistic mode
        run_sql_tidb_with_retry "select count(1) from ${shardddl}.${tb};" "count(1): 9"
    fi
}

function DM_063() {
    run_case 063 "double-source-pessimistic" \
    "run_sql_source1 \"create table ${shardddl1}.${tb1} (id smallint);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (id smallint);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (id smallint);\"" \
    "clean_table" "pessimistic"
    run_case 063 "double-source-optimistic" \
    "run_sql_source1 \"create table ${shardddl1}.${tb1} (id smallint);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (id smallint);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (id smallint);\"" \
     "clean_table" "optimistic"
}

function DM_064_CASE() {
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
        run_sql_tidb_with_retry "select count(1) from ${shardddl}.${tb};" "count(1): 9"
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
    run_case 066 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_067_CASE() {
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
    "clean_table" "pessimistic"
    run_case 068 "double-source-pessimistic" \
    "run_sql_source1 \"create table ${shardddl1}.${tb1} (id datetime);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (id datetime);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (id datetime);\"" \
    "clean_table" "optimistic"
}

function run() {
    init_cluster
    init_database
    start=36
    end=70
    except=(042 044 045 052 053 054 055 060 061 069 070)
    for i in $(seq -f "%03g" ${start} ${end}); do
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
