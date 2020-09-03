#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
source $cur/lib.sh

# skip modify column, two sources, no sharding
function DM_SKIP_ERROR_CASE() {
    run_sql_source1 "insert into ${db}.${tb1} values(1);"
    run_sql_source2 "insert into ${db}.${tb2} values(2);"
    run_sql_source1 "alter table ${db}.${tb1} add column new_col1 int;"
    run_sql_source2 "alter table ${db}.${tb2} add column new_col1 int;"
    run_sql_source1 "insert into ${db}.${tb1} values(3,3);"
    run_sql_source2 "insert into ${db}.${tb2} values(4,4);"

    # not support in TiDB
    run_sql_source1 "alter table ${db}.${tb1} modify id varchar(10);"
    run_sql_source2 "alter table ${db}.${tb2} modify id varchar(10);"
    run_sql_source1 "insert into ${db}.${tb1} values('aaa',5);"
    run_sql_source2 "insert into ${db}.${tb2} values('bbb',6);"

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Unsupported modify column" 2

    # begin to handle error
    # skip all sources
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip" \
            "\"result\": true" 3

    # insert fail
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Error .*: Incorrect int value" 2

    # skip one source
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip -s mysql-replica-01" \
            "\"result\": true" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "\"stage\": \"Running\"" 1 \
            "\"stage\": \"Paused\"" 1

    # skip all sources
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip" \
            "\"result\": true" 2 \
            "\"source 'mysql-replica-01' has no error\"" 1

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "\"stage\": \"Running\"" 2

    # '11' -> 11, '22' -> 22, no error
    run_sql_source1 "insert into ${db}.${tb1} values('111',7)"
    run_sql_source2 "insert into ${db}.${tb2} values('222',8)"
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "\"stage\": \"Running\"" 2

    run_sql_tidb_with_retry "select count(1) from ${db}.${tb1} where id=111;" "count(1): 1"
    run_sql_tidb_with_retry "select count(1) from ${db}.${tb2} where id=222;" "count(1): 1"
}

function DM_SKIP_ERROR() {
    run_case SKIP_ERROR "double-source-no-sharding" "init_table 11 22" "clean_table" ""
}

# skip modify column, two sources, 4 tables, sharding
# source1: tb1 first ddl -> tb1 second ddl -> tb2 first ddl -> tb2 second ddl 
# source2: tb1 first ddl -> tb2 first ddl -> tb1 second ddl -> tb2 second ddl 
function DM_SKIP_ERROR_SHARDING_CASE() {
    run_sql_source1 "insert into ${db}.${tb1} values(1);"
    run_sql_source1 "insert into ${db}.${tb2} values(2);"
    run_sql_source2 "insert into ${db}.${tb1} values(3);"
    run_sql_source2 "insert into ${db}.${tb2} values(4);"

    # 11/21 first ddl
    run_sql_source1 "alter table ${db}.${tb1} CHARACTER SET latin1 COLLATE latin1_danish_ci;"
    run_sql_source2 "alter table ${db}.${tb1} CHARACTER SET latin1 COLLATE latin1_danish_ci;"
    # 11 second ddl
    run_sql_source1 "alter table ${db}.${tb1} add column new_col1 varchar(10);"
    run_sql_source1 "insert into ${db}.${tb1} values(5,'aaa');"
    # 12/22 first ddl
    run_sql_source1 "alter table ${db}.${tb2} CHARACTER SET latin1 COLLATE latin1_danish_ci;"
    run_sql_source2 "alter table ${db}.${tb2} CHARACTER SET latin1 COLLATE latin1_danish_ci;"
    # 21 second ddl
    run_sql_source2 "alter table ${db}.${tb1} add column new_col1 varchar(10);"
    run_sql_source2 "insert into ${db}.${tb1} values(6,'bbb');"
    # 12/22 second ddl
    run_sql_source1 "alter table ${db}.${tb2} add column new_col1 varchar(10);"
    run_sql_source1 "insert into ${db}.${tb2} values(7,'ccc');"
    run_sql_source2 "alter table ${db}.${tb2} add column new_col1 varchar(10);"
    run_sql_source2 "insert into ${db}.${tb2} values(8,'ddd');"

    # begin to handle error
    # 11/21 first ddl: unsupport error
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Unsupported modify change collate from latin1_bin to latin1_danish_ci" 2

    # skip 11/21 first ddl
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip" \
            "\"result\": true" 3

    if [[ "$1" = "pessimistic" ]]; then
        # 11 second ddl bypass, 12 first ddl: detect conflict
        # 22 first ddl: unsupport error
        run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
                "query-status test" \
                "detect inconsistent DDL sequence from source" 1 \
                "Unsupported modify change collate from latin1_bin to latin1_danish_ci" 1
    else
        # 12/22 first ddl: unsupport error
        run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
                "query-status test" \
                "Unsupported modify change collate from latin1_bin to latin1_danish_ci" 2
    fi

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip -s mysql-replica-01,mysql-replica-02" \
            "\"result\": true" 3

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "\"stage\": \"Running\"" 2

    run_sql_tidb_with_retry "select count(1) from ${db}.${tb}" "count(1): 8"
}

function DM_SKIP_ERROR_SHARDING() {
    run_case SKIP_ERROR_SHARDING "double-source-pessimistic" "init_table 11 12 21 22" "clean_table" "pessimistic"
    run_case SKIP_ERROR_SHARDING "double-source-optimistic" "init_table 11 12 21 22" "clean_table" "optimistic"
}

# replace add column unique
# one source, one table, no sharding
function DM_REPLACE_ERROR_CASE() {
    run_sql_source1 "insert into ${db}.${tb1} values(1,1);"

    # error in TiDB
    run_sql_source1 "alter table ${db}.${tb1} add column c int unique;"
    run_sql_source1 "insert into ${db}.${tb1} values(2,2,2);"

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "unsupported add column .* constraint UNIQUE KEY" 1

    # replace sql
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test replace alter table ${db}.${tb1} add column c int;alter table ${db}.${tb1} add unique(c);" \
            "\"result\": true" 2

    run_sql_source1 "insert into ${db}.${tb1} values(3,3,3);"

    run_sql_tidb_with_retry "select count(1) from ${db}.${tb1};" "count(1): 3"
}

function DM_REPLACE_ERROR() {
    run_case REPLACE_ERROR "double-source-no-sharding" \
    "run_sql_source1 \"create table ${db}.${tb1} (a int unique, b int);\"" \
    "clean_table" ""
}

# replace add column unique twice
# two source, 4 tables
# source1: tb1 first ddl -> tb1 second ddl -> tb2 first ddl -> tb2 second ddl 
# source2: tb1 first ddl -> tb2 first ddl -> tb1 second ddl -> tb2 second ddl 
function DM_REPLACE_ERROR_SHARDING_CASE() {
    # 11/21 first ddl
    run_sql_source1 "alter table ${db}.${tb1} add column c int unique;"
    run_sql_source2 "alter table ${db}.${tb1} add column c int unique;"
    run_sql_source1 "insert into ${db}.${tb1} values(1,1,1);"
    run_sql_source2 "insert into ${db}.${tb1} values(2,2,2);"

    # 11 second ddl
    run_sql_source1 "alter table ${db}.${tb1} add column d int unique;"
    run_sql_source1 "insert into ${db}.${tb1} values(3,3,3,3);"

    # 12/22 first ddl
    run_sql_source1 "alter table ${db}.${tb2} add column c int unique;"
    run_sql_source2 "alter table ${db}.${tb2} add column c int unique;"
    run_sql_source1 "insert into ${db}.${tb2} values(4,4,4);"
    run_sql_source2 "insert into ${db}.${tb2} values(5,5,5);"

    # 21 second ddl
    run_sql_source2 "alter table ${db}.${tb1} add column d int unique;"
    run_sql_source2 "insert into ${db}.${tb1} values(6,6,6,6);"

    # 12/22 second ddl
    run_sql_source1 "alter table ${db}.${tb2} add column d int unique;"
    run_sql_source2 "alter table ${db}.${tb2} add column d int unique;"
    run_sql_source1 "insert into ${db}.${tb2} values(7,7,7,7);"
    run_sql_source2 "insert into ${db}.${tb2} values(8,8,8,8);"

    # 11/21 first ddl: unsupport error
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "unsupported add column .* constraint UNIQUE KEY" 2

    # begin to handle error
    # split 11/21 first ddl into two ddls
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test replace alter table ${db}.${tb1} add column c int;alter table ${db}.${tb1} add unique(c)" \
            "\"result\": true" 3

    if [[ "$1" = "pessimistic" ]]; then
        # 11 second ddl bypass, 12 first ddl detect conflict
        # 22 first ddl: detect conflict
        run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
                "query-status test" \
                "detect inconsistent DDL sequence from source" 2

        # split 12,22 first ddl into two ddls
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
                "handle-error test -s mysql-replica-01,mysql-replica-02 replace alter table ${db}.${tb2} add column c int;alter table ${db}.${tb2} add unique(c);" \
                "\"result\": true" 3

        # 11/21 second ddl: unsupport error
        run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
                "query-status test" \
                "unsupported add column .* constraint UNIQUE KEY" 2

        # split 11/21 second ddl into two ddls
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test replace alter table ${db}.${tb1} add column d int;alter table ${db}.${tb1} add unique(d);" \
            "\"result\": true" 3

        # 12/22 second ddl: detect conflict
        run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
                "query-status test" \
                "detect inconsistent DDL sequence from source" 2

        # split 11/21 second ddl into two ddls one by one
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test -s mysql-replica-01 replace alter table ${db}.${tb2} add column d int;alter table ${db}.${tb2} add unique(d);" \
            "\"result\": true" 2
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test -s mysql-replica-02 replace alter table ${db}.${tb2} add column d int;alter table ${db}.${tb2} add unique(d);" \
            "\"result\": true" 2
    else
        # 11 second ddl, 22 first ddl: unsupport error
        run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
                "query-status test" \
                "unsupported add column .* constraint UNIQUE KEY" 2

        # replace 11 second ddl
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
                "handle-error test -s mysql-replica-01 replace alter table ${db}.${tb1} add column d int;alter table ${db}.${tb1} add unique(d);" \
                "\"result\": true" 2

        # replace 22 first ddl
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
                "handle-error test -s mysql-replica-02 replace alter table ${db}.${tb2} add column c int;alter table ${db}.${tb2} add unique(c);" \
                "\"result\": true" 2

        # 12 first ddl, 21 second ddl: unsupport error
        run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
                "query-status test" \
                "unsupported add column .* constraint UNIQUE KEY" 2

        # replace 12 first ddl
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
                "handle-error test -s mysql-replica-01 replace alter table ${db}.${tb2} add column c int;alter table ${db}.${tb2} add unique(c);" \
                "\"result\": true" 2

        # replace 21 second ddl
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
                "handle-error test -s mysql-replica-02 replace alter table ${db}.${tb1} add column d int;alter table ${db}.${tb1} add unique(d);" \
                "\"result\": true" 2

        # 12 first ddl, 22 second ddl: unspport error
        run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
                "query-status test" \
                "unsupported add column .* constraint UNIQUE KEY" 2

        # replace 12/22 second ddl
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test replace alter table ${db}.${tb2} add column d int;alter table ${db}.${tb1} add unique(d);" \
            "\"result\": true" 3

    fi

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "\"stage\": \"Running\"" 2 \

    run_sql_tidb_with_retry "select count(1) from ${db}.${tb}" "count(1): 8"
}

function DM_REPLACE_ERROR_SHARDING() {
    run_case REPLACE_ERROR_SHARDING "double-source-pessimistic" \
    "run_sql_source1 \"create table ${db}.${tb1} (a int, b int);\"; \
     run_sql_source1 \"create table ${db}.${tb2} (a int, b int);\"; \
     run_sql_source2 \"create table ${db}.${tb1} (a int, b int);\"; \
     run_sql_source2 \"create table ${db}.${tb2} (a int, b int);\"" \
     "clean_table" "pessimistic"

    run_case REPLACE_ERROR_SHARDING "double-source-optimistic" \
    "run_sql_source1 \"create table ${db}.${tb1} (a int, b int);\"; \
     run_sql_source1 \"create table ${db}.${tb2} (a int, b int);\"; \
     run_sql_source2 \"create table ${db}.${tb1} (a int, b int);\"; \
     run_sql_source2 \"create table ${db}.${tb2} (a int, b int);\"" \
     "clean_table" "optimistic"
}

# test handle_error fail on second replace ddl
# two sources, two tables
function DM_REPLACE_ERROR_MULTIPLE_CASE() {
    run_sql_source1 "alter table ${db}.${tb1} add column a int unique, add column b int unique;"
    run_sql_source2 "alter table ${db}.${tb1} add column a int unique, add column b int unique;"
    run_sql_source1 "insert into ${db}.${tb1} values(1,1,1);"
    run_sql_source2 "insert into ${db}.${tb1} values(2,2,2);"

    # 11, 21 unspported error
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "unsupported add column 'a' constraint UNIQUE KEY" 2

    # begin to handle error
    # replace 11/21 ddl, wrong on second replace ddl
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test replace \"alter table ${db}.${tb1} add column a int; alter table ${db}.${tb1} add column b int unique;\"" \
            "\"result\": true" 3

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "unsupported add column 'b' constraint UNIQUE KEY" 2
        
    # now we change the second replace ddl, but first replace ddl will error because it has been executed in TiDB ...
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test replace \"alter table ${db}.${tb1} add column a int; alter table ${db}.${tb1} add column b int;\"" \
            "\"result\": true" 3

    # 11, 21 first replace ddl error
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Duplicate column name 'a'" 2

    # test handle-error revert
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test revert" \
            "\"result\": true" 3
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "unsupported add column 'a' constraint UNIQUE KEY" 2

    # now we only replace with ddl2
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test replace \"alter table ${db}.${tb1} add column b int;\"" \
            "\"result\": true" 3

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "\"stage\": \"Running\"" 2 \

    run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 2"
}

function DM_REPLACE_ERROR_MULTIPLE() {
    run_case REPLACE_ERROR_MULTIPLE "double-source-pessimistic" "init_table 11 21" "clean_table" "pessimistic"
    run_case REPLACE_ERROR_MULTIPLE "double-source-optimistic" "init_table 11 21" "clean_table" "optimistic"
}

function run() {
    init_cluster
    init_database
    DM_SKIP_ERROR
    DM_SKIP_ERROR_SHARDING
    DM_REPLACE_ERROR
    DM_REPLACE_ERROR_SHARDING
    DM_REPLACE_ERROR_MULTIPLE
}

cleanup_data $db
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
