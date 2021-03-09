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
            "only support to handle ddl error currently" 1

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "\"stage\": \"Paused\"" 2

    # skip all sources
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip" \
            "only support to handle ddl error currently" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "\"stage\": \"Paused\"" 2
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
            "\"stage\": \"Running\"" 4

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
    run_sql_source1 "alter table ${db}.${tb1} add column new_col text, add column c int unique;"
    run_sql_source1 "insert into ${db}.${tb1} values(2,2,'haha',2);"

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "unsupported add column .* constraint UNIQUE KEY" 1 \
            "origin SQL: \[alter table ${db}.${tb1} add column new_col text, add column c int unique\]" 1

    # replace sql
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test replace alter table ${db}.${tb1} add column new_col text, add column c int; alter table ${db}.${tb1} add unique(c);" \
            "\"result\": true" 2

    run_sql_source1 "insert into ${db}.${tb1} values(3,3,'hihi',3);"

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
            "\"stage\": \"Running\"" 4 \

    run_sql_tidb_with_retry "select count(1) from ${db}.${tb}" "count(1): 8"
}

function DM_REPLACE_ERROR_SHARDING() {
    run_case REPLACE_ERROR_SHARDING "double-source-pessimistic" \
    "run_sql_source1 \"create table ${db}.${tb1} (a int primary key, b int);\"; \
     run_sql_source1 \"create table ${db}.${tb2} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${db}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${db}.${tb2} (a int primary key, b int);\"" \
     "clean_table" "pessimistic"

    run_case REPLACE_ERROR_SHARDING "double-source-optimistic" \
    "run_sql_source1 \"create table ${db}.${tb1} (a int primary key, b int);\"; \
     run_sql_source1 \"create table ${db}.${tb2} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${db}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${db}.${tb2} (a int primary key, b int);\"" \
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
            "\"stage\": \"Running\"" 4 \

    run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 2"
}

function DM_REPLACE_ERROR_MULTIPLE() {
    run_case REPLACE_ERROR_MULTIPLE "double-source-pessimistic" "init_table 11 21" "clean_table" "pessimistic"
    run_case REPLACE_ERROR_MULTIPLE "double-source-optimistic" "init_table 11 21" "clean_table" "optimistic"
}

function DM_EXEC_ERROR_SKIP_CASE() {
    run_sql_source1 "insert into ${db}.${tb1} values(1,1,1);"
    run_sql_source2 "insert into ${db}.${tb1} values(2,2,2);"
    run_sql_tidb "insert into ${db}.${tb} values(3,1,1);"
    run_sql_tidb "insert into ${db}.${tb} values(4,2,2);"
    run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 4"

    run_sql_source1 "alter table ${db}.${tb1} add unique index ua(a);"
    run_sql_source2 "alter table ${db}.${tb1} add unique index ua(a);"

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Error 1062: Duplicate " 1

    run_sql_tidb "insert into ${db}.${tb} values(5,3,3);"
    run_sql_tidb "insert into ${db}.${tb} values(6,4,4);"
    
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip" \
            "\"result\": true" 2

    run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 6"
}

function DM_EXEC_ERROR_SKIP() {
     run_case EXEC_ERROR_SKIP "double-source-pessimistic" \
     "run_sql_source1 \"create table ${db}.${tb1} (id int primary key, a int, b int);\"; \
      run_sql_source2 \"create table ${db}.${tb1} (id int primary key, a int, b int);\"" \
     "clean_table" "pessimistic"
     run_case EXEC_ERROR_SKIP "double-source-optimistic" \
     "run_sql_source1 \"create table ${db}.${tb1} (id int primary key, a int, b int);\"; \
      run_sql_source2 \"create table ${db}.${tb1} (id int primary key, a int, b int);\"" \
     "clean_table" "optimistic"
}

function DM_4202_CASE() {
    run_sql_source1 "insert into ${db}.${tb1} values(1);"
    run_sql_source1 "alter table ${db}.${tb1} modify id varchar(10);"

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Unsupported modify column" 1

    start_location=$(get_start_location 127.0.0.1:$MASTER_PORT $source1)
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip -b $start_location" \
            "\"result\": true" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "\"result\": true" 2

    run_sql_source1 "insert into ${db}.${tb1} values(2);"
    run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 2"
}

function DM_4202() {
     run_case 4202 "single-source-no-sharding" "init_table 11" "clean_table" ""
}

function DM_4204_CASE() {
    run_sql_source1 "insert into ${db}.${tb1} values(1);"
    run_sql_source1 "alter table ${db}.${tb1} modify id varchar(10);"

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Unsupported modify column" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip" \
            "\"result\": true" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "\"result\": true" 2

    run_sql_source1 "insert into ${db}.${tb1} values(2);"
    run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 2"
}

function DM_4204() {
     run_case 4204 "single-source-no-sharding" "init_table 11" "clean_table" ""
}

# 4206, 4208
function DM_4206_CASE() {
    run_sql_source1 "insert into ${db}.${tb1} values(1);"
    run_sql_source2 "insert into ${db}.${tb1} values(2);"
    run_sql_source1 "alter table ${db}.${tb1} modify id varchar(10);"
    run_sql_source2 "alter table ${db}.${tb1} modify id varchar(10);"
    run_sql_source1 "alter table ${db}.${tb1} modify id varchar(20);"
    run_sql_source2 "alter table ${db}.${tb1} modify id varchar(20);"

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Unsupported modify column.*varchar(10)" 2

    first_pos1=$(get_start_pos 127.0.0.1:$MASTER_PORT $source1)
    first_pos2=$(get_start_pos 127.0.0.1:$MASTER_PORT $source2)
    first_name1=$(get_start_name 127.0.0.1:$MASTER_PORT $source1)
    first_name2=$(get_start_name 127.0.0.1:$MASTER_PORT $source2)

    second_pos1=$(get_next_query_pos $MYSQL_PORT1 $MYSQL_PASSWORD1 $first_pos1)
    second_pos2=$(get_next_query_pos $MYSQL_PORT2 $MYSQL_PASSWORD2 $first_pos2)

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip -b $first_name1:$second_pos1 -s $source1" \
            "\"result\": true" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Unsupported modify column.*varchar(10)" 2

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip -b $first_name1:$first_pos1 -s $source1" \
            "\"result\": true" 2

    run_sql_source1 "insert into ${db}.${tb1} values(3);"

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Unsupported modify column.*varchar(10)" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip -b $first_name2:$first_pos2 -s $source2" \
            "\"result\": true" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Unsupported modify column.*varchar(20)" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip -b $first_name2:$second_pos2 -s $source2" \
            "\"result\": true" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "\"result\": true" 3

    run_sql_source2 "insert into ${db}.${tb1} values(4);"

    run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 4"
}

function DM_4206() {
     run_case 4206 "double-source-pessimistic" "init_table 11 21" "clean_table" "pessimistic"
     run_case 4206 "double-source-optimistic" "init_table 11 21" "clean_table" "optimistic"
}

function DM_4207_CASE() {
    run_sql_source1 "insert into ${db}.${tb1} values(1);"
    run_sql_source2 "insert into ${db}.${tb1} values(2);"
    run_sql_source1 "alter table ${db}.${tb1} modify id varchar(10);"
    run_sql_source2 "alter table ${db}.${tb1} modify id varchar(10);"

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Unsupported modify column" 2

    start_location1=$(get_start_location 127.0.0.1:$MASTER_PORT $source1)
    start_location2=$(get_start_location 127.0.0.1:$MASTER_PORT $source2)

    if [ "$start_location1" = "$start_location2" ]; then
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip -b $start_location1" \
            "\"result\": true" 3
    else
        # WARN: may skip unknown event like later insert, test will fail
        # It hasn't happened yet.
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip -b $start_location1" \
            "\"result\": true" 3

        run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Unsupported modify column" 1

        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip -b $start_location2" \
            "\"result\": true" 3
    fi

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "\"result\": true" 3

    run_sql_source1 "insert into ${db}.${tb1} values(3);"
    run_sql_source2 "insert into ${db}.${tb1} values(4);"

    run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where id<100;" "count(1): 4"
}

function DM_4207() {
     run_case 4207 "double-source-pessimistic" "init_table 11 21" "clean_table" "pessimistic"
     run_case 4207 "double-source-optimistic" "init_table 11 21" "clean_table" "optimistic"
        
     # test different error locations
     run_case 4207 "double-source-pessimistic" "init_table 11 21;run_sql_source1 \"insert into ${db}.${tb1} values(100);\"" "clean_table" "pessimistic"
     run_case 4207 "double-source-optimistic" "init_table 11 21;run_sql_source1 \"insert into ${db}.${tb1} values(100);\"" "clean_table" "optimistic"
}

function DM_4209_CASE() {
    run_sql_source1 "insert into ${db}.${tb1} values(1);"
    run_sql_source2 "insert into ${db}.${tb1} values(2);"
    run_sql_source1 "alter table ${db}.${tb1} modify id varchar(10);"
    run_sql_source2 "alter table ${db}.${tb1} modify id varchar(10);"

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Unsupported modify column" 2

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip -s $source1" \
            "\"result\": true" 2

    run_sql_source1 "insert into ${db}.${tb1} values(3);"

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Unsupported modify column" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip -s $source2" \
            "\"result\": true" 2

    run_sql_source2 "insert into ${db}.${tb1} values(4);"

    run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 4"
}

function DM_4209() {
     run_case 4209 "double-source-pessimistic" "init_table 11 21" "clean_table" "pessimistic"
     run_case 4209 "double-source-optimistic" "init_table 11 21" "clean_table" "optimistic"
}

function DM_4211_CASE() {
    run_sql_source1 "insert into ${db}.${tb1} values(1);"
    run_sql_source1 "alter table ${db}.${tb1} add column c int unique;"

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "unsupported add column .* constraint UNIQUE KEY" 1

    run_sql_source1 "insert into ${db}.${tb1} values(2,2);"
        
    start_location=$(get_start_location 127.0.0.1:$MASTER_PORT $source1)
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test --binlog-pos $start_location replace alter table ${db}.${tb1} add column c int;" \
            "\"result\": true" 2


    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "\"result\": true" 2

    run_sql_source1 "insert into ${db}.${tb1} values(3,3);"

    run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 3"
}

function DM_4211() {
     run_case 4211 "single-source-no-sharding" "init_table 11" "clean_table" ""
}

function DM_4213_CASE() {
    run_sql_source1 "insert into ${db}.${tb1} values(1);"
    run_sql_source1 "alter table ${db}.${tb1} add column c int unique;"

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "unsupported add column .* constraint UNIQUE KEY" 1

    run_sql_source1 "insert into ${db}.${tb1} values(2,2);"
        
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test replace alter table ${db}.${tb1} add column c int;alter table ${db}.${tb1} add unique(c);" \
            "\"result\": true" 2


    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "\"result\": true" 2

    run_sql_source1 "insert into ${db}.${tb1} values(3,3);"

    run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 3"
}

function DM_4213() {
     run_case 4213 "single-source-no-sharding" "init_table 11" "clean_table" ""
}

# 4215, 4217
function DM_4215_CASE() {
    run_sql_source1 "insert into ${db}.${tb1} values(1);"
    run_sql_source2 "insert into ${db}.${tb1} values(2);"
    run_sql_source1 "alter table ${db}.${tb1} add column c int unique;"
    run_sql_source2 "alter table ${db}.${tb1} add column c int unique;"
    run_sql_source1 "alter table ${db}.${tb1} add column d int unique;"
    run_sql_source2 "alter table ${db}.${tb1} add column d int unique;"

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "unsupported add column 'c' constraint UNIQUE KEY" 2

    first_pos1=$(get_start_pos 127.0.0.1:$MASTER_PORT $source1)
    first_pos2=$(get_start_pos 127.0.0.1:$MASTER_PORT $source2)
    first_name1=$(get_start_name 127.0.0.1:$MASTER_PORT $source1)
    first_name2=$(get_start_name 127.0.0.1:$MASTER_PORT $source2)

    second_pos1=$(get_next_query_pos $MYSQL_PORT1 $MYSQL_PASSWORD1 $first_pos1)
    second_pos2=$(get_next_query_pos $MYSQL_PORT2 $MYSQL_PASSWORD2 $first_pos2)

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test -s $source1 -b $first_name1:$second_pos1 replace alter table ${db}.${tb1} add column d int;alter table ${db}.${tb1} add unique(d);" \
            "\"result\": true" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "unsupported add column 'c' constraint UNIQUE KEY" 2

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test -s $source1 -b $first_name1:$first_pos1 replace alter table ${db}.${tb1} add column c int;alter table ${db}.${tb1} add unique(c);" \
            "\"result\": true" 2

    run_sql_source1 "insert into ${db}.${tb1} values(3,3,3);"

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "unsupported add column 'c' constraint UNIQUE KEY" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test -s $source2 -b $first_name2:$first_pos2 replace alter table ${db}.${tb1} add column c int;alter table ${db}.${tb1} add unique(c);" \
            "\"result\": true" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "unsupported add column 'd' constraint UNIQUE KEY" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test -s $source2 -b $first_name2:$second_pos2 replace alter table ${db}.${tb1} add column d int;alter table ${db}.${tb1} add unique(d);" \
            "\"result\": true" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "\"result\": true" 3

    run_sql_source2 "insert into ${db}.${tb1} values(4,4,4);"

    run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 4"
}

function DM_4215() {
     run_case 4215 "double-source-pessimistic" "init_table 11 21" "clean_table" "pessimistic"
     run_case 4215 "double-source-optimistic" "init_table 11 21" "clean_table" "optimistic"
}

function DM_4216_CASE() {
    run_sql_source1 "insert into ${db}.${tb1} values(1);"
    run_sql_source2 "insert into ${db}.${tb1} values(2);"
    run_sql_source1 "alter table ${db}.${tb1} add column c int unique;"
    run_sql_source2 "alter table ${db}.${tb1} add column c int unique;"

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "unsupported add column .* constraint UNIQUE KEY" 2

    start_location1=$(get_start_location 127.0.0.1:$MASTER_PORT $source1)
    start_location2=$(get_start_location 127.0.0.1:$MASTER_PORT $source2)

    if [ "$start_location1" = "$start_location2" ]; then
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test -b $start_location1 replace alter table ${db}.${tb1} add column c int;alter table ${db}.${tb1} add unique(c);" \
            "\"result\": true" 3
    else
        # WARN: may replace unknown event like later insert, test will fail
        # It hasn't happened yet.
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test -b $start_location1 replace alter table ${db}.${tb1} add column c int;alter table ${db}.${tb1} add unique(c);" \
            "\"result\": true" 3

        run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "unsupported add column .* constraint UNIQUE KEY" 1

        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test -b $start_location2 replace alter table ${db}.${tb1} add column c int;alter table ${db}.${tb1} add unique(c);" \
            "\"result\": true" 3
    fi

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "\"result\": true" 3

    run_sql_source1 "insert into ${db}.${tb1} values(3,3);"
    run_sql_source2 "insert into ${db}.${tb1} values(4,4);"

    run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where id<100;" "count(1): 4"
}

function DM_4216() {
     run_case 4216 "double-source-pessimistic" "init_table 11 21" "clean_table" "pessimistic"
     run_case 4216 "double-source-optimistic" "init_table 11 21" "clean_table" "optimistic"
        
     # test different error locations
     run_case 4216 "double-source-pessimistic" "init_table 11 21;run_sql_source1 \"insert into ${db}.${tb1} values(100);\"" "clean_table" "pessimistic"
     run_case 4216 "double-source-optimistic" "init_table 11 21;run_sql_source1 \"insert into ${db}.${tb1} values(100);\"" "clean_table" "optimistic"
}

function DM_4219_CASE() {
    run_sql_source1 "insert into ${db}.${tb1} values(1);"
    run_sql_source2 "insert into ${db}.${tb1} values(2);"
    run_sql_source1 "alter table ${db}.${tb1} add column c int unique;"
    run_sql_source2 "alter table ${db}.${tb1} add column c int unique;"

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "unsupported add column .* constraint UNIQUE KEY" 2

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test -s $source1 replace alter table ${db}.${tb1} add column c int;alter table ${db}.${tb1} add unique(c);" \
            "\"result\": true" 2

    run_sql_source1 "insert into ${db}.${tb1} values(3,3);"

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "unsupported add column .* constraint UNIQUE KEY" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test -s $source2 replace alter table ${db}.${tb1} add column c int;alter table ${db}.${tb1} add unique(c);" \
            "\"result\": true" 2

    run_sql_source2 "insert into ${db}.${tb1} values(4,4);"

    run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 4"
}

function DM_4219() {
     run_case 4219 "double-source-pessimistic" "init_table 11 21" "clean_table" "pessimistic"
     run_case 4219 "double-source-optimistic" "init_table 11 21" "clean_table" "optimistic"
}

# 4220, 4224, 4226, 4229, 4194, 4195, 4196
function DM_4220_CASE() {
    run_sql_source1 "insert into ${db}.${tb1} values(1);"
    run_sql_source2 "insert into ${db}.${tb1} values(2);"
    run_sql_source1 "alter table ${db}.${tb1} modify id varchar(10);"
    run_sql_source1 "alter table ${db}.${tb1} modify id varchar(20);"
    run_sql_source1 "alter table ${db}.${tb1} modify id varchar(30);"
    run_sql_source2 "alter table ${db}.${tb1} modify id varchar(10);"
    run_sql_source2 "alter table ${db}.${tb1} modify id varchar(20);"
    run_sql_source2 "alter table ${db}.${tb1} modify id varchar(30);"

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Unsupported modify column.*varchar(10)" 2

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test revert" \
            "operator not exist" 2

    first_pos1=$(get_start_pos 127.0.0.1:$MASTER_PORT $source1)
    first_pos2=$(get_start_pos 127.0.0.1:$MASTER_PORT $source2)
    first_name1=$(get_start_name 127.0.0.1:$MASTER_PORT $source1)
    first_name2=$(get_start_name 127.0.0.1:$MASTER_PORT $source2)

    second_pos1=$(get_next_query_pos $MYSQL_PORT1 $MYSQL_PASSWORD1 $first_pos1)
    second_pos2=$(get_next_query_pos $MYSQL_PORT2 $MYSQL_PASSWORD2 $first_pos2)
    third_pos1=$(get_next_query_pos $MYSQL_PORT1 $MYSQL_PASSWORD1 $second_pos1)
    third_pos2=$(get_next_query_pos $MYSQL_PORT2 $MYSQL_PASSWORD2 $second_pos2)

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip -s $source1 -b $first_name1:$second_pos1" \
            "\"result\": true" 2
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip -s $source2 -b $first_name2:$second_pos2" \
            "\"result\": true" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Unsupported modify column.*varchar(10)" 2

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test revert" \
            "operator not exist" 2

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip -s $source1 -b $first_name1:$third_pos1" \
            "\"result\": true" 2
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip -s $source2 -b $first_name2:$third_pos2" \
            "\"result\": true" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Unsupported modify column.*varchar(10)" 2

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test revert -s $source1 -b $first_name1:$third_pos1" \
            "\"result\": true" 2
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test revert -s $source2 -b $first_name2:$third_pos2" \
            "\"result\": true" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Unsupported modify column.*varchar(10)" 2
    
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip" \
            "\"result\": true" 3

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Unsupported modify column.*varchar(30)" 2

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip" \
            "\"result\": true" 3

    # flush checkpoint, otherwise revert may "succeed"
    run_sql_source1 "alter table ${db}.${tb1} add column new_col int;"
    run_sql_source2 "alter table ${db}.${tb1} add column new_col int;"

    run_sql_source1 "insert into ${db}.${tb1} values(3,3);"
    run_sql_source2 "insert into ${db}.${tb1} values(4,4);"
    run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 4"

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test revert -s $source1 -b $first_name1:$third_pos1" \
            "operator not exist" 1
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test revert -s $source2 -b $first_name2:$third_pos2" \
            "operator not exist" 1
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test revert -b $first_name2:$third_pos2" \
            "operator not exist" 2

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test revert" \
            "source.*has no error" 2

    run_sql_source1 "insert into ${db}.${tb1} values(5,5);"
    run_sql_source2 "insert into ${db}.${tb1} values(6,6);"

    run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 6"
}

function DM_4220() {
     run_case 4220 "double-source-pessimistic" "init_table 11 21" "clean_table" "pessimistic"
     run_case 4220 "double-source-optimistic" "init_table 11 21" "clean_table" "optimistic"
}

# 4185, 4187, 4186
function DM_4185_CASE() {
    run_sql_source1 "insert into ${db}.${tb1} values(1);"
    run_sql_source2 "insert into ${db}.${tb1} values(2);"
    run_sql_source1 "alter table ${db}.${tb1} modify id varchar(10);"
    run_sql_source2 "alter table ${db}.${tb1} modify id varchar(10);"
    run_sql_source1 "alter table ${db}.${tb1} modify id varchar(20);"
    run_sql_source2 "alter table ${db}.${tb1} modify id varchar(20);"

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Unsupported modify column.*varchar(10)" 2

    first_pos1=$(get_start_pos 127.0.0.1:$MASTER_PORT $source1)
    first_pos2=$(get_start_pos 127.0.0.1:$MASTER_PORT $source2)
    first_name1=$(get_start_name 127.0.0.1:$MASTER_PORT $source1)
    first_name2=$(get_start_name 127.0.0.1:$MASTER_PORT $source2)
    second_pos1=$(get_next_query_pos $MYSQL_PORT1 $MYSQL_PASSWORD1 $first_pos1)
    second_pos2=$(get_next_query_pos $MYSQL_PORT2 $MYSQL_PASSWORD2 $first_pos2)

    if [ "$second_pos1" = "$second_pos2" ]; then
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip -b $first_name1:$second_pos1" \
            "\"result\": true" 3
    else
        # WARN: may skip unknown event like later insert, test will fail
        # It hasn't happened yet.
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip -b $first_name1:$second_pos1" \
            "\"result\": true" 3

        run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Unsupported modify column.*varchar(10)" 2

        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip -b $first_name1:$second_pos2" \
            "\"result\": true" 3
    fi

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Unsupported modify column.*varchar(10)" 2

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip" \
            "\"result\": true" 3

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "\"result\": true" 3

    run_sql_source1 "insert into ${db}.${tb1} values(3);"
    run_sql_source2 "insert into ${db}.${tb1} values(4);"

    run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where id<100;" "count(1): 4"
}

function DM_4185() {
     run_case 4185 "double-source-pessimistic" "init_table 11 21" "clean_table" "pessimistic"
     run_case 4185 "double-source-optimistic" "init_table 11 21" "clean_table" "optimistic"
        
     # test different error locations
     run_case 4185 "double-source-pessimistic" "init_table 11 21;run_sql_source1 \"insert into ${db}.${tb1} values(100);\"" "clean_table" "pessimistic"
     run_case 4185 "double-source-optimistic" "init_table 11 21;run_sql_source1 \"insert into ${db}.${tb1} values(100);\"" "clean_table" "optimistic"
}

# 4201, 4203, 4205
function DM_4201_CASE() {
    run_sql_source1 "insert into ${db}.${tb1} values(1);"
    run_sql_source1 "alter table ${db}.${tb1} modify id varchar(10);"
    run_sql_source1 "alter table ${db}.${tb1} modify id varchar(20);"

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Unsupported modify column.*varchar(10)" 1

    first_pos1=$(get_start_pos 127.0.0.1:$MASTER_PORT $source1)
    first_name1=$(get_start_name 127.0.0.1:$MASTER_PORT $source1)
    second_pos1=$(get_next_query_pos $MYSQL_PORT1 $MYSQL_PASSWORD1 $first_pos1)

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip -b $first_name1:$second_pos1" \
            "\"result\": true" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Unsupported modify column.*varchar(10)" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip" \
            "\"result\": true" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "\"result\": true" 2

    run_sql_source1 "insert into ${db}.${tb1} values(2);"

    run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where id<100;" "count(1): 2"
}

function DM_4201() {
     run_case 4201 "single-source-no-sharding" "init_table 11" "clean_table" ""
}

# 4189, 4190, 4191, 4192, 4214, 4218
function DM_4189_CASE() {
    run_sql_source1 "insert into ${db}.${tb1} values(1);"
    run_sql_source2 "insert into ${db}.${tb1} values(2);"
    run_sql_source1 "alter table ${db}.${tb1} add column c int unique;"
    run_sql_source2 "alter table ${db}.${tb1} add column c int unique;"

    run_sql_source1 "insert into ${db}.${tb1} values(3,3);"
    run_sql_source2 "insert into ${db}.${tb1} values(4,4);"

    run_sql_source1 "alter table ${db}.${tb1} add column d int unique;"
    run_sql_source2 "alter table ${db}.${tb1} add column d int unique;"

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "unsupported add column 'c' constraint UNIQUE KEY" 2

    first_pos1=$(get_start_pos 127.0.0.1:$MASTER_PORT $source1)
    first_pos2=$(get_start_pos 127.0.0.1:$MASTER_PORT $source2)
    first_name1=$(get_start_name 127.0.0.1:$MASTER_PORT $source1)
    first_name2=$(get_start_name 127.0.0.1:$MASTER_PORT $source2)
    second_pos1=$(get_next_query_pos $MYSQL_PORT1 $MYSQL_PASSWORD1 $first_pos1)
    second_pos2=$(get_next_query_pos $MYSQL_PORT2 $MYSQL_PASSWORD2 $first_pos2)

    if [ "$second_pos1" = "$second_pos2" ]; then
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test -b $first_name1:$second_pos1 replace alter table ${db}.${tb1} add column d int;alter table ${db}.${tb1} add unique(d);" \
            "\"result\": true" 3
    else
        # WARN: may replace unknown event like later insert, test will fail
        # It hasn't happened yet.
        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test -b $first_name1:$second_pos1 replace alter table ${db}.${tb1} add column d int;alter table ${db}.${tb1} add unique(d);" \
            "\"result\": true" 3

        run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "unsupported add column 'c' constraint UNIQUE KEY" 2

        run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test -b $first_name2:$second_pos2 replace alter table ${db}.${tb1} add column d int;alter table ${db}.${tb1} add unique(d);" \
            "\"result\": true" 3
    fi

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "unsupported add column 'c' constraint UNIQUE KEY" 2

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test replace alter table ${db}.${tb1} add column c int;alter table ${db}.${tb1} add unique(c);" \
            "\"result\": true" 3

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "\"result\": true" 3

    run_sql_source1 "insert into ${db}.${tb1} values(5,5,5);"
    run_sql_source2 "insert into ${db}.${tb1} values(6,6,6);"

    run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where id<100;" "count(1): 6"
}

function DM_4189() {
     run_case 4189 "double-source-pessimistic" "init_table 11 21" "clean_table" "pessimistic"
     run_case 4189 "double-source-optimistic" "init_table 11 21" "clean_table" "optimistic"
        
     # test different error locations
     run_case 4189 "double-source-pessimistic" "init_table 11 21;run_sql_source1 \"insert into ${db}.${tb1} values(100);\"" "clean_table" "pessimistic"
     run_case 4189 "double-source-optimistic" "init_table 11 21;run_sql_source1 \"insert into ${db}.${tb1} values(100);\"" "clean_table" "optimistic"
}

# 4210, 4212
function DM_4210_CASE() {
    run_sql_source1 "insert into ${db}.${tb1} values(1);"
    run_sql_source1 "alter table ${db}.${tb1} add column c int unique;"
    run_sql_source1 "insert into ${db}.${tb1} values(2,2);"
    run_sql_source1 "alter table ${db}.${tb1} add column d int unique;"

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "unsupported add column 'c' constraint UNIQUE KEY" 1

    first_pos1=$(get_start_pos 127.0.0.1:$MASTER_PORT $source1)
    first_name1=$(get_start_name 127.0.0.1:$MASTER_PORT $source1)
    second_pos1=$(get_next_query_pos $MYSQL_PORT1 $MYSQL_PASSWORD1 $first_pos1)

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test -b $first_name1:$second_pos1 replace alter table ${db}.${tb1} add column d int;alter table ${db}.${tb1} add unique(d);" \
            "\"result\": true" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "unsupported add column 'c' constraint UNIQUE KEY" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test replace alter table ${db}.${tb1} add column e int unique;" \
            "\"result\": true" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "unsupported add column 'e' constraint UNIQUE KEY" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test replace alter table ${db}.${tb1} add column c int;alter table ${db}.${tb1} add unique(c);" \
            "\"result\": true" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "\"result\": true" 2

    run_sql_source1 "insert into ${db}.${tb1} values(3,3,3);"

    run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where id<100;" "count(1): 3"
}

function DM_4210() {
     run_case 4210 "single-source-no-sharding" "init_table 11" "clean_table" ""
}

# 4193, 4221, 4225, 4227, 4228
function DM_4193_CASE() {
    run_sql_source1 "insert into ${db}.${tb1} values(1);"
    run_sql_source2 "insert into ${db}.${tb1} values(2);"

    run_sql_source1 "alter table ${db}.${tb1} modify id varchar(10);"
    run_sql_source1 "alter table ${db}.${tb1} add column c int;"
    run_sql_source1 "alter table ${db}.${tb1} modify id varchar(20);"
    run_sql_source1 "alter table ${db}.${tb1} add column d int;"
    run_sql_source1 "alter table ${db}.${tb1} modify id varchar(30);"

    run_sql_source2 "alter table ${db}.${tb1} modify id varchar(10);"
    run_sql_source2 "alter table ${db}.${tb1} add column c int;"
    run_sql_source2 "alter table ${db}.${tb1} modify id varchar(20);"
    run_sql_source2 "alter table ${db}.${tb1} add column d int;"
    run_sql_source2 "alter table ${db}.${tb1} modify id varchar(30);"

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Unsupported modify column.*varchar(10)" 2

    first_pos1=$(get_start_pos 127.0.0.1:$MASTER_PORT $source1)
    first_pos2=$(get_start_pos 127.0.0.1:$MASTER_PORT $source2)
    first_name1=$(get_start_name 127.0.0.1:$MASTER_PORT $source1)
    first_name2=$(get_start_name 127.0.0.1:$MASTER_PORT $source2)

    temp_pos1=$(get_next_query_pos $MYSQL_PORT1 $MYSQL_PASSWORD1 $first_pos1)
    temp_pos2=$(get_next_query_pos $MYSQL_PORT2 $MYSQL_PASSWORD2 $first_pos2)
    second_pos1=$(get_next_query_pos $MYSQL_PORT1 $MYSQL_PASSWORD1 $temp_pos1)
    second_pos2=$(get_next_query_pos $MYSQL_PORT2 $MYSQL_PASSWORD2 $temp_pos2)
    temp_pos1=$(get_next_query_pos $MYSQL_PORT1 $MYSQL_PASSWORD1 $second_pos1)
    temp_pos2=$(get_next_query_pos $MYSQL_PORT2 $MYSQL_PASSWORD2 $second_pos2)
    third_pos1=$(get_next_query_pos $MYSQL_PORT1 $MYSQL_PASSWORD1 $temp_pos1)
    third_pos2=$(get_next_query_pos $MYSQL_PORT2 $MYSQL_PASSWORD2 $temp_pos2)

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip -s $source1 -b $first_name1:$first_pos1" \
            "\"result\": true" 2
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip -s $source2 -b $first_name2:$first_pos2" \
            "\"result\": true" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Unsupported modify column.*varchar(20)" 2

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test revert -s $source1 -b $first_name1:$first_pos1" \
            "operator not exist" 1
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test revert -s $source2 -b $first_name2:$first_pos2" \
            "operator not exist" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip -s $source1 -b $first_name1:$third_pos1" \
            "\"result\": true" 2

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test revert -b $first_name1:$third_pos1" \
            "operator not exist" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip -s $source1 -b $first_name1:$third_pos1" \
            "\"result\": true" 2

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test revert -s $source2 -b $first_name2:$third_pos2" \
            "operator not exist" 1
        
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip" \
            "\"result\": true" 3

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Unsupported modify column.*varchar(30)" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip" \
            "\"result\": true" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "\"result\": true" 3

    run_sql_source1 "insert into ${db}.${tb1} values(3,3,3);"
    run_sql_source2 "insert into ${db}.${tb1} values(4,4,4);"

    run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 4"
}

function DM_4193() {
     run_case 4193 "double-source-pessimistic" "init_table 11 21" "clean_table" "pessimistic"
     run_case 4193 "double-source-optimistic" "init_table 11 21" "clean_table" "optimistic"
}

function DM_4230_CASE() {
    run_sql_source1 "insert into ${db}.${tb1} values(1);"
    run_sql_source1 "alter table ${db}.${tb1} add column c int unique;"

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "unsupported add column 'c' constraint UNIQUE KEY" 1

    run_sql_source1 "insert into ${db}.${tb1} values(2,2);"
        
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test replace alter table ${db}.${tb1} add column d int unique;" \
            "\"result\": true" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "unsupported add column 'd' constraint UNIQUE KEY" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test revert" \
            "\"result\": true" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "unsupported add column 'c' constraint UNIQUE KEY" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test replace alter table ${db}.${tb1} add column c int;" \
            "\"result\": true" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "\"result\": true" 2

    run_sql_source1 "insert into ${db}.${tb1} values(3,3);"

    run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 3"
}

function DM_4230() {
     run_case 4230 "single-source-no-sharding" "init_table 11" "clean_table" ""
}

# 4177, 4178, 4179, 4181, 4183, 4188, 4180, 4182
function DM_4177_CASE() {
    run_sql_source1 "insert into ${db}.${tb1} values(1);"
    run_sql_source1 "alter table ${db}.${tb1} modify id varchar(10);"

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Unsupported modify column" 1

    start_location=$(get_start_location 127.0.0.1:$MASTER_PORT $source1)

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error wrong-task skip" \
            "\"result\": false" 1 \
            "task wrong-task.*not exist" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error skip" \
            "Usage" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip -b $start_location -s wrong-source" \
            "source wrong-source.*not found" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip -b mysql-bin|1111 -s wrong-source" \
            "invalid --binlog-pos" 1

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "Unsupported modify column" 1
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip" \
            "\"result\": true" 2

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip" \
            "\"result\": false" 1 \
            "source.*has no error" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test replace \"alter table ${db}.${tb1} add column c int;\"" \
            "\"result\": false" 1 \
            "source.*has no error" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip -b mysql-bin.000000:00000" \
            "\"result\": true" 2

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test aaa" \
            "invalid operation 'aaa'" 1

    run_sql_source1 "insert into ${db}.${tb1} values(2);"
    run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 2"
}

function DM_4177() {
     run_case 4177 "single-source-no-sharding" "init_table 11" "clean_table" ""
     # 4184
     run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip" \
            "task.*not exist" 1
     # 4223
     run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test replace alter table ${db}.${tb1} add column c int" \
            "task.*not exist" 1
     # 4197
     run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test revert" \
            "task.*not exist" 1
}

function DM_4231_CASE() {
    run_sql_source1 "insert into ${db}.${tb1} values(1);"
    run_sql_source1 "alter table ${db}.${tb1} add column c int unique;"

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "unsupported add column 'c' constraint UNIQUE KEY" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test replace \"alter table ${db}.${tb1} add column c int; alter table ${db}.${tb1} add column d int unique;\"" \
            "\"result\": true" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "unsupported add column 'd' constraint UNIQUE KEY" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test revert" \
            "\"result\": true" 2

    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "unsupported add column 'c' constraint UNIQUE KEY" 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test replace alter table ${db}.${tb1} add unique(c);" \
            "\"result\": true" 2

    run_sql_source1 "insert into ${db}.${tb1} values(2,2);"
    run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 2"
}

function DM_4231() {
     run_case 4231 "single-source-no-sharding" "init_table 11" "clean_table" ""
}

function DM_SKIP_INCOMPATIBLE_DDL_CASE() {
    run_sql_source1 "insert into ${db}.${tb1} values(1);"

    run_sql_source1 "CREATE FUNCTION ${db}.hello (s CHAR(20)) RETURNS CHAR(50) DETERMINISTIC RETURN CONCAT('Hello, ',s,'!');"
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "\"stage\": \"Running\"" 2

    run_sql_source1 "/*!50003 drop function ${db}.hello*/;"
    run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "query-status test" \
            "drop function `hello`" 2 \
            "Please confirm your DDL statement is correct and needed." 1

    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "handle-error test skip" \
            "\"result\": true" 2

    run_sql_source1 "insert into ${db}.${tb1} values(2);"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_SKIP_INCOMPATIBLE_DDL() {
    run_case SKIP_INCOMPATIBLE_DDL "single-source-no-sharding" "init_table 11" "clean_table" ""
}

function run() {
    init_cluster
    init_database
    DM_SKIP_ERROR
    DM_SKIP_ERROR_SHARDING
    DM_REPLACE_ERROR
    DM_REPLACE_ERROR_SHARDING
    DM_REPLACE_ERROR_MULTIPLE
    DM_EXEC_ERROR_SKIP
    DM_SKIP_INCOMPATIBLE_DDL

    implement=(4202 4204 4206 4207 4209 4211 4213 4215 4216 4219 4220 4185 4201 4189 4210 4193 4230 4177 4231)
    for i in ${implement[@]}; do
        DM_${i}
        sleep 1
    done
}

cleanup_data $db
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
