#!/bin/bash

function sql_replace_wrong_arg() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "sql-replace" \
        "sql-replace <-s source> \[-b binlog-pos\] \[-p sql-pattern\] \[--sharding\] <task-name> <sql1;sql2;> \[flags\]" 1
}

function sql_replace_invalid_binlog_pos() {
    binlog_pos="mysql-bin:shoud-bin-digital"
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "sql-replace test-task --binlog-pos $binlog_pos sql1" \
        "\[.*\] invalid --binlog-pos $binlog_pos in sql operation: the pos should be digital" 1
}

function sql_replace_non_sharding_without_one_worker() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "sql-replace test-task --source $SOURCE_ID1,$SOURCE_ID2 --binlog-pos mysql-bin:13426 sql1" \
        "should only specify one source, but got \[$SOURCE_ID1 $SOURCE_ID2\]" 1
}

function sql_replace_while_master_down() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "sql-replace test-task --sharding --sql-pattern ~(?i)ALTER\\s+TABLE\\s+ ALTER TABLE tbl DROP COLUMN col" \
        "can not replace SQL:" 1
}
