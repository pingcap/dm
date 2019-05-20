#!/bin/bash

function sql_skip_wrong_arg() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "sql-skip" \
        "sql-skip <-w worker> \[-b binlog-pos\] \[-s sql-pattern\] \[--sharding\] <task-name> \[flags\]" 1
}

function sql_skip_binlogpos_sqlpattern_conflict() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "sql-skip test-task --binlog-pos mysql-bin:194 --sql-pattern ~(?i)ALTER\\s+TABLE\\s+" \
        "cannot specify both --binlog-pos and --sql-pattern in sql operation" 1
}

function sql_skip_invalid_binlog_pos() {
    binlog_pos="mysql-bin:shoud-bin-digital"
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "sql-skip test-task --binlog-pos $binlog_pos" \
        "invalid --binlog-pos $binlog_pos in sql operation: the pos should be digital" 1
}

function sql_skip_invalid_regex() {
    regex="~(\[a-z\])\\1"
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "sql-skip test-task --sql-pattern $regex" \
        "invalid --sql-pattern .* in sql operation:" 1
}

function sql_skip_sharding_with_binlogpos() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "sql-skip test-task --sharding --binlog-pos mysql-bin:13426" \
        "cannot specify --binlog-pos with --sharding in sql operation" 1
}

function sql_skip_non_sharding_without_one_worker() {
    worker1="127.0.0.1:$WORKER1_PORT"
    worker2="127.0.0.1:$WORKER2_PORT"
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "sql-skip test-task --worker $worker1,$worker2 --binlog-pos mysql-bin:13426" \
        "should only specify one worker, but got \[$worker1 $worker2\]" 1
}

function sql_skip_while_master_down() {
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
        "sql-skip test-task --sharding --sql-pattern ~(?i)ALTER\\s+TABLE\\s+" \
        "can not skip SQL:" 1
}
