#!/bin/bash

set -eu

shardddl="shardddl"
shardddl1="shardddl1"
shardddl2="shardddl2"
tb1="tb1"
tb2="tb2"
tb3="tb3"
tb4="tb4"
tb="tb"

function init_database() {
    run_sql_both_source "drop database if exists ${shardddl1};"
    run_sql_both_source "drop database if exists ${shardddl2};"
    run_sql_both_source "create database if not exists ${shardddl1};"
    run_sql_both_source "create database if not exists ${shardddl2};"
}

function extract() {
    str="$1"
    source=${str:0:1}
    database=${str:1:1}
    table=${str:2:1}
}

function init_table() {
    for i in $@; do
        extract $i
        run_sql_source${source} "create table shardddl${database}.tb${table} (id int);"
    done
}

function clean_table() {
    run_sql_both_source "drop table if exists ${shardddl1}.${tb1};"
    run_sql_both_source "drop table if exists ${shardddl1}.${tb2};"
    run_sql_both_source "drop table if exists ${shardddl1}.${tb3};"
    run_sql_both_source "drop table if exists ${shardddl1}.${tb4};"
    run_sql_both_source "drop table if exists ${shardddl2}.${tb1};"
    run_sql_both_source "drop table if exists ${shardddl2}.${tb2};"
    run_sql_both_source "drop table if exists ${shardddl2}.${tb3};"
    run_sql_both_source "drop table if exists ${shardddl2}.${tb4};"
    run_sql_tidb "drop table if exists ${shardddl}.${tb};"
    run_sql_tidb "drop database if exists dm_meta;"
}