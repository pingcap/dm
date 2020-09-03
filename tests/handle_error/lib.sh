#!/bin/bash

set -eu

db="handle_error"
tb1="tb1"
tb2="tb2"
tb="tb"
ta1="ta1"
ta2="ta2"
ta="ta"

function init_database() {
    run_sql_both_source "drop database if exists ${db};"
    run_sql_both_source "create database if not exists ${db};"
}

function extract() {
    str="$1"
    source=${str:0:1}
    table=${str:1:1}
}

function init_table() {
    for i in $@; do
        extract $i
        run_sql_source${source} "create table ${db}.tb${table} (id int);"
    done
}

function clean_table() {
    run_sql_both_source "drop table if exists ${db}.${tb1};"
    run_sql_both_source "drop table if exists ${db}.${tb2};"
    run_sql_both_source "drop table if exists ${db}.${ta1};"
    run_sql_both_source "drop table if exists ${db}.${ta2};"
    run_sql_tidb "drop table if exists ${db}.${tb};"
    run_sql_tidb "drop table if exists ${db}.${ta};"
    run_sql_tidb "drop table if exists ${db}.${tb1};"
    run_sql_tidb "drop table if exists ${db}.${tb2};"

    run_sql_tidb "drop database if exists dm_meta;"
}