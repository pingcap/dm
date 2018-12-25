#!/bin/bash 

source ./util.sh 
OLDPWD="${ws}"

shard_db_1="shard_db_01"
shard_db_2="shard_db_02"
dump_dir="/tmp/dump"
shard_table_1="shard_table_01"
shard_table_2="shard_table_02"

import_data() {
    cd "${IMPORTER_DIR}" || exit
    ./bin/importer -L info -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" -u root -D $1 -t "create table $2(a int unique comment '[[range=1,1000]]', b double, c varchar(10));"  -c 1 -b 1000 -n 1000
    ./bin/importer -L info -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" -u root -D $3 -t "create table $4(a int unique comment '[[range=1001,2000]]', b double, c varchar(10));"  -c 1 -b 1000 -n 1000
    ./bin/importer -L info -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" -u root -D test -t "create table t(a int unique comment '[[range=1,1000]]', b double, c varchar(10));"  -c 1 -b 1000 -n 1000
    cd "${OLDPWD}" || exit
}

dump_data() {
    cd "${MYDUMPER_DIR}" || exit
    # clear dump dir
    rm -rf "$1"
    ./bin/mydumper -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" -u root -t 16 -F 128 --regex '^(?!(mysql|information_schema|performance_schema|sys))' --skip-tz-utc -o "$1"
    check_previous_command_success_or_exit
    cd "${OLDPWD}" || exit
}

load_data_to_tidb() {
    cd "${LOADER_DIR}" || exit
    cat > loader_config_sharding.toml << __EOF__
name = "test"
log-level = "debug"
log-file = "loader.log"
# Directory of the dump to import
dir = "$1"
# Loader pprof addr
pprof-addr = ":8272"
source-id = "127.0.0.1:3306"
# We saved checkpoint data to tidb, which schema name is defined here.
checkpoint-schema-prefix = "tidb_checkpoint_sharding"

# for old checkpoint
checkpoint = "$2"
# Number of threads for each pool
pool-size = 4
# Number of pools restore concurrently, one pool restore one block at a time, increase this as TiKV nodes increase
pool-count = 16
# Number of data files per block
file-num-per-block = 64
# DB config
[to]
host = "127.0.0.1"
user = "root"
password = ""
port = 4000

[[route-rules]]
schema-pattern = "shard_db_*"
target-schema = "shard_db"

[[route-rules]]
schema-pattern = "shard_db_*"
table-pattern = "shard_table_*"
target-schema = "shard_db"
target-table = "shard_table"

__EOF__
     ./bin/loader -config loader_config_sharding.toml
     if [ "$?" != 0 ];
     then
        echo "loader exits unexpectedly"
        tail -n 1000 ./loader.log
        exit 1
     fi
     cd "${OLDPWD}" || exit
}

diff_mysql_and_tidb() {
    # just simple counting
    count1=$(mysql -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" -u root -Nse "select count(*) from $1.$2")
    count2=$(mysql -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" -u root -Nse "select count(*) from $3.$4")
    count3=$(mysql -h 127.0.0.1 -P 4000 -u root -Nse "select count(*) from $5.$6")
    if [ "$((count1 + count2))"  != "$count3" ];then
        echo "quantity not matched"
        exit 1
    fi
    
    count4=$(mysql -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" -u root -Nse "select count(*) from test.t")
    count5=$(mysql -h 127.0.0.1 -P 4000 -u root -Nse "select count(*) from test.t")
    if [ "$count4" != "$count5" ]; then 
        echo "quantity not matched"
        exit 1
    fi
}

start_tidb tidb_loader_generic_test.log

# check mysql 
check_db_status 127.0.0.1 4000 tidb 
check_db_status "${MYSQL_HOST}" "${MYSQL_PORT}" mysql
mysql -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" -u root -e "create database ${shard_db_1}; create database ${shard_db_2}; create database test;"
import_data ${shard_db_1} ${shard_table_1}   ${shard_db_2}  ${shard_table_2} 
dump_data  ${dump_dir}
load_data_to_tidb ${dump_dir} "loader.checkpoint"
diff_mysql_and_tidb ${shard_db_1} ${shard_table_1} ${shard_db_2} ${shard_table_2} shard_db shard_table
stop_tidb
echo "loader sharding test finished"
