#!/bin/bash 

source ./util.sh

OLDPWD="${ws}"
shard_db_1="shard_db_01"
shard_db_2="shard_db_02"
shard_table_1="shard_table_01"
shard_table_2="shard_table_02"
syncer_status_port=8271

import_data() {
    cd "${IMPORTER_DIR}" || exit
    ./bin/importer -L info -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" -u root -D "$1" -t "create table $2(a int unique comment '[[range=1,1000]]', b double, c varchar(10));"  -c 1 -b 1000 -n 1000
    ./bin/importer -L info -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" -u root -D "$3" -t "create table $4(a int unique comment '[[range=1001,2000]]', b double, c varchar(10));"  -c 1 -b 1000 -n 1000
    ./bin/importer -L info -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" -u root -D test -t "create table t(a int unique comment '[[range=1,1000]]', b double, c varchar(10));"  -c 1 -b 1000 -n 1000
    cd "${OLDPWD}" || exit
}

# run in background
start_syncer() {
    cd "$SYNCER_DIR" || exit 
    killall -9 syncer || true

    # remove old .meta file if exists
    rm -f "$1"

    cat > "$1" << __EOF__
binlog-name = "$5"
binlog-pos = $6
binlog-gtid = ""
__EOF__

    cat > syncer_config_sharding.toml << __EOF__
name = "test"
log-level = "info"
server-id = 101
source-id = "127.0.0.1:3306"
meta-file = "$1"
worker-count = 16
batch = 1000
status-addr = ":$2"

[from]
host = "$3"
user = "root"
password = ""
port = $4

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
    ./bin/syncer -config syncer_config_sharding.toml -log-file syncer_sharding_test.log --disable-heartbeat=true &
}

check_syncer_complete_and_stop() {
    # wait for synce start
    wait_for_syncer_alive
    sleep 10
    while true
    do
        # make sure syncer is still alive
        check_syncer_alive
        ret=$?
        if [ "$ret" == 0 ]; then 
            echo "syncer is not alive"
            exit 1
        fi 
        m_file=$(curl "$1" | grep 'syncer_binlog_file' | grep 'node="master"' | awk '{print $2}')
        s_file=$(curl "$1" | grep 'syncer_binlog_file' | grep 'node="syncer"' | awk '{print $2}')
        echo "$m_file"
        echo "$s_file"
        if [ -z "$m_file" ] || [ -z "$s_file" ];then
            sleep 1
            continue 
        fi

        if [ "$m_file" != "$s_file" ]; then
            sleep 1
            continue
        fi

        m_pos=$(curl "$1" | grep 'syncer_binlog_pos' | grep 'node="master"'| awk '{print $2}')
        s_pos=$(curl "$1" | grep 'syncer_binlog_pos' | grep 'node="syncer"'| awk '{print $2}')
        if [ -z "$m_pos" ] || [ -z "$s_pos" ];then
            sleep 1
            continue 
        fi

        if [ "$m_pos" != "$s_pos" ]; then
            sleep 1
           continue
        fi
        break
    done 
    
    echo "syncer sync completes"

    stop_syncer
    cat syncer_sharding_test.log
    echo "stopped syncer"
}


diff_mysql_and_tidb() {
    # just simple counting
    count1=$(mysql -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" -u root -Nse "select count(*) from $1.$2")
    count2=$(mysql -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" -u root -Nse "select count(*) from $3.$4")
    count3=$(mysql -h 127.0.0.1 -P 4000 -u root -Nse "select count(*) from $5.$6")
    if [ "$((count1 + count2))"  != "$count3" ];
    then
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


start_tidb tidb_syncer_sharding_test.log
check_db_status 127.0.0.1 4000 tidb 
check_db_status "${MYSQL_HOST}" "${MYSQL_PORT}" mysql

mysql -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" -u root -e "select @@version"
mysql -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" -u root -e "drop database if exists test;drop database if exists ${shard_db_1};drop database if exists ${shard_db_2}; reset master;"
read -r file pos <<< $(mysql -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" -u root -Nse 'show master status' | awk '{print $1,$2}')
start_syncer "syncer_sharding.meta" ${syncer_status_port} "${MYSQL_HOST}" "${MYSQL_PORT}"  "$file" "$pos"
mysql -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" -u root -e "create database test; create database ${shard_db_1}; create database ${shard_db_2};"

sleep 25
import_data "${shard_db_1}" "${shard_table_1}"  "${shard_db_2}" "${shard_table_2}" 

check_syncer_complete_and_stop "http://127.0.0.1:${syncer_status_port}/metrics"
diff_mysql_and_tidb ${shard_db_1} ${shard_table_1} ${shard_db_2} ${shard_table_2} shard_db shard_table
stop_tidb
echo "syncer_sharding_test finished"
