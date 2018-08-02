#!/bin/bash

source ./util.sh

OLDPWD="${ws}"

import_data() {
    cd "${IMPORTER_DIR}" || exit
    ./bin/importer -L info -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" -u root -D test -t "create table t(a int unique comment '[[range=1,1000]]', b double, c varchar(10));"  -c 1 -b 1000 -n 1000
    check_previous_command_success_or_exit
    mysql -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" -u root -e "create index b on test.t(b); create index c on test.t(c);"
    check_previous_command_success_or_exit
    mysql -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" -u root -e "drop index a on test.t; drop index b on test.t; drop index c on test.t;"
    check_previous_command_success_or_exit
    mysql -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" -u root -e "alter table test.t add column id1 int not null, add column id2 int not null default 1;"
    check_previous_command_success_or_exit
    mysql -h  "${MYSQL_HOST}" -P "${MYSQL_PORT}" -u root -e "alter table test.t drop column id1, drop column id2;"
    check_previous_command_success_or_exit
    mysql -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" -u root -e "alter table test.t rename to test.t1, add index a(a), add index b(b), rename to test.t;"
    check_previous_command_success_or_exit
    mysql -h  "${MYSQL_HOST}" -P "${MYSQL_PORT}" -u root -e "use test; create table t2(id int); truncate table t2; drop table t2;"
    check_previous_command_success_or_exit
    cd "${OLDPWD}" || exit
}

# run in background
start_syncer() {
    cd "$SYNCER_DIR" || exit 
    killall -9 syncer || true

    # remove old .meta file if exists
    rm -f "$1"

    # for toml config, value has to be set.
    cat > "$1" << __EOF__
binlog-name = ""
binlog-pos = 0
binlog-gtid = "$5"
__EOF__

    cat > syncer_config_gtid.toml << __EOF__
log-level = "info"
server-id = 101
meta = "$1"
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
__EOF__
    ./bin/syncer -config syncer_config_gtid.toml --enable-gtid=true -log-file syncer_gtid_test.log &
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
        m_file=$(curl "$1" | grep 'syncer_binlog_file{node="master"}' | awk '{print $2}')
        s_file=$(curl "$1" | grep 'syncer_binlog_file{node="syncer"}' | awk '{print $2}')
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

        m_pos=$(curl "$1" | grep 'syncer_binlog_pos{node="master"}'| awk '{print $2}')
        s_pos=$(curl "$1" | grep 'syncer_binlog_pos{node="syncer"}'| awk '{print $2}')
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
    echo "stopped syncer"
}

diff_mysql_and_tidb() {
    cd "${DIFF_DIR}" || exit
    result=$(./bin/diff -B "test" -url1 "root@127.0.0.1:4000" -url2 "root@${MYSQL_HOST}:${MYSQL_PORT}")
    if [ "$result" != "true" ]; then
        echo ""
        exit 1
    fi
    cd "${OLDPWD}" || exit
    echo "diff result is identical"
}

start_tidb tidb_syncer_gtid_test.log
check_db_status 127.0.0.1 4000 tidb 
check_db_status "${MYSQL_HOST}" "${MYSQL_PORT}" mysql


mysql -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" -u root -e 'select @@version'
mysql -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" -u root -e 'drop database if exists test; reset master;'
read -r gtidSet <<< $(mysql -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" -u root -Nse 'show master status' | awk '{print $3}')
start_syncer "syncer_gtid.meta" 10081 "${MYSQL_HOST}" "${MYSQL_PORT}"  "$gtidSet"
mysql -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" -u root -e 'create database test;'

sleep 25
import_data 

check_syncer_complete_and_stop "http://127.0.0.1:${STATUS_PORT}/metrics" 
diff_mysql_and_tidb 
stop_tidb
echo "syncer gtid test finished"
