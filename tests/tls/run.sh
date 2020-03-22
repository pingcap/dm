#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare

WORK_DIR=$TEST_DIR/$TEST_NAME

API_VERSION="v1alpha1"

function run_tidb_with_tls() {
    echo "run a new tidb server with tls"
    cat - > "$WORK_DIR/tidb-tls-config.toml" <<EOF
status-port = 10090
[security]
# set the path for certificates. Empty string means disabling secure connectoins.
ssl-ca = "$cur/conf/ca.pem"
ssl-cert = "$cur/conf/dm.pem"
ssl-key = "$cur/conf/dm.key"
cluster-ssl-ca = "$cur/conf/ca.pem"
cluster-ssl-cert = "$cur/conf/dm.pem"
cluster-ssl-key = "$cur/conf/dm.key"
EOF

    bin/tidb-server \
    -P 4400 \
    --path $WORK_DIR/tidb \
    --store mocktikv \
    --config $WORK_DIR/tidb-tls-config.toml \
    --log-file "$WORK_DIR/tidb.log" &

    sleep 3
    mysql -uroot -h127.0.0.1 -P4400 --default-character-set utf8 --ssl-ca $cur/conf/ca.pem --ssl-cert $cur/conf/dm.pem --ssl-key $cur/conf/dm.key -E -e "drop database if exists tls"
    mysql -uroot -h127.0.0.1 -P4400 --default-character-set utf8 --ssl-ca $cur/conf/ca.pem --ssl-cert $cur/conf/dm.pem --ssl-key $cur/conf/dm.key -E -e "drop database if exists dm_meta"
}

function prepare_data() {
    run_sql 'DROP DATABASE if exists tls;' $MYSQL_PORT1 $MYSQL_PASSWORD1
    run_sql 'CREATE DATABASE tls;' $MYSQL_PORT1 $MYSQL_PASSWORD1
    run_sql "CREATE TABLE tls.t(i TINYINT, j INT UNIQUE KEY);" $MYSQL_PORT1 $MYSQL_PASSWORD1
    for j in $(seq 10); do
        run_sql "INSERT INTO tls.t VALUES ($j,${j}000$j),($j,${j}001$j);" $MYSQL_PORT1 $MYSQL_PASSWORD1
    done
}

function run() {
    run_tidb_with_tls

    prepare_data

    cp $cur/conf/dm-master1.toml $WORK_DIR/
    cp $cur/conf/dm-master2.toml $WORK_DIR/
    cp $cur/conf/dm-master3.toml $WORK_DIR/
    cp $cur/conf/dm-worker1.toml $WORK_DIR/
    cp $cur/conf/dm-task.yaml $WORK_DIR/

    sed -i "s%dir-placeholer%$cur\/conf%g" $WORK_DIR/dm-master1.toml
    sed -i "s%dir-placeholer%$cur\/conf%g" $WORK_DIR/dm-master2.toml
    sed -i "s%dir-placeholer%$cur\/conf%g" $WORK_DIR/dm-master3.toml
    sed -i "s%dir-placeholer%$cur\/conf%g" $WORK_DIR/dm-worker1.toml
    sed -i "s%dir-placeholer%$cur\/conf%g" $WORK_DIR/dm-task.yaml

    run_dm_master $WORK_DIR/master1 $MASTER_PORT1 $WORK_DIR/dm-master1.toml
    run_dm_master $WORK_DIR/master2 $MASTER_PORT2 $WORK_DIR/dm-master2.toml
    run_dm_master $WORK_DIR/master3 $MASTER_PORT3 $WORK_DIR/dm-master3.toml
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT1 "$cur/conf/ca.pem" "$cur/conf/dm.pem" "$cur/conf/dm.key"
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT2 "$cur/conf/ca.pem" "$cur/conf/dm.pem" "$cur/conf/dm.key"
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT3 "$cur/conf/ca.pem" "$cur/conf/dm.pem" "$cur/conf/dm.key"

    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $WORK_DIR/dm-worker1.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT "$cur/conf/ca.pem" "$cur/conf/dm.pem" "$cur/conf/dm.key"
    # operate mysql config to worker
    cp $cur/conf/source1.toml $WORK_DIR/source1.toml
    sed -i "/relay-binlog-name/i\relay-dir = \"$WORK_DIR/worker1/relay_log\"" $WORK_DIR/source1.toml
    run_dm_ctl_with_tls $WORK_DIR "127.0.0.1:$MASTER_PORT" $cur/conf/ca.pem $cur/conf/dm.pem $cur/conf/dm.key \
        "operate-source create $WORK_DIR/source1.toml" \
        "\"result\": true" 2 \
        "\"source\": \"$SOURCE_ID1\"" 1

    echo "start task and check stage"
    run_dm_ctl_with_tls $WORK_DIR "127.0.0.1:$MASTER_PORT" $cur/conf/ca.pem $cur/conf/dm.pem $cur/conf/dm.key \
            "start-task $WORK_DIR/dm-task.yaml" \
            "\"result\": true" 2

    run_dm_ctl_with_tls $WORK_DIR "127.0.0.1:$MASTER_PORT" $cur/conf/ca.pem $cur/conf/dm.pem $cur/conf/dm.key \
            "query-status test" \
            "\"result\": true" 2

    echo "test http interface"
    check_rpc_alive $cur/../bin/check_master_online_http 127.0.0.1:$MASTER_PORT1 "$cur/conf/ca.pem" "$cur/conf/dm.pem" "$cur/conf/dm.key"

    # FIXME: should not success
    check_rpc_alive $cur/../bin/check_master_online_http 127.0.0.1:$MASTER_PORT1 "$cur/conf/ca.pem" "$cur/conf/other.pem" "$cur/conf/other.key"

    sleep 1

    echo "check data"
    mysql -uroot -h127.0.0.1 -P4400 --default-character-set utf8 --ssl-ca $cur/conf/ca.pem --ssl-cert $cur/conf/dm.pem --ssl-key $cur/conf/dm.key -E -e "select count(*) from tls.t" > "$TEST_DIR/sql_res.$TEST_NAME.txt"
    check_contains "count(*): 20"
}

cleanup_data tls
cleanup_process

run $*

# kill the tidb with tls
pkill -hup tidb-server 2>/dev/null || true
wait_process_exit tidb-server

run_tidb_server 4000 $TIDB_PASSWORD

cleanup_process

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
