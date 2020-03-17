#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare

WORK_DIR=$TEST_DIR/$TEST_NAME

API_VERSION="v1alpha1"

function run_tidb_with_tls() {
cat - > "$WORK_DIR/tidb-tls-config.toml" <<EOF
[security]
# set the path for certificates. Empty string means disabling secure connectoins.
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

    cp $cur/conf/dm-master.toml $WORK_DIR/
    cp $cur/conf/dm-worker1.toml $WORK_DIR/
    cp $cur/conf/dm-task.yaml $WORK_DIR/

    sed -i "s%dir-placeholer%$cur\/conf%g" $WORK_DIR/dm-master.toml
    sed -i "s%dir-placeholer%$cur\/conf%g" $WORK_DIR/dm-worker1.toml
    sed -i "s%dir-placeholer%$cur\/conf%g" $WORK_DIR/dm-task.yaml

    run_dm_master $WORK_DIR/master $MASTER_PORT $WORK_DIR/dm-master.toml
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT "ca.pem" "dm.pem" "dm.key"
    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $WORK_DIR/dm-worker1.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT "ca.pem" "dm.pem" "dm.key"
    # operate mysql config to worker
    cp $cur/conf/source1.toml $WORK_DIR/source1.toml
    sed -i "/relay-binlog-name/i\relay-dir = \"$WORK_DIR/worker1/relay_log\"" $WORK_DIR/source1.toml
    dmctl_operate_source create $WORK_DIR/source1.toml $SOURCE_ID1

    echo "start task and check stage"
    run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
            "start-task $WORK_DIR/dm-task.yaml" \
            "\"result\": true" 1

	sleep 1
    curl -X GET 127.0.0.1:$MASTER_PORT/apis/${API_VERSION}/status/test > $WORK_DIR/status.log
    check_log_contains $WORK_DIR/status.log "\"stage\":\"Running\"" 1
    check_log_contains $WORK_DIR/status.log "\"name\":\"test\"" 1

    echo "pause task and check stage"
    curl -X PUT 127.0.0.1:$MASTER_PORT/apis/${API_VERSION}/tasks/test -d '{ "op": 2 }' > $WORK_DIR/pause.log
    check_log_contains $WORK_DIR/pause.log "\"op\":\"Pause\"" 1

	sleep 1
    curl -X GET 127.0.0.1:$MASTER_PORT/apis/${API_VERSION}/status/test > $WORK_DIR/status.log
    check_log_contains $WORK_DIR/status.log "\"stage\":\"Paused\"" 1
    check_log_contains $WORK_DIR/status.log "\"name\":\"test\"" 1

    echo "resume task and check stage"
    curl -X PUT 127.0.0.1:$MASTER_PORT/apis/${API_VERSION}/tasks/test -d '{ "op": 3 }' > $WORK_DIR/resume.log
    check_log_contains $WORK_DIR/resume.log "\"op\":\"Resume\"" 1

	sleep 1
    curl -X GET 127.0.0.1:$MASTER_PORT/apis/${API_VERSION}/status/test > $WORK_DIR/status.log
    check_log_contains $WORK_DIR/status.log "\"stage\":\"Running\"" 1
    check_log_contains $WORK_DIR/status.log "\"name\":\"test\"" 1

    echo "check data"
    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

cleanup_data tls
cleanup_process

run $*

cleanup_process

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
