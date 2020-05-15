#!/bin/bash

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/test_prepare

WORK_DIR=$TEST_DIR/$TEST_NAME

API_VERSION="v1alpha1"

function prepare_data() {
    run_sql 'DROP DATABASE if exists http_apis;' $MYSQL_PORT1 $MYSQL_PASSWORD1
    run_sql 'CREATE DATABASE http_apis;' $MYSQL_PORT1 $MYSQL_PASSWORD1
    run_sql "CREATE TABLE http_apis.t(i TINYINT, j INT UNIQUE KEY);" $MYSQL_PORT1 $MYSQL_PASSWORD1
    for j in $(seq 100); do
        run_sql "INSERT INTO http_apis.t VALUES ($j,${j}000$j),($j,${j}001$j);" $MYSQL_PORT1 $MYSQL_PASSWORD1
    done
}

function run() {
    prepare_data

    run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
    check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
    run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
    check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
    # operate mysql config to worker
    cp $cur/conf/source1.toml $WORK_DIR/source1.toml
    sed -i "/relay-binlog-name/i\relay-dir = \"$WORK_DIR/worker1/relay_log\"" $WORK_DIR/source1.toml

    cat $WORK_DIR/source1.toml | sed 's/$/\\n/' | sed 's/"/\\"/g' | tr -d '\n' > $WORK_DIR/source1.toml.bak
    source_data=`cat $WORK_DIR/source1.toml.bak`
    rm $WORK_DIR/source1.toml.bak
    echo $source_data
    curl -X PUT 127.0.0.1:$MASTER_PORT/apis/${API_VERSION}/sources -d '{"op": 1, "config": "'"$source_data"'"}' > $WORK_DIR/create-source.log
    check_log_contains $WORK_DIR/create-source.log "\"result\":true" 1
    check_log_contains $WORK_DIR/create-source.log "\"source\":\"$SOURCE_ID1\"" 1

    echo "start task and check stage"
    cat $cur/conf/dm-task.yaml | sed 's/$/\\n/' | sed 's/"/\\"/g' | tr -d '\n' > $WORK_DIR/task.yaml.bak
    task_data=`cat $WORK_DIR/task.yaml.bak`
    rm $WORK_DIR/task.yaml.bak
    echo $task_data

    check_http_alive 127.0.0.1:$MASTER_PORT/apis/${API_VERSION}/status/test-task "task test-task has no source or not exist" 3

    curl -X POST 127.0.0.1:$MASTER_PORT/apis/${API_VERSION}/tasks -d '{"task": "'"$task_data"'"}' > $WORK_DIR/start-task.log
    check_log_contains $WORK_DIR/start-task.log "\"result\":true" 1

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

cleanup_data http_apis
cleanup_process

run $*

cleanup_process

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
