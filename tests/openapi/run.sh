#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare

WORK_DIR=$TEST_DIR/$TEST_NAME

function prepare_database() {
	run_sql 'DROP DATABASE if exists openapi;' $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql 'CREATE DATABASE openapi;' $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql "CREATE TABLE openapi.t(i TINYINT, j INT UNIQUE KEY);" $MYSQL_PORT1 $MYSQL_PASSWORD1
}

function incr_data() {
	run_sql "INSERT INTO openapi.t(i,j) VALUES (1, 2);" $MYSQL_PORT1 $MYSQL_PASSWORD1
}

function test_source() {
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>START TEST OPENAPI: SOURCE"
	# create source succesfully
	python $cur/client/source.py "create_source_success"

	# recreate source will failed
	python $cur/client/source.py "create_source_failed"

	# get source list success
	python $cur/client/source.py "list_source_success" 1

	# delete source success
	python $cur/client/source.py "delete_source_success"

	# after delete source, source list should be empty
	python $cur/client/source.py "list_source_success" 0

	# re delete source failed
	python $cur/client/source.py "delete_source_failed"

	# send request to not leader node
	python $cur/client/source.py "list_source_with_redirect" 0

	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>TEST OPENAPI: SOURCE SUCCESS"
}

function test_relay() {
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>START TEST OPENAPI: RELAY"

	# create source succesfully
	python $cur/client/source.py "create_source_success"

	# start relay failed
	python $cur/client/source.py "start_relay_failed"

	# start relay success
	python $cur/client/source.py "start_relay_success"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status -s mysql-01" \
		"\"result\": true" 2 \
		"\"worker\": \"worker1\"" 1 \
		"\"relayCatchUpMaster\": true" 1

	# get source status failed
	python $cur/client/source.py "get_source_status_failed"

	# get source status success
	python $cur/client/source.py "get_source_status_success"

	# stop relay failed
	python $cur/client/source.py "stop_relay_failed"

	# stop relay success
	python $cur/client/source.py "stop_relay_success"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status -s mysql-01" \
		"\"result\": true" 2 \
		"\"worker\": \"worker1\"" 1 \
		"\"relayStatus\": null" 1

	# delete source success
	python $cur/client/source.py "delete_source_success"

	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>TEST OPENAPI: RELAY SUCCESS"

}

function test_task() {
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>START TEST OPENAPI: TASK"

	# create source succesfully
	python $cur/client/source.py "create_source_success"

	# get source status success
	python $cur/client/source.py "get_source_status_success"

	# start task success
	python $cur/client/task.py "start_task_success"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Running\"" 1

	incr_data
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# stop task success
	python $cur/client/task.py "stop_task_success" "test"

	# stop task failed
	python $cur/client/task.py "stop_task_failed" "test"

	# delete source success
	python $cur/client/source.py "delete_source_success"

	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>TEST OPENAPI: TASK SUCCESS"

}

function run() {
	pip install requests
	prepare_database

	run_dm_master $WORK_DIR/master1 $MASTER_PORT1 $cur/conf/dm-master1.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT1
	# join master2
	run_dm_master $WORK_DIR/master2 $MASTER_PORT2 $cur/conf/dm-master2.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT2
	# run dm-worker1
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	test_source
	test_relay
	test_task
}

cleanup_data openapi
cleanup_process

run

cleanup_process

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
