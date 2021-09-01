#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare

WORK_DIR=$TEST_DIR/$TEST_NAME

function prepare_database() {
	run_sql 'DROP DATABASE if exists openapi;' $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql 'CREATE DATABASE openapi;' $MYSQL_PORT1 $MYSQL_PASSWORD1

	run_sql 'DROP DATABASE if exists openapi;' $MYSQL_PORT2 $MYSQL_PASSWORD2
	run_sql 'CREATE DATABASE openapi;' $MYSQL_PORT2 $MYSQL_PASSWORD2
}

function test_source() {
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>START TEST OPENAPI: SOURCE"
	prepare_database
	# create source succesfully
	$cur/client/source.py "create_source1_success"

	# recreate source will failed
	$cur/client/source.py "create_source_failed"

	# get source list success
	$cur/client/source.py "list_source_success" 1

	# delete source success
	$cur/client/source.py "delete_source_success" "mysql-01"

	# after delete source, source list should be empty
	$cur/client/source.py "list_source_success" 0

	# re delete source failed
	$cur/client/source.py "delete_source_failed" "mysql-01"

	# send request to not leader node
	$cur/client/source.py "list_source_with_redirect" 0

	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>TEST OPENAPI: SOURCE SUCCESS"
}

function run() {
	if [[ $(pip list) == *"requests"* ]]; then
		echo "requests already installed"
	else
		sudo pip install requests # in ci need sudo to install requests
	fi

	# run dm-master1
	run_dm_master $WORK_DIR/master1 $MASTER_PORT1 $cur/conf/dm-master1.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT1
	# join master2
	run_dm_master $WORK_DIR/master2 $MASTER_PORT2 $cur/conf/dm-master2.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT2
	# run dm-worker1
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	# run dm-worker2
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	test_source
}

cleanup_data openapi
cleanup_process

run

cleanup_process

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
