#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare

WORK_DIR=$TEST_DIR/$TEST_NAME

function prepare_data() {
	run_sql 'DROP DATABASE if exists openapi;' $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql 'CREATE DATABASE openapi;' $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql "CREATE TABLE openapi.t(i TINYINT, j INT UNIQUE KEY);" $MYSQL_PORT1 $MYSQL_PASSWORD1
}

function test_source() {
	echo "start test source openapi"
	# create source succesfully
	python $cur/client/source.py "source_success"

	# recreate source will failed
	python $cur/client/source.py "source_failed"

	# get source list success
	python $cur/client/source.py "source_list_success" 1

	# delete source success
	python $cur/client/source.py "delete_source_success"

	# after delete source, source list should be empty
	python $cur/client/source.py "source_list_success" 0

	# re delete source failed
	python $cur/client/source.py "delete_source_failed"

	# send request to not leader node
	python $cur/client/source.py "list_source_by_openapi_redirect" 0
	echo "test source openapi success"
}

function run() {
	pip install requests
	prepare_data

	run_dm_master $WORK_DIR/master1 $MASTER_PORT1 $cur/conf/dm-master1.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT1
	# join master2
	run_dm_master $WORK_DIR/master2 $MASTER_PORT2 $cur/conf/dm-master2.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT2

	test_source

}

cleanup_data openapi
cleanup_process

run $*

cleanup_process

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
