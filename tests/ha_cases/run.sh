#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
API_VERSION="v1alpha1"
# import helper functions
source $cur/lib.sh

function test_running() {
	echo "[$(date)] <<<<<< start test_running >>>>>>"
	cleanup
	prepare_sql
	start_cluster

	# make sure task to step in "Sync" stage
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT3" \
		"query-status test" \
		"\"stage\": \"Running\"" 2 \
		"\"unit\": \"Sync\"" 2

	echo "use sync_diff_inspector to check full dump loader"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	echo "flush logs to force rotate binlog file"
	run_sql "flush logs;" $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql "flush logs;" $MYSQL_PORT2 $MYSQL_PASSWORD2

	echo "apply increment data before restart dm-worker to ensure entering increment phase"
	run_sql_file_withdb $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1 $ha_test
	run_sql_file_withdb $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2 $ha_test

	sleep 3 # wait for flush checkpoint
	echo "use sync_diff_inspector to check increment data"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	echo "[$(date)] <<<<<< finish test_running >>>>>>"
}

function test_join_masters_and_worker {
	echo "[$(date)] <<<<<< start test_join_masters_and_worker >>>>>>"
	cleanup

	run_dm_master $WORK_DIR/master-join1 $MASTER_PORT1 $cur/conf/dm-master-join1.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT1

	echo "query-status from unique master"
	run_dm_ctl_with_retry $WORK_DIR 127.0.0.1:$MASTER_PORT1 "query-status" '"result": true' 1

	run_dm_master $WORK_DIR/master-join2 $MASTER_PORT2 $cur/conf/dm-master-join2.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT2
	sleep 5
	run_dm_master $WORK_DIR/master-join3 $MASTER_PORT3 $cur/conf/dm-master-join3.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT3
	sleep 5
	run_dm_master $WORK_DIR/master-join4 $MASTER_PORT4 $cur/conf/dm-master-join4.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT4
	sleep 5
	run_dm_master $WORK_DIR/master-join5 $MASTER_PORT5 $cur/conf/dm-master-join5.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT5

	run_dm_ctl_with_retry $WORK_DIR 127.0.0.1:$MASTER_PORT2 "query-status" '"result": true' 1
	run_dm_ctl_with_retry $WORK_DIR 127.0.0.1:$MASTER_PORT3 "query-status" '"result": true' 1
	run_dm_ctl_with_retry $WORK_DIR 127.0.0.1:$MASTER_PORT4 "query-status" '"result": true' 1
	run_dm_ctl_with_retry $WORK_DIR 127.0.0.1:$MASTER_PORT5 "query-status" '"result": true' 1

	echo "join worker with dm-master1 endpoint"
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker-join2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	run_dm_ctl_with_retry $WORK_DIR 127.0.0.1:$MASTER_PORT2 "list-member --worker --name=worker2" '"stage": "free",' 1

	echo "kill dm-master-join1"
	ps aux | grep dm-master-join1 | awk '{print $2}' | xargs kill || true
	check_port_offline $MASTER_PORT1 20
	rm -rf $WORK_DIR/master1/default.master1

	run_dm_ctl_with_retry $WORK_DIR 127.0.0.1:$MASTER_PORT2 "list-member --worker --name=worker2" '"stage": "free",' 1

	echo "join worker with 5 masters endpoint"
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker-join1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	echo "query-status from master2"
	run_dm_ctl_with_retry $WORK_DIR 127.0.0.1:$MASTER_PORT2 "query-status" '"result": true' 1

	echo "[$(date)] <<<<<< finish test_join_masters_and_worker >>>>>>"
}

function test_standalone_running() {
	echo "[$(date)] <<<<<< start test_standalone_running >>>>>>"
	cleanup
	prepare_sql
	start_standalone_cluster

	echo "use sync_diff_inspector to check full dump loader"
	check_sync_diff $WORK_DIR $cur/conf/diff-standalone-config.toml

	echo "flush logs to force rotate binlog file"
	run_sql "flush logs;" $MYSQL_PORT1 $MYSQL_PASSWORD1

	echo "apply increment data before restart dm-worker to ensure entering increment phase"
	run_sql_file_withdb $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1 $ha_test

	echo "use sync_diff_inspector to check increment data"
	check_sync_diff $WORK_DIR $cur/conf/diff-standalone-config.toml

	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $cur/conf/standalone-task2.yaml" \
		"\"result\": false" 1

	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $cur/conf/standalone-task2.yaml" \
		"\"result\": true" 2 \
		"\"source\": \"$SOURCE_ID2\"" 1

	worker=$($PWD/bin/dmctl.test DEVEL --master-addr "127.0.0.1:$MASTER_PORT" query-status test2 |
		grep 'worker' | awk -F: '{print $2}')
	worker_name=${worker:0-9:7}
	worker_idx=${worker_name:0-1:1}
	worker_ports=(0 WORKER1_PORT WORKER2_PORT)

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status" \
		"\"taskStatus\": \"Running\"" 2

	echo "kill $worker_name"
	ps aux | grep dm-worker${worker_idx} | awk '{print $2}' | xargs kill || true
	check_port_offline ${worker_ports[$worker_idx]} 20
	rm -rf $WORK_DIR/worker${worker_idx}/relay-dir

	# test running, test2 fail
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status" \
		"\"taskStatus\": \"Running\"" 1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task test2" \
		"\"result\": true" 1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $cur/conf/standalone-task2.yaml" \
		"\"result\": false" 1

	# test should still running
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Running\"" 1

	echo "[$(date)] <<<<<< finish test_standalone_running >>>>>>"
}

function test_config_name() {
	echo "[$(date)] <<<<<< start test_config_name >>>>>>"

	cp $cur/conf/dm-master-join2.toml $WORK_DIR/dm-master-join2.toml
	sed -i "s/name = \"master2\"/name = \"master1\"/g" $WORK_DIR/dm-master-join2.toml
	run_dm_master $WORK_DIR/master-join1 $MASTER_PORT1 $cur/conf/dm-master-join1.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT1
	run_dm_master $WORK_DIR/master-join2 $MASTER_PORT2 $WORK_DIR/dm-master-join2.toml
	check_log_contain_with_retry "missing data or joining a duplicate member master1" $WORK_DIR/master-join2/log/dm-master.log

	TEST_CHAR="!@#$%^\&*()_+¥"
	cp $cur/conf/dm-master-join2.toml $WORK_DIR/dm-master-join2.toml
	sed -i "s/name = \"master2\"/name = \"test$TEST_CHAR\"/g" $WORK_DIR/dm-master-join2.toml
	run_dm_master $WORK_DIR/master-join2 $MASTER_PORT2 $WORK_DIR/dm-master-join2.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT2

	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	cp $cur/conf/dm-worker2.toml $WORK_DIR/dm-worker2.toml
	sed -i "s/name = \"worker2\"/name = \"worker1\"/g" $WORK_DIR/dm-worker2.toml
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $WORK_DIR/dm-worker2.toml
	sleep 2

	check_log_contain_with_retry "[dm-worker with name {\"name\":\"worker1\",\"addr\":\"127.0.0.1:8262\"} already exists]" $WORK_DIR/worker2/log/dm-worker.log

	cp $cur/conf/dm-worker2.toml $WORK_DIR/dm-worker2.toml
	sed -i "s/name = \"worker2\"/name = \"master1\"/g" $WORK_DIR/dm-worker2.toml
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $WORK_DIR/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	echo "[$(date)] <<<<<< finish test_config_name >>>>>>"
}

function test_last_bound() {
	echo "[$(date)] <<<<<< start test_last_bound >>>>>>"
	test_running

	# now in start_cluster, we ensure source_i is bound to worker_i
	worker1bound=$($PWD/bin/dmctl.test DEVEL --master-addr "127.0.0.1:$MASTER_PORT1" list-member --name worker1 |
		grep 'source' | awk -F: '{print $2}')
	echo "worker1bound $worker1bound"
	worker2bound=$($PWD/bin/dmctl.test DEVEL --master-addr "127.0.0.1:$MASTER_PORT1" list-member --name worker2 |
		grep 'source' | awk -F: '{print $2}')
	echo "worker2bound $worker2bound"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $SOURCE_ID1 worker1" \
		"\"result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $SOURCE_ID2 worker2" \
		"\"result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Running\"" 4

	kill_2_worker_ensure_unbound 1 2

	# start 1 then 2
	start_2_worker_ensure_bound 1 2

	check_bound
	# only contains 1 "will try purge ..." which is printed the first time dm worker start
	check_log_contains $WORK_DIR/worker1/log/dm-worker.log "will try purge whole relay dir for new relay log" 1
	check_log_contains $WORK_DIR/worker2/log/dm-worker.log "will try purge whole relay dir for new relay log" 1

	kill_2_worker_ensure_unbound 1 2

	# start 2 then 1
	start_2_worker_ensure_bound 2 1

	check_bound
	check_log_contains $WORK_DIR/worker1/log/dm-worker.log "will try purge whole relay dir for new relay log" 1
	check_log_contains $WORK_DIR/worker2/log/dm-worker.log "will try purge whole relay dir for new relay log" 1

	# kill 12, start 34, kill 34
	kill_2_worker_ensure_unbound 1 2
	start_2_worker_ensure_bound 3 4
	worker3bound=$($PWD/bin/dmctl.test DEVEL --master-addr "127.0.0.1:$MASTER_PORT1" list-member --name worker3 |
		grep 'source' | awk -F: '{print $2}' | cut -d'"' -f 2)
	worker4bound=$($PWD/bin/dmctl.test DEVEL --master-addr "127.0.0.1:$MASTER_PORT1" list-member --name worker4 |
		grep 'source' | awk -F: '{print $2}' | cut -d'"' -f 2)
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $worker3bound worker3" \
		"\"result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $worker4bound worker4" \
		"\"result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Running\"" 4

	# let other workers rather then 1 2 forward the syncer's progress
	run_sql_file_withdb $cur/data/db1.increment2.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1 $ha_test
	run_sql "flush logs;" $MYSQL_PORT2 $MYSQL_PASSWORD2
	run_sql_file_withdb $cur/data/db2.increment2.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2 $ha_test
	# wait the checkpoint updated
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	kill_2_worker_ensure_unbound 3 4

	# start 1 then 2
	start_2_worker_ensure_bound 1 2

	# check
	check_bound
	# other workers has forwarded the sync progress, if moved to a new binlog file, original relay log could be removed
	num1=$(grep "will try purge whole relay dir for new relay log" $WORK_DIR/worker1/log/dm-worker.log | wc -l)
	num2=$(grep "will try purge whole relay dir for new relay log" $WORK_DIR/worker2/log/dm-worker.log | wc -l)
	echo "num1 $num1 num2 $num2"
	[[ $num1+$num2 -eq 3 ]]

	echo "[$(date)] <<<<<< finish test_last_bound >>>>>>"
}

function run() {
	test_last_bound
	test_config_name             # TICASE-915, 916, 954, 955
	test_join_masters_and_worker # TICASE-928, 930, 931, 961, 932, 957
	test_standalone_running      # TICASE-929, 959, 960, 967, 977, 980, 983
}

cleanup_data $ha_test
cleanup_data $ha_test2
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
