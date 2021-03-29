#!/bin/bash

set -eu

TEST_DIR=/tmp/dm_test
CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/_utils/env_variables

stop_services() {
    # killall -9 tidb-server || true
    echo "..."
}

check_mysql() {
    host=$1
    port=$2
    password=$3
    while ! mysql -u root -h ${host} -P ${port} -p${password} -e 'select version();'; do
        i=$((i+1))
        if [ "$i" -gt 10 ]; then
            echo "wait for mysql ${host}:${port} timeout"
            exit 1
        fi
        sleep 1
    done
}

start_services() {
    stop_services

    mkdir -p "$TEST_DIR"
    rm -rf "$TEST_DIR/*.log"

    $CUR/_utils/run_tidb_server $TIDB_PORT $TIDB_PASSWORD

    i=0

    check_mysql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
    check_mysql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
}

if [ "$#" -ge 1 ]; then
    test_case="$@"
else
    test_case="*"
fi

should_run=0
if [ "$test_case" == "*" ]; then
    should_run=1
elif [ "$test_case" == "compatibility" ]; then
    should_run=1
elif [ "$test_case" == "others" ]; then
    test_case=$(cat $CUR/others_integration.txt)
    should_run=1
else
    exist_case=""
    for one_case in $test_case; do
        if [ ! -d "tests/$one_case" ]; then
            echo $one_case "not exist"
        else
            exist_case="$exist_case $one_case"
            should_run=1
        fi
    done
    test_case=$exist_case
fi

if [ $should_run -eq 0 ]; then
    exit 0
fi

trap stop_services EXIT
start_services

function run() {
    script=$1
    echo "Running test $script..."
    # run in verbose mode?
    echo "Verbose mode = $VERBOSE"
    if $VERBOSE ; then
        TEST_DIR="$TEST_DIR" \
        PATH="tests/_utils:$PATH" \
        TEST_NAME="$(basename "$(dirname "$script")")" \
        bash -x "$script"
    else
        TEST_DIR="$TEST_DIR" \
        PATH="tests/_utils:$PATH" \
        TEST_NAME="$(basename "$(dirname "$script")")" \
        bash +x "$script"
    fi
}

if [ "$test_case" == "*" ]; then
    for script in $CUR/$test_case/run.sh; do
        run $script
    done
elif [ "$test_case" == "compatibility" ]; then
    script="$CUR/compatibility/start.sh"
    run $script
else
    for name in $test_case; do
        script="$CUR/$name/run.sh"
        echo "run $script"
        run $script
    done
fi
