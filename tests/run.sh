#!/bin/bash

set -eu

TEST_DIR=/tmp/dm_test

stop_services() {
    # killall -9 tidb-server || true
    echo "..."
}

check_mysql() {
    host=$1
    port=$2
    while ! mysql -u root -h ${host} -P ${port} -e 'select version();'; do
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

    echo "Starting TiDB..."
    bin/tidb-server \
        -P 4000 \
        --store mocktikv \
        --log-file "$TEST_DIR/tidb.log" &

    echo "Verifying TiDB is started..."
    i=0
    while ! mysql -uroot -h127.0.0.1 -P4000 --default-character-set utf8 -e 'select * from mysql.tidb;'; do
        i=$((i+1))
        if [ "$i" -gt 10 ]; then
            echo 'Failed to start TiDB'
            exit 1
        fi
        sleep 2
    done

    i=0
    MYSQL_HOST1=${MYSQL_HOST1:-127.0.0.1}
    MYSQL_PORT1=${MYSQL_PORT1:-3306}
    MYSQL_HOST2=${MYSQL_HOST2:-127.0.0.1}
    MYSQL_PORT2=${MYSQL_PORT2:-3307}
    check_mysql $MYSQL_HOST1 $MYSQL_PORT1
    check_mysql $MYSQL_HOST2 $MYSQL_PORT2
}

if [ "$#" -ge 1 ]; then
    test_case=$1
    if [ ! -d "tests/$test_case" ]; then
        echo "test case $test_case not found"
        exit 1
    fi
else
    test_case="*"
fi

trap stop_services EXIT
start_services

for script in tests/$test_case/run.sh; do
    echo "Running test $script..."
    TEST_DIR="$TEST_DIR" \
    PATH="tests/_utils:$PATH" \
    TEST_NAME="$(basename "$(dirname "$script")")" \
    bash +x "$script"
done
