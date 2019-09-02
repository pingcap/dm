#!/bin/bash

set -eu

TEST_DIR=/tmp/dm_test
CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

function run() {
    script=$1
    echo "Running test $script..."
    TEST_DIR="$TEST_DIR" \
    PATH="tests/_utils:$PATH" \
    TEST_NAME="$(basename "$(dirname "$script")")" \
    bash +x "$script"
}

function run2() {
    if [ $1 == "all" ];then 
        $CUR/run.sh
    else
        $CUR/run.sh $1
    fi
}

if [ "$#" -ge 1 ]; then
    test_case=$1
else
    run2 compatibility
    exit 0
fi

echo "run test with current dm-master and previous dm-worker"
cp $PWD/bin/dm-master.test.current $PWD/bin/dm-master.test
cp $PWD/bin/dm-worker.test.previous $PWD/bin/dm-worker.test

run2 "$test_case"

echo "run test with previous dm-master and current dm-worker"
cp $PWD/bin/dm-master.test.previous $PWD/bin/dm-master.test
cp $PWD/bin/dm-worker.test.current $PWD/bin/dm-worker.test

run2 "$test_case"
