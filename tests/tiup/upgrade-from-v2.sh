#!/bin/bash

set -eu

CLUSTER_NAME="dm-v2"

PRE_VER=$1
CUR_VER=$2

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
PATH=$CUR/../_utils:$PATH # for sync_diff_inspector

source $CUR/lib.sh

function deploy_previous_v2() {
    # install TiUP-DM
    curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh
    source /root/.profile
    tiup install dm

    tiup install dmctl:$PRE_VER

    tiup dm deploy --yes $CLUSTER_NAME $PRE_VER $CUR/conf/topo.yaml
    tiup dm start --yes $CLUSTER_NAME
}

function migrate_in_previous_v2() {
    exec_full_stage

    # v2.0.0 doesn't support relay log
    if [[ "$PRE_VER" == "v2.0.0" ]]; then
        sed -i "s/enable-relay: true/enable-relay: false/g" $CUR/conf/source2.yaml
    fi

    tiup dmctl:$PRE_VER --master-addr=master1:8261 operate-source create $CUR/conf/source1.yaml
    tiup dmctl:$PRE_VER --master-addr=master1:8261 operate-source create $CUR/conf/source2.yaml

    tiup dmctl:$PRE_VER --master-addr=master1:8261 start-task $CUR/conf/task.yaml
    tiup dmctl:$PRE_VER --master-addr=master1:8261 start-task $CUR/conf/task_optimistic.yaml

    exec_incremental_stage1

    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
}

function upgrade_to_current_v2() {
    if [[ "$CUR_VER" == "nightly" && "$ref" == "refs/pull"* ]]; then
        patch_nightly_with_tiup_mirror
    fi
    tiup update dmctl:$CUR_VER
    tiup dm upgrade --yes $CLUSTER_NAME $CUR_VER
}

function migrate_in_v2 {
    exec_incremental_stage2

    echo "check sources"
    run_dmctl_with_retry $CUR_VER "operate-source show" "mysql-replica-01" 1 "mysql-replica-02" 1
    echo "check workers"
    run_dmctl_with_retry $CUR_VER "list-member --worker" "\"stage\": \"bound\"" 2

    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
    check_sync_diff $WORK_DIR $CUR/conf/diff_config_optimistic.toml

    echo "check locks"
    run_dmctl_with_retry $CUR_VER "show-ddl-locks" "no DDL lock exists" 1

    tiup dmctl:$CUR_VER --master-addr=master1:8261 stop-task $TASK_NAME
}

function destroy_v2_by_tiup() {
    tiup dm destroy --yes $CLUSTER_NAME
}

function test() {
    install_sync_diff

    deploy_previous_v2

    migrate_in_previous_v2

    upgrade_to_current_v2

    migrate_in_v2

    destroy_v2_by_tiup
}

test
