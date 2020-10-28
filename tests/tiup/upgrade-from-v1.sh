#!/bin/bash

set -eu

export TEST_DIR=/tmp/dm_test
export TEST_NAME="upgrade-via-tiup"

WORK_DIR=$TEST_DIR/$TEST_NAME
mkdir -p $WORK_DIR

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
PATH=$CUR/../_utils:$PATH # for sync_diff_inspector

TASK_NAME="upgrade_via_tiup"
CLUSTER_NAME="dm-v1"
DM_V2_VER="nightly"

DB1=sharding1
DB2=sharding2
TBL1=t1
TBL2=t2
TBL3=t3

function exec_sql() {
    echo $3 | mysql -h $1 -P $2
}

function install_sync_diff() {
    curl http://download.pingcap.org/tidb-enterprise-tools-latest-linux-amd64.tar.gz | tar xz
    mv tidb-enterprise-tools-latest-linux-amd64/bin/sync_diff_inspector bin/
}

# ref https://docs.pingcap.com/zh/tidb-data-migration/stable/deploy-a-dm-cluster-using-ansible
# script following the docs to deploy dm 1.0.6 using ./ansible_data/inventory.ini
function deploy_v1_by_ansible() {
    # step 1
    apt-get -y install git curl sshpass python-pip sudo

    # step 2
    useradd -m -d /home/tidb tidb
    echo "tidb:tidb" | chpasswd
    echo "tidb ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

    # use the same key from root instead of create one.
    mkdir -p /home/tidb/.ssh
    cp ~/.ssh/* /home/tidb/.ssh/
    chown -R tidb:tidb /home/tidb/.ssh/

    # step 3
su tidb <<EOF
    cd /home/tidb
    wget https://download.pingcap.org/dm-ansible-v1.0.6.tar.gz
EOF

    # step 4
su tidb <<EOF
    cd /home/tidb
    tar -xzvf dm-ansible-v1.0.6.tar.gz &&
        mv dm-ansible-v1.0.6 dm-ansible &&
        cd /home/tidb/dm-ansible &&
        sudo pip install -r ./requirements.txt
    ansible --version
EOF

    # step 5
    cp $CUR/ansible_data/hosts.ini /home/tidb/dm-ansible/
    cp $CUR/ansible_data/inventory.ini /home/tidb/dm-ansible/

    # not following the docs, use root and without password to run it
    cd /home/tidb/dm-ansible
    sudo ansible-playbook -i hosts.ini create_users.yml -u root
    cd $CUR/../..

    #step 6
su tidb <<EOF
    cd /home/tidb/dm-ansible
    ansible-playbook local_prepare.yml
EOF

    # skip 7,8

    # step 9
su tidb <<EOF
    cd /home/tidb/dm-ansible
    ansible -i inventory.ini all -m shell -a 'whoami'
    ansible -i inventory.ini all -m shell -a 'whoami' -b
    ansible-playbook deploy.yml
    ansible-playbook start.yml
EOF

}

function stop_v1_by_ansible() {
    su tidb <<EOF
    cd /home/tidb/dm-ansible
    ansible-playbook stop.yml
EOF
}

function migrate_in_v1 {
    # drop previous data
    exec_sql mysql1 3306 "DROP DATABASE IF EXISTS $DB1;" 
    exec_sql mysql2 3306 "DROP DATABASE IF EXISTS $DB2;" 
    exec_sql tidb 4000 "DROP DATABASE IF EXISTS db_target;"
    exec_sql tidb 4000 "DROP DATABASE IF EXISTS dm_meta;"

    # # prepare full data
    exec_sql mysql1 3306 "CREATE DATABASE $DB1;"
    exec_sql mysql2 3306 "CREATE DATABASE $DB2;"
    exec_sql mysql1 3306 "CREATE TABLE $DB1.$TBL1 (c1 INT PRIMARY KEY, c2 TEXT);"
    exec_sql mysql1 3306 "CREATE TABLE $DB1.$TBL2 (c1 INT PRIMARY KEY, c2 TEXT);"
    exec_sql mysql2 3306 "CREATE TABLE $DB2.$TBL2 (c1 INT PRIMARY KEY, c2 TEXT);"
    exec_sql mysql2 3306 "CREATE TABLE $DB2.$TBL3 (c1 INT PRIMARY KEY, c2 TEXT);"

    exec_sql mysql1 3306 "INSERT INTO $DB1.$TBL1 (c1, c2) VALUES (1, '1');"
    exec_sql mysql1 3306 "INSERT INTO $DB1.$TBL2 (c1, c2) VALUES (2, '2');"
    exec_sql mysql2 3306 "INSERT INTO $DB2.$TBL2 (c1, c2) VALUES (11, '11');"
    exec_sql mysql2 3306 "INSERT INTO $DB2.$TBL3 (c1, c2) VALUES (12, '12');"

    # # start v1 task
    /home/tidb/dm-ansible/dmctl/dmctl --master-addr=master1:8261 start-task $CUR/conf/task.yaml

    # prepare incremental data
    exec_sql mysql1 3306 "INSERT INTO $DB1.$TBL1 (c1, c2) VALUES (101, '101');"
    exec_sql mysql1 3306 "INSERT INTO $DB1.$TBL2 (c1, c2) VALUES (102, '102');"
    exec_sql mysql2 3306 "INSERT INTO $DB2.$TBL2 (c1, c2) VALUES (111, '111');"
    exec_sql mysql2 3306 "INSERT INTO $DB2.$TBL3 (c1, c2) VALUES (112, '112');"

    # check data
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
}

# ref https://docs.pingcap.com/zh/tidb-data-migration/v2.0/deploy-a-dm-cluster-using-tiup
function import_to_v2_by_tiup() {
    # install TiUP-DM
    curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh
    source /root/.profile
    tiup install dm

    # import from v1
    # TODO: update `--cluster-version` to the target version later.
    tiup install dmctl:$DM_V2_VER
    tiup dm import --yes --dir=/home/tidb/dm-ansible --cluster-version $DM_V2_VER
    tiup dm start --yes $CLUSTER_NAME
}

function migrate_in_v2 {
    # prepare incremental data
    exec_sql mysql1 3306 "INSERT INTO $DB1.$TBL1 (c1, c2) VALUES (201, '201');"
    exec_sql mysql1 3306 "INSERT INTO $DB1.$TBL2 (c1, c2) VALUES (202, '202');"
    exec_sql mysql2 3306 "INSERT INTO $DB2.$TBL2 (c1, c2) VALUES (211, '211');"
    exec_sql mysql2 3306 "INSERT INTO $DB2.$TBL3 (c1, c2) VALUES (212, '212');"

    # check data
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    # stop v2 task
    tiup dmctl:$DM_V2_VER --master-addr=master1:8261 stop-task $TASK_NAME
}

function destroy_v2_by_tiup() {
    tiup dm destroy --yes $CLUSTER_NAME
}

function test() {
    install_sync_diff

    deploy_v1_by_ansible

    migrate_in_v1

    stop_v1_by_ansible

    import_to_v2_by_tiup

    migrate_in_v2

    destroy_v2_by_tiup
}

test
