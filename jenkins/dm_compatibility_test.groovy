/*
    Run dm campatibility test in Jenkins with String paramaters

    * ghprbActualCommit (by bot)
    * ghprbPullId (by bot)
*/

// prepare all vars
MYSQL_HOST = '127.0.0.1'
MYSQL_PORT = 3306
MYSQL2_PORT = 3307
MYSQL_PSWD = 123456
TEST_CASE = ''
TIDB_BRANCH = 'master'
BUILD_NUMBER = "${env.BUILD_NUMBER}"
PRE_COMMIT = 'tags/v2.0.3'
BREAK_COMPATIBILITY = 'false'

// disable SSL for MySQL (especial for MySQL 8.0) in release-1.0
if ("${ghprbTargetBranch}" == 'release-1.0') {
    PRE_COMMIT = 'tags/v1.0.4'
    MYSQL_ARGS = '--ssl=OFF --log-bin --binlog-format=ROW --enforce-gtid-consistency=ON --gtid-mode=ON --server-id=1 --default-authentication-plugin=mysql_native_password'
}else {
    MYSQL_ARGS = '--log-bin --binlog-format=ROW --enforce-gtid-consistency=ON --gtid-mode=ON --server-id=1 --default-authentication-plugin=mysql_native_password'
}

// parse var by pr comment.
if (ghprbCommentBody =~ /break_compatibility\s*=\s*([^\s\\]+)(\s|\\|$)/) {
    BREAK_COMPATIBILITY = "${m0[0][1]}"
}
if (ghprbCommentBody =~ /tidb\s*=\s*([^\s\\]+)(\s|\\|$)/) {
    TIDB_BRANCH = "${m1[0][1]}"
}
if (ghprbCommentBody =~ /pre_commit\s*=\s*([^\s\\]+)(\s|\\|$)/) {
    PRE_COMMIT = "${m2[0][1]}"
}
if (ghprbCommentBody =~ /case\s*=\s*([^\s\\]+)(\s|\\|$)/) {
    TEST_CASE = "${m3[0][1]}"
}

def print_all_vars() {
    println '================= ALL TEST VARS ================='
    println "[MYSQL_HOST]: ${MYSQL_HOST}"
    println "[MYSQL_PORT]: ${MYSQL_PORT}"
    println "[MYSQL2_PORT]: ${MYSQL2_PORT}"
    println "[MYSQL_PSWD]: ${MYSQL_PSWD}"
    println "[MYSQL_ARGS]: ${MYSQL_ARGS}"

    println "[TEST_CASE]: ${TEST_CASE}"
    println "[TIDB_BRANCH]: ${TIDB_BRANCH}"
    println "[BUILD_NUMBER]: ${BUILD_NUMBER}"
    println "[PRE_COMMIT]: ${PRE_COMMIT}"
    println "[BREAK_COMPATIBILITY]: ${BREAK_COMPATIBILITY}"
}

def checkout_and_stash() {
    node("${GO_BUILD_SLAVE}") {
        println "debug command: \nkubectl -n jenkins-ci exec -ti ${env.NODE_NAME} bash -c golang"
        container('golang') {
            ws = pwd()
            deleteDir()
            dir('/home/jenkins/agent/git/dm') {
                if (sh(returnStatus: true, script: '[ -d .git ] && [ -f Makefile ] && git rev-parse --git-dir > /dev/null 2>&1') != 0) {
                    deleteDir()
                }
                try {
                    checkout changelog: false, poll: false, scm: [$class: 'GitSCM', branches: [[name: 'master']], doGenerateSubmoduleConfigurations: false, extensions: [[$class: 'PruneStaleBranch'], [$class: 'CleanBeforeCheckout']], submoduleCfg: [], userRemoteConfigs: [[credentialsId: 'github-sre-bot-ssh', refspec: '+refs/pull/*:refs/remotes/origin/pr/*', url: 'git@github.com:pingcap/dm.git']]]
                } catch (error) {
                    retry(2) {
                        echo 'checkout failed, retry..'
                        sleep 60
                        if (sh(returnStatus: true, script: '[ -d .git ] && [ -f Makefile ] && git rev-parse --git-dir > /dev/null 2>&1') != 0) {
                            deleteDir()
                        }
                        checkout changelog: false, poll: false, scm: [$class: 'GitSCM', branches: [[name: 'master']], doGenerateSubmoduleConfigurations: false, extensions: [[$class: 'PruneStaleBranch'], [$class: 'CleanBeforeCheckout']], submoduleCfg: [], userRemoteConfigs: [[credentialsId: 'github-sre-bot-ssh', refspec: '+refs/pull/*:refs/remotes/origin/pr/*', url: 'git@github.com:pingcap/dm.git']]]
                    }
                }
            }

            dir('go/src/github.com/pingcap/dm') {
                environment {
                    PATH = "/nfs/cache/gopath/bin:$PATH"
                }
                try {
                    sh """export GOPROXY=https://goproxy.cn
                    archive=dm-go-mod-cache_latest_\$(go version | awk '{ print \$3; }').tar.gz
                    archive_url=${FILE_SERVER_URL}/download/builds/pingcap/dm/cache/\$archive
                    if [ ! -f /tmp/\$archive ]; then
                        curl -sL \$archive_url -o /tmp/\$archive
                        tar --skip-old-files -xf /tmp/\$archive -C / || true
                    fi
                    cp -R /home/jenkins/agent/git/dm/. ./

                    echo "build binary with previous version"
                    git checkout -f ${PRE_COMMIT}
                    PATH=$PATH:/nfs/cache/gopath/bin:/usr/local/go/bin make dm_integration_test_build
                    mv bin/dm-master.test bin/dm-master.test.previous
                    mv bin/dm-worker.test bin/dm-worker.test.previous

                    echo "build binary with current version"
                    git checkout -f ${ghprbActualCommit}
                    PATH=$PATH:/nfs/cache/gopath/bin:/usr/local/go/bin make dm_integration_test_build
                    mv bin/dm-master.test bin/dm-master.test.current
                    mv bin/dm-worker.test bin/dm-worker.test.current
                    """
                }catch (Exception e) {
                    sleep 10000000
                }
            }

            stash includes: 'go/src/github.com/pingcap/dm/**', name: 'dm', useDefaultExcludes: false

            tidb_sha1 = sh(returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/tidb/${TIDB_BRANCH}/sha1").trim()
            sh "curl ${FILE_SERVER_URL}/download/builds/pingcap/tidb/${tidb_sha1}/centos7/tidb-server.tar.gz | tar xz"

            // binlogctl
            sh 'curl http://download.pingcap.org/tidb-enterprise-tools-latest-linux-amd64.tar.gz | tar xz'
            sh 'curl https://download.pingcap.org/tidb-tools-test-linux-amd64.tar.gz | tar xz'
            sh 'mv tidb-tools-test-linux-amd64/bin/sync_diff_inspector bin/'
            //sh "mv tidb-enterprise-tools-latest-linux-amd64/bin/sync_diff_inspector bin/"
            sh 'mv tidb-enterprise-tools-latest-linux-amd64/bin/mydumper bin/'
            sh 'rm -r tidb-enterprise-tools-latest-linux-amd64 || true'

            // use a new version of gh-ost to overwrite the one in container("golang") (1.0.47 --> 1.1.0)
            sh 'curl -L https://github.com/github/gh-ost/releases/download/v1.1.0/gh-ost-binary-linux-20200828140552.tar.gz | tar xz'
            sh 'mv gh-ost bin/'
            stash includes: 'bin/**', name: 'binaries'
        }
    }
}

def run_single_compatibility_test(String case_name) {
    label = "test-${UUID.randomUUID()}"
    podTemplate(label: label,
                nodeSelector: 'role_type=slave',
                namespace: 'jenkins-ci',
                containers: [
                        containerTemplate(
                            name: 'golang', alwaysPullImage: true,
                            image: 'hub.pingcap.net/jenkins/centos7_golang-1.13:cached', ttyEnabled: true,
                            resourceRequestCpu: '2000m', resourceRequestMemory: '4Gi',
                            command: 'cat'),
                        containerTemplate(
                            name: 'mysql1', alwaysPullImage: false,
                            image: 'hub.pingcap.net/jenkins/mysql:5.7',ttyEnabled: true,
                            resourceRequestCpu: '1000m', resourceRequestMemory: '1Gi',
                            envVars: [
                                envVar(key: 'MYSQL_ROOT_PASSWORD', value: "${MYSQL_PSWD}"),
                            ],
                            args: "${MYSQL_ARGS}"),
                        // mysql 8.0
                        containerTemplate(
                            name: 'mysql2', alwaysPullImage: false,
                            image: 'hub.pingcap.net/zhangxuecheng/mysql:8.0.21',ttyEnabled: true,
                            resourceRequestCpu: '1000m', resourceRequestMemory: '1Gi',
                            envVars: [
                                envVar(key: 'MYSQL_ROOT_PASSWORD', value: "${MYSQL_PSWD}"),
                                envVar(key: 'MYSQL_TCP_PORT', value: "${MYSQL2_PORT}")
                            ],
                            args: "${MYSQL_ARGS}")
                        ],
                volumes:[emptyDirVolume(mountPath: '/tmp', memory: true),
                            emptyDirVolume(mountPath: '/home/jenkins', memory: true)]) {
                node(label) {
                    println "${NODE_NAME}"
                    println "debug command: \nkubectl -n jenkins-ci exec -ti ${env.NODE_NAME} bash -c golang"
                    container('golang') {
                        ws = pwd()
                        deleteDir()
                        unstash 'dm'
                        unstash 'binaries'
                        dir('go/src/github.com/pingcap/dm') {
                            try {
                        sh"""
                                # use a new version of gh-ost to overwrite the one in container("golang") (1.0.47 --> 1.1.0)
                                export PATH=bin:$PATH

                                rm -rf /tmp/dm_test
                                mkdir -p /tmp/dm_test
                                mkdir -p bin
                                mv ${ws}/bin/* bin

                                export MYSQL_HOST1=${MYSQL_HOST}
                                export MYSQL_PORT1=${MYSQL_PORT}
                                export MYSQL_HOST2=${MYSQL_HOST}
                                export MYSQL_PORT2=${MYSQL2_PORT}

                                # wait for mysql container ready.
                                set +e && for i in {1..90}; do mysqladmin ping -h127.0.0.1 -P 3306 -p123456 -uroot --silent; if [ \$? -eq 0 ]; then set -e; break; else if [ \$i -eq 90 ]; then set -e; exit 2; fi; sleep 2; fi; done
                                set +e && for i in {1..90}; do mysqladmin ping -h127.0.0.1 -P 3307 -p123456 -uroot --silent; if [ \$? -eq 0 ]; then set -e; break; else if [ \$i -eq 90 ]; then set -e; exit 2; fi; sleep 2; fi; done
                                # run test
                                # export GOPATH=\$GOPATH:${ws}/go
                                make compatibility_test CASE="${case_name}"
                                """
                            }catch (Exception e) {
                                sh """
                                    echo "${case_name} test faild print all log..."
                                    for log in `ls /tmp/dm_test/*/*/log/*.log`; do
                                        echo "-----------------\$log begin-----------------"
                                        cat "\$log"
                                        echo "-----------------\$log end-----------------"
                                    done
                                    """
                                sleep 1000000
                                throw e
                            }
                        }
                    stash includes: 'go/src/github.com/pingcap/dm/cov_dir/**', name: "integration-cov-${case_name}"
                    }
                }
                            }
}

pipeline {
    agent any

    stages {
        stage('Check Code and build') {
            steps {
                print_all_vars()

                script {
                    if ("${BREAK_COMPATIBILITY}"  == 'true') {
                        println 'do not neet run tets this time'
                        currentBuild.result = 'SUCCESS'
                        return
                    }
                }
                checkout_and_stash()
            }
        }

        stage('Run Compatibility test') {
            steps {
                script {
                    run_single_compatibility_test("${TEST_CASE}")
                }
            }
        }

        stage('Print Summary') {
            steps {
                script {
                    def duration = ((System.currentTimeMillis() - currentBuild.startTimeInMillis) / 1000 / 60).setScale(2, BigDecimal.ROUND_HALF_UP)
                    println "all test succeed time=${duration}"
                }
            }
        }
    }
}
