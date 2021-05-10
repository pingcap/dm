def TIDB_BRANCH = 'master'
def MYSQL_ARGS = '--log-bin --binlog-format=ROW --enforce-gtid-consistency=ON --gtid-mode=ON --server-id=1 --default-authentication-plugin=mysql_native_password'

def build_dm_bin() {
    node("${GO_BUILD_SLAVE}") {
        container('golang') {
            def ws = pwd()
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
                        sleep 5
                        if (sh(returnStatus: true, script: '[ -d .git ] && [ -f Makefile ] && git rev-parse --git-dir > /dev/null 2>&1') != 0) {
                            deleteDir()
                        }
                        checkout changelog: false, poll: false, scm: [$class: 'GitSCM', branches: [[name: 'master']], doGenerateSubmoduleConfigurations: false, extensions: [[$class: 'PruneStaleBranch'], [$class: 'CleanBeforeCheckout']], submoduleCfg: [], userRemoteConfigs: [[credentialsId: 'github-sre-bot-ssh', refspec: '+refs/pull/*:refs/remotes/origin/pr/*', url: 'git@github.com:pingcap/dm.git']]]
                    }
                }
            }
            dir('go/src/github.com/pingcap/dm') {
                sh """
                        # export GOPROXY=https://goproxy.cn
                        archive=dm-go-mod-cache_latest_\$(go version | awk '{ print \$3; }').tar.gz
                        archive_url=${FILE_SERVER_URL}/download/builds/pingcap/dm/cache/\$archive
                        if [ ! -f /tmp/\$archive ]; then
                            curl -sL \$archive_url -o /tmp/\$archive
                            tar --skip-old-files -xf /tmp/\$archive -C / || true
                        fi
                        cp -R /home/jenkins/agent/git/dm/. ./
                        git checkout -f ${ghprbActualCommit}
                        # TODO(ehco): check this if not need make dm_integration_test_build
                    """
            }

            stash includes: 'go/src/github.com/pingcap/dm/**', name: 'dm', useDefaultExcludes: false

            def tidb_sha1 = sh(returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/tidb/${TIDB_BRANCH}/sha1").trim()
            sh "curl ${FILE_SERVER_URL}/download/builds/pingcap/tidb/${tidb_sha1}/centos7/tidb-server.tar.gz | tar xz"

            // binlogctl
            sh 'curl https://download.pingcap.org/tidb-enterprise-tools-nightly-linux-amd64.tar.gz | tar xz'

            // sh "curl http://download.pingcap.org/tidb-enterprise-tools-latest-linux-amd64.tar.gz | tar xz"
            // sh "curl https://download.pingcap.org/tidb-tools-test-linux-amd64.tar.gz | tar xz"
            // sh "mv tidb-tools-test-linux-amd64/bin/sync_diff_inspector bin/"

            sh 'mv tidb-enterprise-tools-nightly-linux-amd64/bin/sync_diff_inspector bin/'
            sh 'mv tidb-enterprise-tools-nightly-linux-amd64/bin/mydumper bin/'
            sh 'rm -r tidb-enterprise-tools-nightly-linux-amd64 || true'

            // sh "mv tidb-enterprise-tools-latest-linux-amd64/bin/sync_diff_inspector bin/"
            // sh "mv tidb-enterprise-tools-latest-linux-amd64/bin/mydumper bin/"
            // sh "rm -r tidb-enterprise-tools-latest-linux-amd64 || true"

            // use a new version of gh-ost to overwrite the one in container("golang") (1.0.47 --> 1.1.0)
            sh 'curl -L https://github.com/github/gh-ost/releases/download/v1.1.0/gh-ost-binary-linux-20200828140552.tar.gz | tar xz'
            sh 'mv gh-ost bin/'

            stash includes: 'bin/**', name: 'binaries'
            println "debug command:\nkubectl -n jenkins-ci exec -ti ${env.NODE_NAME} bash"
        }
    }
}

def run_single_unit_test(case_name, mysql_args) {
    def label = "dm-test-${case_name}-${UUID.randomUUID()}"
    podTemplate(label: label,
                        nodeSelector: 'role_type=slave',
                        containers: [
                        containerTemplate(name: 'golang', alwaysPullImage: true, image: 'hub.pingcap.net/jenkins/centos7_golang-1.13:cached', ttyEnabled: true,
                        resourceRequestCpu: '2000m', resourceRequestMemory: '4Gi',
                        command: 'cat'),
                        containerTemplate(
                            name: 'mysql',
                            image: 'hub.pingcap.net/jenkins/mysql:5.7',
                            ttyEnabled: true,
                            alwaysPullImage: false,
                            resourceRequestCpu: '2000m', resourceRequestMemory: '4Gi',
                            envVars: [
                                envVar(key: 'MYSQL_ROOT_PASSWORD', value: '123456'),
                            ],
                            args: "${mysql_args}",
                        )]) {
        node(label) {
            println "${NODE_NAME}"
            container('golang') {
                def ws = pwd()
                deleteDir()
                unstash 'dm'
                dir('go/src/github.com/pingcap/dm') {
                    sh """
                                        rm -rf /tmp/dm_test
                                        mkdir -p /tmp/dm_test
                                        set +e && for i in {1..90}; do mysqladmin ping -h127.0.0.1 -P 3306 -uroot --silent; if [ \$? -eq 0 ]; then set -e; break; else if [ \$i -eq 90 ]; then set -e; exit 2; fi; sleep 2; fi; done
                                        export MYSQL_HOST=127.0.0.1
                                        export MYSQL_PORT=3306
                                        export MYSQL_PSWD=123456

                                        if [ "${ghprbSourceBranch}" == "fixTestDisbaled111111" ]; then
                                            echo "run stress"
                                            sudo rm -f /etc/yum.repos.d/ius*
                                            sudo yum makecache
                                            sudo yum install -y stress
                                            ( sleep 30 && stress --cpu 2 --timeout 120 ) &
                                        else
                                            echo "skip stress"
                                        fi

                                        GOPATH=\$GOPATH:${ws}/go make unit_test_${case_name}
                                        rm -rf cov_dir
                                        mkdir -p cov_dir
                                        ls /tmp/dm_test
                                        cp /tmp/dm_test/cov*out cov_dir
                                        """
                }
                stash includes: 'go/src/github.com/pingcap/dm/cov_dir/**', name: "unit-cov-${case_name}"
                println "debug command:\nkubectl -n jenkins-ci exec -ti ${env.NODE_NAME} bash"
            }
        }
                        }
}

def run_make_check() {
    node("${GO_TEST_SLAVE}") {
        container('golang') {
            sh '''
                        rm -rf /tmp/dm_test
                        mkdir -p /tmp/dm_test
                        '''
            def ws = pwd()
            deleteDir()
            unstash 'dm'

            dir('go/src/github.com/pingcap/dm') {
                container('golang') {
                    timeout(30) {
                        sh """
                                GOPATH=\$GOPATH:${ws}/go PATH=\$GOPATH/bin:${ws}/go/bin:\$PATH make check
                                """
                    }
                }
            }
        }
    }
}

pipeline {
    agent any

    stages {
        stage('Prepare And Build') {
            steps {
                build_dm_bin()
            }
        }

        stage('Check DM') {
            steps {
                run_make_check()
            }
        }

        stage('Parallel Run Tests') {
            failFast true
            parallel {
                stage('relay') {
                    steps {
                        run_single_unit_test('relay', "${MYSQL_ARGS}")
                    }
                }
                stage('syncer') {
                    steps {
                        run_single_unit_test('syncer', "${MYSQL_ARGS}")
                    }
                }
                stage('pkg_binlog') {
                    steps {
                        run_single_unit_test('pkg_binlog', "${MYSQL_ARGS}")
                    }
                }
                stage('others') {
                    steps {
                        run_single_unit_test('others', "${MYSQL_ARGS}")
                    }
                }
            }
        }

        stage('Print Summary') {
            steps {
                script {
                    def duration = ((System.currentTimeMillis() - currentBuild.startTimeInMillis) / 1000 / 60).setScale(2, BigDecimal.ROUND_HALF_UP)
                }
                println "all test succeed time=${duration}"
            }
        }
    }
}
