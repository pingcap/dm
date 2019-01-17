#!groovy

node {
    def TIDB_BINLOG_BRANCH = "master"
    def TIDB_TOOLS_BRANCH = "master"
    def TIDB_BRANCH = "master"
   
    fileLoader.withGit('git@github.com:pingcap/SRE.git', 'master', 'github-iamxy-ssh', '') {
        fileLoader.load('jenkins/ci/pingcap_dm_branch.groovy').call(TIDB_BINLOG_BRANCH, TIDB_TOOLS_BRANCH, TIDB_BRANCH)
    }
}
