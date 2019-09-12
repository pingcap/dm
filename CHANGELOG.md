# DM Changelog

All notable changes to this project will be documented in this file.

## [1.0.1] 2019-09-10

#### Bug fixes

- Fix a bug that may cause database connection re-establish too frequent [#280](https://github.com/pingcap/dm/pull/280)
- Fix a potential panic bug when we query-status during subtask unit transforming between different units [#274](https://github.com/pingcap/dm/pull/274)

## [1.0.0] 2019-09-06

### v1.0.0 What's New

#### Improvements

- Add auto recovery framework
- Make task retryable when encounters a database driver error
- Make task retryable when encounters network issue
- Improve the DDL compatibility in DM

#### Bug fixes

- Fix the bug that has a risk of data loss when the upstream database connection is abnormal 

#### Action required

- When upgrading from a previous version, please note that you must upgrade all DM components (dmctl/DM-master/DM-worker) together

### Detailed Bug Fixes and Changes

- Retry for upstream bad connection [#265](https://github.com/pingcap/dm/pull/265)
- Added some retryable errors in underlying database implementation [#256](https://github.com/pingcap/dm/pull/256)
- Added task auto recovery framework [#246](https://github.com/pingcap/dm/pull/246)
- Unify DB operation into one pkg for load and binlog replication [#236](https://github.com/pingcap/dm/pull/236)
- Fixed a bug in task handler that may cause dm-worker panic [#225](https://github.com/pingcap/dm/pull/225)
- Refactor DM error system [#216](https://github.com/pingcap/dm/pull/216)
- Add strictly config verification in dm-master, dm-worker and task [#212](https://github.com/pingcap/dm/pull/212)
- Limit DM error and log message length [#257](https://github.com/pingcap/dm/pull/257)
- Support case insensitive binlog event filter in binlog replication [#188](https://github.com/pingcap/dm/pull/188)
- Refactor DM log with pingcap/log [#195](https://github.com/pingcap/dm/pull/195)
- Use `INSERT` instead of `REPLACE` under non safe-mode [#199](https://github.com/pingcap/dm/pull/199)


## [1.0.0-rc.1] 2019-07-01

- Remove the restriction of "the next shard DDL statement cannot be executed unless the current shard DDL operation is completely finished in shard merge scene for binlog replication" [#177](https://github.com/pingcap/dm/pull/177)
- Support retry task on the `invalid connection` error for binlog replication [#66](https://github.com/pingcap/dm/pull/66)
- Support generating `-schema-create.sql` files automatically for full migration [#186](https://github.com/pingcap/dm/pull/186)
- Use [TiDB SQL Parser](https://github.com/pingcap/parser) to parse and restore DDL statements in binlog query events, and remove the `use db` statement when replicating to downstream [#54](https://github.com/pingcap/dm/pull/54)
- Support migrating tables with generated column for full migration and binlog replication [#42](https://github.com/pingcap/dm/pull/42) [#60](https://github.com/pingcap/dm/pull/60)
- Support appending the task name as a suffix to the dumped data directory (`dir` in the task configuration file) for full migration [#100](https://github.com/pingcap/dm/pull/100)
- Support resuming tasks automatically after the DM-worker process restarted [#88](https://github.com/pingcap/dm/pull/88) [#116](https://github.com/pingcap/dm/pull/116)
- Support skipping pre-check items when starting tasks [#65](https://github.com/pingcap/dm/pull/65)
- Support case-insensitive in binlog event type of Binlog Event Filter rules [#188](https://github.com/pingcap/dm/pull/188)
- Optimize relay log mechanism to improve compatibility with upstream MySQL/MariaDB [#92](https://github.com/pingcap/dm/pull/92) [#108](https://github.com/pingcap/dm/pull/108) [#117](https://github.com/pingcap/dm/pull/117) [#140](https://github.com/pingcap/dm/pull/140) [#171](https://github.com/pingcap/dm/pull/171)
- Optimize RPC framework to limit the concurrency between DM-master and DM-worker [#157](https://github.com/pingcap/dm/pull/157)
- Redirect the log of mydumper into the log of DM-worker [#93](https://github.com/pingcap/dm/pull/93)
- Support using `--print-sample-config` command flag to show the sample config of DM-worker and DM-master [#28](https://github.com/pingcap/dm/pull/28)
- Support showing the status of sharding DDL replication in the Grafana dashboard and optimize the dashboard [#96](https://github.com/pingcap/dm/pull/96) [#101](https://github.com/pingcap/dm/pull/101) [#120](https://github.com/pingcap/dm/pull/120)
- Support setting the `max_allowed_packet` parameter for the database connection to the upstream and downstream [#99](https://github.com/pingcap/dm/pull/99) 
- Support using `unsafe_cleanup` in DM Ansible to clean components of DM cluster [#128](https://github.com/pingcap/dm/pull/128)
- Fix flushing checkpoint wrongly when existing multiple sharding groups [#124](https://github.com/pingcap/dm/pull/124)
- Fix the wrong progress status when loading dumped files [#89](https://github.com/pingcap/dm/pull/89)
- Greatly improve the test to ensure correctness
- Fix many other bugs that don't affect correctness

## [1.0.0-alpha] 2019-01-18

- Support the full data migration and the incremental data migration from MySQL/MariaDB into TiDB
- Support running multi independent synchronization tasks concurrently on one DM-worker instance
- Support synchronizing a certain table of the upstream MySQL/MariaDB instance to the specified table in TiDB by table routing
- Support only synchronizing or filtering all operations of some databases or some tables by black and white lists
- Support synchronizing or filtering some of the binlog events by binlog event filter
- Support modifying auto-increment primary key fields to resolve the conflicts for shard tables by column mapping
- Support merging the original shard instances and tables into TiDB but with some restrictions
- Support synchronizing data definition changed by online DDL tools (including pt-osc and gh-ost)
- Support handling synchronization trouble caused by DDL which is not supported by TiDB
