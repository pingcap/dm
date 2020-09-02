# DM Changelog

All notable changes to this project will be documented in this file.

## [2.0.0-rc.2] 2020-09-01

### Improvements

- Support more AWS Aurora-specific privileges when pre-checking the data migration task [#950](https://github.com/pingcap/dm/pull/950)
- Check whether GTID is enabled for the upstream MySQL/MariaDB when configuring `enable-gtid: true` and creating a data source [#957](https://github.com/pingcap/dm/pull/957)

### Bug fixes

- Fix the `Column count doesn't match value count` error that occurs in the running migration task after automatically upgrading the DM cluster from v1.0.x to v2.0.0-rc [#952](https://github.com/pingcap/dm/pull/952)
- Fix the issue that the DM-worker or DM-master component might not correctly exit [#963](https://github.com/pingcap/dm/pull/963)
- Fix the issue that the `--no-locks` argument does not take effect on the dump processing unit in DM v2.0 [#961](https://github.com/pingcap/dm/pull/961)
- Fix the `field remove-meta not found in type config.TaskConfig` error that occurs when using the task configuration file of the v1.0.x cluster to start the task of a v2.0 cluster [#965](https://github.com/pingcap/dm/pull/965)
- Fix the issue that when the domain name is used as the connection address of each component, the component might not be correctly started [#955](https://github.com/pingcap/dm/pull/955)
- Fix the issue that the connection between the upstream and downstream might not be released after the migration task is stopped [#943](https://github.com/pingcap/dm/pull/943)
- Fix the issue that in the optimistic sharding DDL mode, concurrently executing the DDL statement on multiple sharded tables might block the sharding DDL coordination [#944](https://github.com/pingcap/dm/pull/944)
- Fix the issue that the newly started DM-master might cause the `list-member` to panic [#970](https://github.com/pingcap/dm/pull/970)

### Action required

- When upgrading from a previous version, note that you must upgrade all DM components (dmctl/DM-master/DM-worker) together

## [2.0.0-rc] 2020-08-21

### Improvements

- Support high availability for data migration tasks
- Add an optimistic mode for sharding DDL statements
- Add the `handle-error` command to handle errors during DDL incremental replication
- Add a `workaround` field in the error returned by `query-status` to suggest the error handling method
- Improve the monitoring dashboards and alert rules
- Replace Mydumper with Dumpling as the full export unit
- Support the GTID mode when performing incremental replication to the downstream
- Support TLS connections between upstream and downstream databases, and between DM components
- Support the incremental replication scenarios where the table of the downstream has more columns than that of the upstream
- Add a `--remove-meta` option to the `start-task` command to clean up metadata related to data migration tasks
- Support dropping columns with single-column indices
- Support automatically cleaning up temporary files after a successful full import
- Support checking whether the table to be migrated has a primary key or a unique key before starting a migration task
- Support connectivity check between dmctl and DM-master while starting dmctl
- Support connectivity check for downstream TiDB during the execution of `start-task`/`check-task`
- Support replacing task names with task configuration files for some commands such as `pause-task`
- Support logs in `json` format for DM-master and DM-worker components
- Remove the call stack information and redundant fields in the error message returned by `query-status`
- Improve the binlog position information of the upstream database returned by `query-status`
- Improve the processing of `auto resume` when an error is encountered during the full export

### Bug fixes

- Fix the issue of goroutine leak after executing `stop-task`
- Fix the issue that the task might not be paused after executing `pause-task`
- Fix the issue that the checkpoint might not be saved correctly in the initial stage of incremental replication
- Fix the issue that the `BIT` data type is incorrectly handled during incremental replication

### Action required

- When upgrading from a previous version, note that you must upgrade all DM components (dmctl/DM-master/DM-worker) together

### Detailed Bug Fixes and Changes

- Support high availability for data migration tasks [#473](https://github.com/pingcap/dm/pull/473)
- Add an optimistic mode for sharding DDL statements [#568](https://github.com/pingcap/dm/pull/568)
- Add the `handle-error` command to handle errors during DDL incremental replication [#850](https://github.com/pingcap/dm/pull/850)
- Add a `workaround` field in the error returned by `query-status` to suggest the error handling method [#753](https://github.com/pingcap/dm/pull/753)
- Improve the monitoring dashboards and alert rules [#853](https://github.com/pingcap/dm/pull/853)
- Replace Mydumper with Dumpling as the full export unit [#540](https://github.com/pingcap/dm/pull/540)
- Support the GTID mode when performing incremental replication to the downstream [#521](https://github.com/pingcap/dm/pull/521)
- Support TLS connections between upstream and downstream databases, and between DM components [#569](https://github.com/pingcap/dm/pull/569)
- Support the incremental replication scenarios where the table of the downstream has more columns than that of the upstream [#379](https://github.com/pingcap/dm/pull/379)
- Add a `--remove-meta` option to the `start-task` command to clean up metadata related to data migration tasks [#651](https://github.com/pingcap/dm/pull/651)
- Support dropping columns with single-column indices [#801](https://github.com/pingcap/dm/pull/801)
- Support automatically cleaning up temporary files after a successful full import [#770](https://github.com/pingcap/dm/pull/770)
- Support checking whether the table to be migrated has a primary key or a unique key before starting a migration task [#870](https://github.com/pingcap/dm/pull/870)
- Support connectivity check between dmctl and DM-master while starting dmctl [#786](https://github.com/pingcap/dm/pull/786)
- Support connectivity check for downstream TiDB during the execution of `start-task`/`check-task` [#769](https://github.com/pingcap/dm/pull/769) 
- Support replacing task names with task configuration files for some commands such as `pause-task` [#854](https://github.com/pingcap/dm/pull/854)
- Support logs in `json` format for DM-master and DM-worker components [#808](https://github.com/pingcap/dm/pull/808)
- Remove the call stack information in the error message returned by `query-status` [#746](https://github.com/pingcap/dm/pull/746)
- Remove the redundant fields in the error message returned by `query-status` [#771](https://github.com/pingcap/dm/pull/771)
- Improve the binlog position information of the upstream database returned by `query-status` [#830](https://github.com/pingcap/dm/pull/830)
- Improve the processing of `auto resume` when an error is encountered during the full export [#872](https://github.com/pingcap/dm/pull/872)
- Fix the issue of goroutine leak after executing `stop-task` [#731](https://github.com/pingcap/dm/pull/731)
- Fix the issue that the task might not be paused after executing `pause-task` [#644](https://github.com/pingcap/dm/pull/644)
- Fix the issue that the checkpoint might not be saved correctly in the initial stage of incremental replication [#758](https://github.com/pingcap/dm/pull/758)
- Fix the issue that the `BIT` data type is incorrectly handled during incremental replication [#876](https://github.com/pingcap/dm/pull/876)

## [1.0.6] 2020-06-17

### Improvements

- Support the original plaintext passwords for upstream and downstream databases
- Support configuring session variables for DM’s connections to upstream and downstream databases
- Remove the call stack information in some error messages returned by the `query-status` command when the data migration task encounters an exception 
- Filter out the items that pass the precheck from the message returned when the precheck of the data migration task fails

### Bug fixes

- Fix the issue that the data migration task is not automatically paused and the error cannot be identified by executing the `query-status` command if an error occurs when the load unit creates a table 
- Fix possible DM-worker panics when data migration tasks run simultaneously 
- Fix the issue that the existing data migration task cannot be automatically restarted when the DM-worker process is restarted if the `enable-heartbeat` parameter of the task is set to `true` 
- Fix the issue that the shard DDL conflict error may not be returned after the task is resumed 
- Fix the issue that the `replicate lag` information is displayed incorrectly for an initial period of time when the `enable-heartbeat` parameter of  the data migration task is set to `true` 
- Fix the issue that `replicate lag` cannot be calculated using the heartbeat information when `lower_case_table_names` is set to `1` in the upstream database 
- Disable the meaningless auto-resume tasks triggered by the `unsupported collation` error during data migration

### Action required

- When upgrading from a previous version, note that you must upgrade all DM components (dmctl/DM-master/DM-worker) together

### Detailed Bug Fixes and Changes

- Support the original plaintext passwords for upstream and downstream databases [#676](https://github.com/pingcap/dm/pull/676)
- Support configuring session variables for DM’s connections to upstream and downstream databases [#692](https://github.com/pingcap/dm/pull/692)
- Remove the call stack information in some error messages returned by the `query-status` command when the data migration task encounters an exception [#733](https://github.com/pingcap/dm/pull/733) [#747](https://github.com/pingcap/dm/pull/747)
- Filter out the items that pass the precheck from the message returned when the precheck of the data migration task fails [#730](https://github.com/pingcap/dm/pull/730)
- Fix the issue that the data migration task is not automatically paused and the error cannot be identified by executing the `query-status` command if an error occurs when the load unit creates a table [#747](https://github.com/pingcap/dm/pull/747)
- Fix possible DM-worker panics when data migration tasks run simultaneously [#710](https://github.com/pingcap/dm/pull/710)
- Fix the issue that the existing data migration task cannot be automatically restarted when the DM-worker process is restarted if the `enable-heartbeat` parameter of the task is set to `true` [#739](https://github.com/pingcap/dm/pull/739)
- Fix the issue that the shard DDL conflict error may not be returned after the task is resumed [#739](https://github.com/pingcap/dm/pull/739) [#742](https://github.com/pingcap/dm/pull/742)
- Fix the issue that the `replicate lag` information is displayed incorrectly for an initial period of time when the `enable-heartbeat` parameter of  the data migration task is set to `true` [#704](https://github.com/pingcap/dm/pull/704)
- Fix the issue that `replicate lag` cannot be calculated using the heartbeat information when `lower_case_table_names` is set to `1` in the upstream database [#704](https://github.com/pingcap/dm/pull/704)
- Disable the meaningless auto-resume tasks triggered by the `unsupported collation` error during data migration [#735](https://github.com/pingcap/dm/pull/735)
- Optimize some logs [#660](https://github.com/pingcap/dm/pull/660) [#724](https://github.com/pingcap/dm/pull/724) [#738](https://github.com/pingcap/dm/pull/738)


## [1.0.5] 2020-04-27

### Improvements

- Improve the incremental replication speed when the `UNIQUE KEY` column has the `NULL` value
- Add retry for the `Write conflict` (9007 and 8005) error returned by TiDB

### Bug fixes

- Fix the issue that the `Duplicate entry` error might occur during the full data import
- Fix the issue that the replication task cannot be stopped or paused when the full data import is completed and the upstream has no written data  
- Fix the issue the monitoring metrics still display data after the replication task is stopped

### Action required

- When upgrading from a previous version, note that you must upgrade all DM components (dmctl/DM-master/DM-worker) together

### Detailed Bug Fixes and Changes

- Improve the incremental replication speed when the `UNIQUE KEY` column has the `NULL` value [#588](https://github.com/pingcap/dm/pull/588) [#597](https://github.com/pingcap/dm/pull/597)
- Add retry for the `Write conflict` (9007 and 8005) error returned by TiDB [#632](https://github.com/pingcap/dm/pull/632)
- Fix the issue that the `Duplicate entry` error might occur during the full data import [#554](https://github.com/pingcap/dm/pull/554)
- Fix the issue that the replication task cannot be stopped or paused when the full data import is completed and the upstream has no written data [#622](https://github.com/pingcap/dm/pull/622)
- Fix the issue the monitoring metrics still display data after the replication task is stopped [#616](https://github.com/pingcap/dm/pull/616)
- Fix the issue that the `Column count doesn't match value count` error might be returned during the sharding DDL replication [#624](https://github.com/pingcap/dm/pull/624)
- Fix the issue that some metrics such as `data file size` are incorrectly displayed when the paused task of full data import is resumed [#570](https://github.com/pingcap/dm/pull/570)
- Add and fix multiple monitoring metrics [#590](https://github.com/pingcap/dm/pull/590) [#594](https://github.com/pingcap/dm/pull/594)

## [1.0.4] 2020-03-13

### Improvements

- Add English UI for DM-portal
- Add the `--more` parameter in the `query-status` command to show complete replication status information

### Bug fixes

- Fix the issue that `resume-task` might fail to resume the replication task which is interrupted by the abnormal connection to the downstream TiDB server
- Fix the issue that the online DDL operation cannot be properly replicated after a failed replication task is restarted because the online DDL meta information has been cleared after the DDL operation failure
- Fix the issue that `query-error` might cause the DM-worker to panic after `start-task` goes into error
- Fix the issue that the relay log file and `relay.meta` cannot be correctly recovered when restarting an abnormally stopped DM-worker process before `relay.meta` is successfully written

### Action required

- When upgrading from a previous version, note that you must upgrade all DM components (dmctl/DM-master/DM-worker) together

### Detailed Bug Fixes and Changes

- Add English UI for DM-portal [#480](https://github.com/pingcap/dm/pull/480)
- Add the `--more` parameter in the `query-status` command to show complete replication status information [#533](https://github.com/pingcap/dm/pull/533)
- Fix the issue that `resume-task` might fail to resume the replication task which is interrupted by the abnormal connection to the downstream TiDB server [#436](https://github.com/pingcap/dm/pull/436)
- Fix the issue that the online DDL operation cannot be properly replicated after a failed replication task is restarted because the online DDL meta information is cleared after the DDL operation failure [#465](https://github.com/pingcap/dm/pull/465)
- Fix the issue that `query-error` might cause the DM-worker to panic after `start-task` goes into error [#519](https://github.com/pingcap/dm/pull/519)
- Fix the issue that the relay log file and `relay.meta` cannot be correctly recovered when restarting an abnormally stopped DM-worker process before `relay.meta` is successfully written [#534](https://github.com/pingcap/dm/pull/534)
- Fix the issue that the `value out of range` error might be reported when getting `server-id` from the upstream [#538](https://github.com/pingcap/dm/pull/538)
- Fix the issue that when Prometheus is not configured DM-Ansible prints the wrong error message that DM-master is not configured [#438](https://github.com/pingcap/dm/pull/438)

## [1.0.3] 2019-12-13

### Improvements

- Add the command mode in dmctl
- Support replicating the `ALTER DATABASE` DDL statement
- Optimize the error message output

### Bug fixes

- Fix the panic-causing data race issue occurred when the full import unit pauses or exits
- Fix the issue that `stop-task` and `pause-task` might not take effect when retrying SQL operations to the downstream

### Action required

- When upgrading from a previous version, note that you must upgrade all DM components (dmctl/DM-master/DM-worker) together

### Detailed Bug Fixes and Changes

- Add the command mode in dmctl [#364](https://github.com/pingcap/dm/pull/364)
- Optimize the error message output [#351](https://github.com/pingcap/dm/pull/351)
- Optimize the output of the `query-status` command [#357](https://github.com/pingcap/dm/pull/357)
- Optimize the privilege check for different task modes [#374](https://github.com/pingcap/dm/pull/374)
- Support checking the duplicate quotaed route-rules or filter-rules in task config[#385](https://github.com/pingcap/dm/pull/385)
- Support replicating the `ALTER DATABASE` DDL statement [#389](https://github.com/pingcap/dm/pull/389)
- Optimize the retry mechanism for anomalies [#391](https://github.com/pingcap/dm/pull/391)
- Fix the panic issue caused by the data race when the import unit pauses or exits  [#353](https://github.com/pingcap/dm/pull/353)
- Fix the issue that `stop-task` and `pause-task` might not take effect when retrying SQL operations to the downstream [#400](https://github.com/pingcap/dm/pull/400)
- Upgrade golang to v1.13 and upgrade the version of other dependencies [#362](https://github.com/pingcap/dm/pull/362)
- Filter the error that the context is canceled when a SQL statement is being executed [#382](https://github.com/pingcap/dm/pull/382)
- Fix the issue that the error occurred when performing a rolling update to DM monitor using DM-ansible causes the update to fail [#408](https://github.com/pingcap/dm/pull/408)

## [1.0.2] 2019-10-30

### Improvements

- Generate some config items for DM-worker automatically
- Generate some config items for replication task automatically
- Simplify the output of `query-status` without arguments
- Manage DB connections directly for downstream

### Bug fixes

- Fix some panic when starting up or executing SQL statements
- Fix abnormal sharding DDL replication on DDL execution timeout
- Fix starting task failure caused by the checking timeout or any inaccessible DM-worker
- Fix SQL execution retry for some error

### Action required

- When upgrading from a previous version, note that you must upgrade all DM components (dmctl/DM-master/DM-worker) together

### Detailed Bug Fixes and Changes

- Generate random `server-id` for DM-worker config automatically [#337](https://github.com/pingcap/dm/pull/337)
- Generate `flavor` for DM-worker config automatically [#328](https://github.com/pingcap/dm/pull/328)
- Generate `relay-binlog-name` and `relay-binlog-gtid` for DM-worker config automatically [#318](https://github.com/pingcap/dm/pull/318)
- Generate table name list for dumping in task config from black & white table lists automatically [#326](https://github.com/pingcap/dm/pull/326)
- Add concurrency items (`mydumper-thread`, `loader-thread` and `syncer-thread`) for task config [#314](https://github.com/pingcap/dm/pull/314)
- Simplify the output of `query-status` without arguments [#340](https://github.com/pingcap/dm/pull/340)
- Fix abnormal sharding DDL replication on DDL execution timeout [#338](https://github.com/pingcap/dm/pull/338)
- Fix potential DM-worker panic when restoring subtask from local meta [#311](https://github.com/pingcap/dm/pull/311)
- Fix DM-worker panic when committing a DML transaction failed [#313](https://github.com/pingcap/dm/pull/313)
- Fix DM-worker or DM-master panic when the listening port is being used [#301](https://github.com/pingcap/dm/pull/301)
- Fix retry for error code 1105 [#321](https://github.com/pingcap/dm/pull/321), [#332](https://github.com/pingcap/dm/pull/332)
- Fix retry for `Duplicate entry` and `Data too long for column` [#313](https://github.com/pingcap/dm/pull/313)
- Fix task check timeout when having large amounts of tables in upstream [#327](https://github.com/pingcap/dm/pull/327)
- Fix starting task failure when any DM-worker is not accessible [#319](https://github.com/pingcap/dm/pull/319)
- Fix potential DM-worker startup failure in GTID mode after being recovered from corrupt relay log [#339](https://github.com/pingcap/dm/pull/339)
- Fix in-memory TPS count for sync unit [#294](https://github.com/pingcap/dm/pull/294)
- Manage DB connections directly for downstream [#325](https://github.com/pingcap/dm/pull/325)
- Improve error system by refining error information passed between components [#320](https://github.com/pingcap/dm/pull/320)

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
