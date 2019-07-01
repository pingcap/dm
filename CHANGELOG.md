# DM Changelog

All notable changes to this project will be documented in this file.

## [1.0-rc.1] 2019-07-01

- Remove the restriction of "the next shard DDL statement cannot be executed unless the current shard DDL operation is completely finished in shard merge scene for binlog replication" [#177](https://github.com/pingcap/dm/pull/177)
- Support auto retry when meeting the error of `invalid connection` for incremental migration [#66](https://github.com/pingcap/dm/pull/66)
- Support generating `-schema-create.sql` files automatically for full migration [#186](https://github.com/pingcap/dm/pull/186)
- Use [TiDB SQL Parser](https://github.com/pingcap/parser) to parse and restore DDL statements in binlog query events, and remove the `using` statement when replicating to downstream [#54](https://github.com/pingcap/dm/pull/54)
- Support migrate tables with generated column for full and incremental migration [#42](https://github.com/pingcap/dm/pull/43) [#60](https://github.com/pingcap/dm/pull/60)
- Support appending the task name as a suffix to the dumped data directory (`dir` in the task configuration file) for full migration [#100](https://github.com/pingcap/dm/pull/100)
- Support resuming tasks automatically after the DM-worker process restarted [#88](https://github.com/pingcap/dm/pull/88) [#116](https://github.com/pingcap/dm/pull/116)
- Support skipping pre-check items when starting tasks [#65](https://github.com/pingcap/dm/pull/65)
- Support case-insensitive binlog event type in Binlog Event Filter rules [#188](https://github.com/pingcap/dm/pull/188)
- Optimize relay log mechanism to improve compatibility with upstream MySQL/MariaDB [#92](https://github.com/pingcap/dm/pull/92) [#108](https://github.com/pingcap/dm/pull/108) [#117](https://github.com/pingcap/dm/pull/117) [#140](https://github.com/pingcap/dm/pull/140) [#171](https://github.com/pingcap/dm/pull/171)
- Optimize RCP framework to limit the concurrency between DM-master and DM-worker [#157](https://github.com/pingcap/dm/pull/157)
- Redirect the log of mydumper into the log of DM-worker [#93](https://github.com/pingcap/dm/pull/93)
- Support using `--print-sample-config` command flag to show the sample config of DM-worker and DM-master [#28](https://github.com/pingcap/dm/pull/28)
- Support showing the status of sharding DDL replication in the Grafana dashboard and optimize the dashboard [#96](https://github.com/pingcap/dm/pull/96) [#101](https://github.com/pingcap/dm/pull/101) [#120](https://github.com/pingcap/dm/pull/120)
- Support setting the `max_allowed_packet` parameter for the database connection to the upstream and downstream [#99](https://github.com/pingcap/dm/pull/99) 
- Support using `unsafe_cleanup` in DM Ansible to clean components of DM cluster [#128](https://github.com/pingcap/dm/pull/128)
- Fix the wrong flush for checkpoint when existing multiple sharding groups [#124](https://github.com/pingcap/dm/pull/124)
- Fix the wrong progress when loading dumped files [#89](https://github.com/pingcap/dm/pull/89)
- Improve the test framework and test cases greatly and improve the correctness significantly
- Fix multiple other bugs that don't affect correctness

## [1.0-alpha] 2019-01-18

- Support the full data migration and the incremental data migration from MySQL/MariaDB into TiDB
- Support running multi independent synchronization tasks concurrently on one DM-worker instance
- Support synchronizing a certain table of the upstream MySQL/MariaDB instance to the specified table in TiDB by table routing
- Support only synchronizing or filtering all operations of some databases or some tables by black and white lists
- Support synchronizing or filtering some of the binlog events by binlog event filter
- Support modifying auto-increment primary key fields to resolve the conflicts for shard tables by column mapping
- Support merging the original shard instances and tables into TiDB but with some restrictions
- Support synchronizing data definition changed by online DDL tools (including pt-osc and gh-ost)
- Support handling synchronization trouble caused by DDL which is not supported by TiDB
