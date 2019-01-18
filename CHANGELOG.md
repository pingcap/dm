# DM Changelog

All notable changes to this project will be documented in this file.

## [1.0-alpha] 2019-01-18

- Support the full data migration and the incremental data migration from MySQL/MariaDB into TiDB
- Support running multi independent synchronization tasks concurrently on one DM-worker instance
- Support synchronizing a certain table of the upstream MySQL/MariaDB instance to the specified table in TiDB by table routing
- Support only synchronizing (or filtering) all operations of some databases or some tables by black and white lists
- Support only synchronizing (or filtering) some binlog events by binlog event filtering
- Support modifying auto-increment primary key fields to resolve the conflicts for sharded tables by column mapping
- Support merging the original sharded instances and tables into TiDB but with some restrictions
- Support synchronizing data definition changed by online DDL tools (including pt-osc and gh-ost)
- Support handling synchronization trouble caused by DDL which is not supported by TiDB
