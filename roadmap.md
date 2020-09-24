# Data Migration Roadmap

## Primary Focus

- Make DM easy to use: there are a lot of usability issues that have been spat out for up to a year! We need to address at least known usability issues.
- DM 2.0 GA: DM 2.0 (supports High Availability and better shard DDL replication) has taken so long and no GA, it’s time to GA delivery.

## Usability Improvement

- [ ] solve known usability issue (continuous work)
  - What: solve usability issues recorded in [the project](https://github.com/pingcap/dm/projects/3)
  - Why: a lot of usability issues that have been not resolved yet, we need to stop user churn
- [ ] better configuration file [#775](https://github.com/pingcap/dm/issues/775)
  - What: avoid misuse for configuration files
  - Why: many users meet problem when write configuration file but don’t know how to deal with it

## New features

- [ ] stop/pause until reached the end of a transaction [#1095](https://github.com/pingcap/dm/issues/1095)
  - What: replicate binlog events until reached the end of the current transaction when stopping or pausing the task
  - Why: achieve transaction consistency as the upstream MySQL after stopped/paused the task
- [ ] stop at the specified position/GTID [#348](https://github.com/pingcap/dm/issues/348)
  - What: stop to replicate when reached the specified binlog position or GTID, like `until_option` in MySQL
  - Why: no more data replicated without needs to stop writing in the upstream
- [ ] update source config online [#1076](https://github.com/pingcap/dm/issues/1076)
  - What: update source configs (like upstream connection arguments) online
  - Why: switch from one MySQL instance to another in the replica group easier
- [ ] provides a complete online replication checksum feature [#1097](https://github.com/pingcap/dm/issues/1097)
  - What:
    - check data without stopping writing in upstream
    - no extra writes  in upstream
  - Why: found potential consistency earlier
- [ ] support DM v2.0 in TiDB Operator [tidb-operator#2868](https://github.com/pingcap/tidb-operator/issues/2868)
  - What: use TiDB-Operator to manage DM 2.0

## Out of Scope currently

- Only supports synchronization of MySQL protocol Binlog to TiDB cluster and not all MySQL protocol databases
  - Why: some MySQL protocol databases are not binlog protocol compatible
- Provides a fully automated shard DDL merge and synchronization solution
  - Why: many scenes are difficult to automate
- Replicate multiple upstream MySQL sources with one dm-worker
  - Why:lLarge amount of development work and many uncertainties
