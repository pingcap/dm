drop database if exists `gtid`;
create database `gtid`;
use `gtid`;
create table t1 (id int PRIMARY KEY);
insert into t1 values (1);