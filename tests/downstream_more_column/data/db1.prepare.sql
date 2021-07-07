drop database if exists `downstream_more_column`;
create database `downstream_more_column`;
use `downstream_more_column`;
create table t1 (c1 int, c2 int, c3 int, primary key(c1));
insert into t1 values(1, 1, 1);
insert into t1 values(2, 2, 2);
