drop database if exists `downstream_more_column`;
drop database if exists `downstream_more_column1`;
create database `downstream_more_column1`;
use `downstream_more_column1`;
create table t1 (c1 int, c2 int, c3 int, primary key(c1));
insert into t1 values(1, 1, 1);
insert into t1 values(2, 2, 2);
