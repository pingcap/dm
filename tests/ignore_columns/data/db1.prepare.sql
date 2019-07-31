drop database if exists `ignore_columns`;
create database `ignore_columns`;
use `ignore_columns`;
create table t1 (id int, name varchar(20));
insert into t1 (id, name) values (1, 'arya'), (2, 'catelyn');
