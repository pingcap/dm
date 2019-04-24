drop database if exists `all_mode`;
create database `all_mode`;
use `all_mode`;
create table t1 (id int, name varchar(20));
insert into t1 (id, name) values (1, 'arya'), (2, 'catelyn');
