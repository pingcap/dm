drop database if exists `forward_master`;
create database `forward_master`;
use `forward_master`;
create table t1 (id int, name varchar(20));
insert into t1 (id, name) values (1, 'arya'), (2, 'catelyn');