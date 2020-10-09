drop database if exists `upgrade`;
create database `upgrade`;
use `upgrade`;
create table t1 (id int, PRIMARY KEY (id));
insert into t1 (id) values (1);
