drop database if exists `load_interrupt`;
create database `load_interrupt`;
use `load_interrupt`;
create table t1 (id int, name varchar(20));
insert into t1 (id, name) values (1, 'arya'), (2, 'catelyn');
