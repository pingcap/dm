drop database if exists `ha_test`;
create database `ha_test`;
use `ha_test`;
create table ta (id int, name varchar(20));
insert into ta (id, name) values (1, 'arya'), (2, 'catelyn');
