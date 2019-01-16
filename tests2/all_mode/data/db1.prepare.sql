drop database if exists `all_mode`;
create database `all_mode`;
use `all_mode`;
create table t1 (id int auto_increment, name varchar(20), primary key (`id`));
insert into t1 (name) values ('arya'), ('catelyn');
