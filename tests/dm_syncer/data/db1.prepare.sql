drop database if exists `dm_syncer`;
create database `dm_syncer`;
use `dm_syncer`;
create table t1 (id int, name varchar(20));
insert into t1 (id, name) values (1, 'arya'), (2, 'catelyn');
