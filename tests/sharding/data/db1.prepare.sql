drop database if exists `sharding1`;
drop database if exists `sharding2`;
create database `sharding1`;
use `sharding1`;
create table t1 (id bigint auto_increment, uid int, name varchar(80), info varchar(100), primary key (`id`), unique key(`uid`)) DEFAULT CHARSET=utf8mb4;
create table t2 (id bigint auto_increment, uid int, name varchar(80), info varchar(100), primary key (`id`), unique key(`uid`)) DEFAULT CHARSET=utf8mb4;
insert into t1 (uid, name) values (10001, 'Gabriel García Márquez'), (10002, 'Cien años de soledad');
insert into t2 (uid, name) values (20001, 'José Arcadio Buendía'), (20002, 'Úrsula Iguarán'), (20003, 'José Arcadio');
create view v1 as select id from t1;

create database `shardview1`;
create database `shardview2`;
create table shardview1.t1 (id int primary key);
create table shardview2.t1 (id int primary key);
create view shardview1.v1 as select id from shardview2.t1;
create view shardview2.v1 as select id from shardview1.t1;
-- after sharding route merge, there should be a view reference merged table
