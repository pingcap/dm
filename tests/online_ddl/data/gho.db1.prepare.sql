drop database if exists `online_ddl`;
create database `online_ddl`;
use `online_ddl`;
create table t1_gho (id bigint auto_increment, uid int, name varchar(80), info varchar(100), primary key (`id`), unique key(`uid`)) DEFAULT CHARSET=utf8mb4;
create table t2_gho (id bigint auto_increment, uid int, name varchar(80), info varchar(100), primary key (`id`), unique key(`uid`)) DEFAULT CHARSET=utf8mb4;
insert into t1_gho (uid, name) values (10001, 'Gabriel García Márquez'), (10002, 'Cien años de soledad');
insert into t2_gho (uid, name) values (20001, 'José Arcadio Buendía'), (20002, 'Úrsula Iguarán'), (20003, 'José Arcadio');
