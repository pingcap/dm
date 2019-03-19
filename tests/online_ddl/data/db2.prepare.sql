drop database if exists `online_ddl`;
create database `online_ddl`;
use `online_ddl`;
create table t2 (id bigint auto_increment, uid int, name varchar(80), info varchar(100), primary key (`id`), unique key(`uid`)) DEFAULT CHARSET=utf8mb4;
create table t3 (id bigint auto_increment, uid int, name varchar(80), info varchar(100), primary key (`id`), unique key(`uid`)) DEFAULT CHARSET=utf8mb4;
insert into t2 (uid, name, info) values (40000, 'Remedios Moscote', '{}'), (40001, 'Amaranta', '{"age": 0}');
insert into t3 (uid, name, info) values (30001, 'Aureliano José', '{}'), (30002, 'Santa Sofía de la Piedad', '{}'), (30003, '17 Aurelianos', NULL);
