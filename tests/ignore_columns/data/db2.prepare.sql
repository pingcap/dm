drop database if exists `ignore_columns`;
create database `ignore_columns`;
use `ignore_columns`;
create table t2 (id int auto_increment, name varchar(20), primary key (`id`));
insert into t2 (name) values ('Arya'), ('Bran'), ('Sansa');
