drop database if exists `all_mode`;
create database `all_mode`;
use `all_mode`;
create table t2 (id int auto_increment, name varchar(20), primary key (`id`));
insert into t2 (name) values ('Arya'), ('Bran'), ('Sansa');
create view v2 as select id from t2;

-- test block-allow-list
drop database if exists `ignore_db`;
create database `ignore_db`;
use `ignore_db`;
create table `ignore_table`(id int);