drop database if exists `filter_user_cancel`;
create database `filter_user_cancel`;
use `filter_user_cancel`;
create table t2 (id int auto_increment, name varchar(20), primary key (`id`));
insert into t2 (name) values ('Arya'), ('Bran'), ('Sansa');
