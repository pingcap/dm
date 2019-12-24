drop database if exists `forward_master`;
create database `forward_master`;
use `forward_master`;
create table t2 (id int auto_increment, name varchar(20), primary key (`id`));
insert into t2 (name) values ('Arya'), ('Bran'), ('Sansa');
