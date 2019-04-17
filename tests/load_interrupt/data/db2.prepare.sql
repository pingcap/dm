drop database if exists `load_interrupt`;
create database `load_interrupt`;
use `load_interrupt`;
create table t2 (id int auto_increment, name varchar(20), primary key (`id`));
insert into t2 (name) values ('Arya'), ('Bran'), ('Sansa');
