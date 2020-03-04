drop database if exists `ha_test`;
create database `ha_test`;
use `ha_test`;
create table tb (id int auto_increment, name varchar(20), primary key (`id`));
insert into tb (name) values ('Arya'), ('Bran'), ('Sansa');
