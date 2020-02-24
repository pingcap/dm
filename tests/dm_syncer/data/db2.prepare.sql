drop database if exists `dm_syncer`;
create database `dm_syncer`;
use `dm_syncer`;
create table t2 (id int auto_increment, name varchar(20), primary key (`id`));
insert into t2 (name) values ('Arya'), ('Bran'), ('Sansa');
