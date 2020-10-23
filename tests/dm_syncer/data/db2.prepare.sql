drop database if exists `dm_syncer`;
create database `dm_syncer`;
use `dm_syncer`;
create table t2 (id int auto_increment, name varchar(20), primary key (`id`));
insert into t2 (name) values ('Arya'), ('Bran'), ('Sansa');

drop database if exists `dm_syncer_filter_rules`;
create database `dm_syncer_filter_rules`;
use `dm_syncer_filter_rules`;
create table dm_syncer_filter_rule(id int auto_increment, name varchar(20), primary key (`id`));