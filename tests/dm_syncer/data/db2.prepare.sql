drop database if exists `dm_syncer`;
create database `dm_syncer`;
use `dm_syncer`;
create table t2 (id int auto_increment, name varchar(20), primary key (`id`));
insert into t2 (name) values ('Arya'), ('Bran'), ('Sansa');

create table dm_syncer_route_rules_1 (id int auto_increment, name varchar(20), primary key (`id`));
insert into dm_syncer_route_rules_1 (name) values ('Howie'), ('howie');

create table dm_syncer_route_rules_2 (id int auto_increment, name varchar(20), primary key (`id`));
insert into dm_syncer_route_rules_2 (name) values ('howie'), ('Bran'), ('Sansa');

create table dm_syncer_route_rules_3 (id int auto_increment, name varchar(20), primary key (`id`));
insert into dm_syncer_route_rules_4 (name) values ('Arya'), ('Howie'), ('Sansa');