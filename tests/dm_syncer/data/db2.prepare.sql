drop database if exists `dm_syncer`;
create database `dm_syncer`;
use `dm_syncer`;
create table t2 (id int auto_increment, name varchar(20), primary key (`id`));
insert into t2 (name) values ('Arya'), ('Bran'), ('Sansa');

create table dm_syncer_route_rules_1 (id1 int, name varchar(20), primary key (`id1`));
insert into dm_syncer_route_rules_1 (id1, name) values (1, 'Howie'), (2, 'howie');

create table dm_syncer_route_rules_2 (id2 int, name varchar(20), primary key (`id2`));
insert into dm_syncer_route_rules_2 (id2, name) values (2, 'howie'), (3, 'Bran'), (4, 'Sansa');

create table dm_syncer_route_rules_3 (id3 int, name varchar(20), primary key (`id3`));
insert into dm_syncer_route_rules_3 (id3, name) values (3, 'Arya'), (4, 'Howie'), (5, 'Sansa');