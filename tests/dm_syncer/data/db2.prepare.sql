drop database if exists `dm_syncer_plus`;
create database `dm_syncer_plus`;
use `dm_syncer_plus`;
create table t2 (id int auto_increment, name varchar(20), primary key (`id`));
insert into t2 (name) values ('Arya'), ('Bran'), ('Sansa');

create table dm_syncer_filter_rule(id int auto_increment, name varchar(20), primary key (`id`));
insert into dm_syncer_filter_rule(name) values ('howie');
update dm_syncer_filter_rule set id = 1 where name = 'howie';
drop database if exists `dm_syncer_plus`;

create table dm_syncer_route_rules_1 (id int, name varchar(20), primary key (`id`));
insert into dm_syncer_route_rules_1 (id, name) values (1, 'Howie'), (2, 'howie');

create table dm_syncer_route_rules_2 (id int, name varchar(20), primary key (`id`));
insert into dm_syncer_route_rules_2 (id, name) values (3, 'howie'), (4, 'Bran'), (5, 'Sansa');

create table dm_syncer_route_rules_3 (id int, name varchar(20), primary key (`id`));
insert into dm_syncer_route_rules_3 (id, name) values (6, 'Arya'), (7, 'Howie'), (8, 'Sansa');