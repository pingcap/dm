drop database if exists `dm_syncer`;
create database `dm_syncer`;
use `dm_syncer`;
create table t2 (id int auto_increment, name varchar(20), primary key (`id`));
insert into t2 (name) values ('Arya'), ('Bran'), ('Sansa');

create table dm_syncer_filter_rule(id int auto_increment, name varchar(20), primary key (`id`));
insert into dm_syncer_filter_rule(name) values ('howie', 'Howie');
update dm_syncer_filter_rule set id = 1 where name = 'howie';
drop table dm_syncer_filter_rule;