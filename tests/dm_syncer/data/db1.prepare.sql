drop database if exists `dm_syncer`;
create database `dm_syncer`;
use `dm_syncer`;
create table t1 (id int, name varchar(20), primary key(`id`));
insert into t1 (id, name) values (1, 'arya'), (2, 'catelyn');

drop database if exists `logs`;
create database `logs`;
use `logs`;
create table messages_2016(id int, name varchar(20), primary key(`id`));
insert into messages_2016(id, name) values (1, 'arya'), (2, 'catelyn');
create table messages_2017(id int, name varchar(20), primary key(`id`));
insert into messages_2017(id, name) values (1, 'arya'), (2, 'catelyn');
create table messages_2018(id int, name varchar(20), primary key(`id`));
insert into messages_2018(id, name) values (1, 'arya'), (2, 'catelyn');

drop database if exists `forum`;
create database `forum`;
use `forum`;
create table users(id int, name varchar(20), primary key(`id`));
insert into users(id, name) values (1, 'arya'), (2, 'catelyn');
create table message(id int, name varchar(20), primary key(`id`));
insert into message(id, name) values (1, 'arya'), (2, 'catelyn');

drop database if exists `forum_backup_2016`;
create database `forum_backup_2016`
use `forum_backup_2016`;
create table message(id int, name varchar(20), primary key(`id`));
insert into message(id, name) values (1, 'arya'), (2, 'catelyn');

drop database if exists `forum_backup_2017`;
create database `forum_backup_2017`
use `forum_backup_2017`;
create table message(id int, name varchar(20), primary key(`id`));
insert into message(id, name) values (1, 'arya'), (2, 'catelyn');

drop database if exists `forum_backup_2018`;
create database `forum_backup_2018`
use `forum_backup_2018`;
create table message(id int, name varchar(20), primary key(`id`));
insert into message(id, name) values (1, 'arya'), (2, 'catelyn');