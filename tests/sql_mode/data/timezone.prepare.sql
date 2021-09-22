drop database if exists `sql_mode`;

create database `sql_mode`;

use `sql_mode`;

create table if not exists `sql_mode`.`timezone` (`id` int, `a` timestamp, PRIMARY KEY (id));