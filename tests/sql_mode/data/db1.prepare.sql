drop database if exists `sql_mode`;
create database `sql_mode`;
use `sql_mode`;
CREATE TABLE `t_1` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(60),
  `num` int,
  `dt` datetime,
  -- `ts` timestamp,
  PRIMARY KEY (id)
);