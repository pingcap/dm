drop database if exists `checkpoint_transaction`;
create database `checkpoint_transaction`;
use `checkpoint_transaction`;

create table t1 (
    id int NOT NULL AUTO_INCREMENT,
    a int,
    b int,
    PRIMARY KEY (id)
);
insert into t1(a) values (1);
