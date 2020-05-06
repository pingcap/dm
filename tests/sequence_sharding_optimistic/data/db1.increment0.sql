use `sharding_seq_opt`;

insert into t1 (id, c1, c2) values (11, '11', '11');
insert into t2 (id, c1, c2) values (12, '12', '12');

/* test create and alter database ddl in optimistic mode */
drop database if exists `sharding_seq_tmp`;
create database `sharding_seq_tmp` DEFAULT CHARACTER SET utf8;
alter database `sharding_seq_tmp` DEFAULT CHARACTER SET utf8mb4;
create table `sharding_seq_tmp`.`t1`(id int primary key);
replace into `sharding_seq_tmp`.`t1` values(1);
