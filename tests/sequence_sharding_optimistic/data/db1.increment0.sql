use `sharding_seq_opt`;

insert into t1 (id, c1, c2) values (11, '11', '11');
insert into t2 (id, c1, c2) values (12, '12', '12');

/* test create database ddl in optimistic mode */
drop database if exists `sharding_seq_tmp`;
create database `sharding_seq_tmp`;