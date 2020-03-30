create database sharding22;
create table sharding22.t1 (id int primary key, name varchar(20));
alter table sharding22.t1 add index abc(name);
/*insert into sharding22.t1 values(7, "a"), (8, "b");*/
/*insert into sharding22.t1 values(9, "c"), (10, "d");*/
/*insert into sharding22.t1 values(11, "e"), (12, "f");*/
