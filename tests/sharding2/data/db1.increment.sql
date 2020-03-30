create database sharding22;
create table sharding22.t1 (id int primary key, name varchar(20));
alter table sharding22.t1 add index abc(name);
insert into sharding22.t1 values(1, "a"), (2, "b");
insert into sharding22.t1 values(3, "c"), (4, "d");
insert into sharding22.t1 values(5, "e"), (6, "f");
