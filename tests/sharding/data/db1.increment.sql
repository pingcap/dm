use sharding;
insert into t1 (uid, name) values (10003, 'Buenos Aires');
update t1 set name = 'Gabriel José de la Concordia García Márquez' where `uid` = 10001;
update t1 set name = 'One Hundred Years of Solitude' where name = 'Cien años de soledad';
alter table t1 add column age int;
alter table t2 add column age int;
insert into t2 (uid, name, age) values (20004, 'Colonel Aureliano Buendía', 301);
