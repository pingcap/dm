use online_ddl;
insert into t1 (uid, name) values (10003, 'Buenos Aires');
update t1 set name = 'Gabriel José de la Concordia García Márquez' where `uid` = 10001;
update t1 set name = 'One Hundred Years of Solitude' where name = 'Cien años de soledad';
alter table t1 add column age int;
alter table t2 add column age int;
insert into t2 (uid, name, age, info) values (20004, 'Colonel Aureliano Buendía', 301, '{}');
alter table t2 add column info_json json GENERATED ALWAYS AS (`info`) VIRTUAL;
insert into t1 (uid, name, info) values (10004, 'Buenos Aires', '{"age": 10}');
insert into t2 (uid, name, info) values (20005, 'Buenos Aires', '{"age": 100}');
insert into t2 (uid, name, info) values (20006, 'Buenos Aires', '{"age": 1000}');
alter table t1 add column info_json json GENERATED ALWAYS AS (`info`) VIRTUAL;
alter table t1 add column id_gen int as (uid + 1);
alter table t2 add column id_gen int as (uid + 1);
alter table t1 add unique (id_gen);
alter table t2 add unique (id_gen);
insert into t1 (uid, name, info) values (10005, 'Buenos Aires', '{"age": 100}');
insert into t2 (uid, name, info) values (20007, 'Buenos Aires', '{"age": 200}');
