use sharding1;
delete from t3 where name = 'Santa Sofía de la Piedad';
insert into t2 (uid, name, info) values (40001, 'Amaranta', '{"age": 0}'); # DML to trigger fetch schema from downstream before DDL
alter table t2 add column age int;
update t2 set uid = uid + 10000;
alter table t3 add column age int;
update t3 set age = 1;
alter table t2 add column info_json json GENERATED ALWAYS AS (`info`) VIRTUAL;
update t3 set age = age + 10;
alter table t3 add column info_json json GENERATED ALWAYS AS (`info`) VIRTUAL;
alter table t3 add column id_gen int as (uid + 1);
alter table t2 add column id_gen int as (uid + 1);
alter table t2 add unique (id_gen);
alter table t3 add unique (id_gen);
update t2 set age = age + 10;
alter table t2 add key multi_col_idx(uid, id_gen);
alter table t3 add key multi_col_idx(uid, id_gen);
update t3 set age = age + 10;
