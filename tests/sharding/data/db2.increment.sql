use sharding;
delete from t3 where name = 'Santa Sofía de la Piedad';
alter table t2 add column age int;
update t2 set uid = uid + 10000;
alter table t3 add column age int;
update t3 set age = 1;
alter table t2 add column info_json json GENERATED ALWAYS AS (`info`) VIRTUAL;
update t3 set age = age + 10;
alter table t3 add column info_json json GENERATED ALWAYS AS (`info`) VIRTUAL;
