use online_ddl;
delete from t3_gho where name = 'Santa Sofía de la Piedad';
alter table t2_gho add column age int;
update t2_gho set uid = uid + 10000;
alter table t3_gho add column age int;
update t3_gho set age = 1;
alter table t2_gho add key name (name);
alter table t3_gho add key name (name);
alter table t2_gho add column info_json json GENERATED ALWAYS AS (`info`) VIRTUAL;
update t3_gho set age = age + 10;
alter table t3_gho add column info_json json GENERATED ALWAYS AS (`info`) VIRTUAL;
update t2_gho set age = age + 10;
