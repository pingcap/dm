use safe_mode_test;
delete from t3 where name = 'Santa Sofía de la Piedad';
alter table t2 add column age int;
update t2 set uid = uid + 10000;
alter table t3 add column age int;
update t3 set age = 1;
