use safe_mode_test;
delete from t3 where name = 'Santa Sofía de la Piedad';
alter table t2 add column age int;
insert into t2 (uid, name, age) values (40002, 'Remedios Moscote', 100), (40003, 'Amaranta', 103);
insert into t3 (uid, name) values (30004, 'Aureliano José'), (30005, 'Santa Sofía de la Piedad'), (30006, '17 Aurelianos');
alter table t3 add column age int;
update t3 set age = 1;
