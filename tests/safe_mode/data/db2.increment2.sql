use safe_mode_test;
alter table t2 add column age2 int;
update t2 set age = age + 10;
alter table t3 add column age2 int;
update t3 set age2 = 100;
