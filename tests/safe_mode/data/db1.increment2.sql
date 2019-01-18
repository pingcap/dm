use safe_mode_test;
insert into t1 (uid, name, age) values (10006, 'Buenos Aires', 100);
insert into t2 (uid, name, age) values (20006, 'Colonel Aureliano Buendía', 402);
alter table t1 add column age2 int;
alter table t2 add column age2 int;
insert into t1 (uid, name, age, age2) values (10007, 'Buenos Aires', 300, 600);
insert into t2 (uid, name, age, age2) values (20007, 'Colonel Aureliano Buendía', 301, 602);
insert into t1 (uid, name, age, age2) values (10008, 'Buenos Aires', 400, 800);
insert into t2 (uid, name, age, age2) values (20009, 'Colonel Aureliano Buendía', 401, 802);
