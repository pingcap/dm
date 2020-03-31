use `sharding_seq_opt`;

insert into t3 (id, c1, c2) values (400003, 'eleven', 'eleventh');

-- at this point:
--      t2(id, c1, c2)
--      t3(id, c1, c2)
--      t4(id, c1, c2)

alter table t4 add column c3 int;

-- at this point:
--      t2(id, c1, c2)
--      t3(id, c1, c2)
--      t4(id, c1, c2, c3)

insert into t4 (id, c1, c2, c3) values (500003, 'thirteen', 'thirteenth', 13);
insert into t2 (id, c1, c2) values (300003, 'fourteen', 'forteenth');

alter table t4 drop column c1;

-- at this point:
--      t2(id, c1, c2)
--      t3(id, c1, c2)
--      t4(id,     c2, c3)

alter table t2 drop column c1;
alter table t2 add column c3 int;
alter table t3 drop column c1;
alter table t3 add column c3 int;

-- at this point:
--      t2(id, c2, c3)
--      t3(id, c2, c3)
--      t4(id, c2, c3)
