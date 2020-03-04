use `sharding_seq_opt`;

insert into t2 (id, c1, c2) values (200003, 'twelve', 'twelfth');

-- at this point:
--      t1(id, c1, c2)
--      t2(id, c1, c2)

alter table t1 drop column c1;
insert into t1 (id, c2) values (100003, 'fifteenth');

-- at this point:
--      t1(id,     c2)
--      t2(id, c1, c2)

alter table t2 add column c3 int;
alter table t2 drop column c1;
alter table t2 add column c1 varchar(20);
alter table t2 drop column c1;

-- at this point:
--      t1(id, c2)
--      t2(id, c2, c3)

insert into t2 (id, c2, c3) values (200004, 'sixteenth', 16);

alter table t1 add column c3 int;

-- at this point:
--      t1(id, c2, c3)
--      t2(id, c2, c3)

insert into t1 (id, c2, c3) values (100004, 'seventeenth', 17);
