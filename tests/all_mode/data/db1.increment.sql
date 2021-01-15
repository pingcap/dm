use all_mode;
create view v1 as select id from t1;
insert into t1 (id, name) values (3, 'Eddard Stark');
update t1 set name = 'Arya Stark' where id = 1;
update t1 set name = 'Catelyn Stark' where name = 'catelyn';

-- test multi column index with generated column
alter table t1 add column info json;
alter table t1 add column gen_id int as (info->'$.id');
alter table t1 add index multi_col(`id`, `gen_id`);
insert into t1 (id, name, info) values (4, 'gentest', '{"id": 123}');
insert into t1 (id, name, info) values (5, 'gentest', '{"id": 124}');
update t1 set info = '{"id": 120}' where id = 1;
update t1 set info = '{"id": 121}' where id = 2;
update t1 set info = '{"id": 122}' where id = 3;

-- test genColumnCache is reset after ddl
alter table t1 add column info2 varchar(40);
insert into t1 (id, name, info) values (6, 'gentest', '{"id": 125, "test cache": false}');
alter table t1 add unique key gen_idx(`gen_id`);
update t1 set name = 'gentestxx' where gen_id = 123;

insert into t1 (id, name, info) values (7, 'gentest', '{"id": 126}');
update t1 set name = 'gentestxxxxxx' where gen_id = 124;
-- delete with unique key
delete from t1 where gen_id > 124;

-- test alter database
-- tidb doesn't support alter character set from latin1 to utf8m64 so we comment this now
-- alter database all_mode CHARACTER SET = utf8mb4;

-- test decimal type
alter table t1 add column lat decimal(9,6) default '0.000000';
insert into t1 (id, name, info, lat) values (8, 'gentest', '{"id":127}', '123.123');

-- test bit type
alter table t1 add column bin bit(1) default NULL;
insert into t1 (id, name, info, lat, bin) values (9, 'gentest', '{"id":128}', '123.123', b'0');
insert into t1 (id, name, info, lat, bin) values (10, 'gentest', '{"id":129}', '123.123', b'1');

-- test bigint, min and max value for bigint/bigint unsigned
alter table t1 add column big1 bigint;
alter table t1 add column big2 bigint unsigned;
insert into t1 (id, name, info, lat, big1, big2) values (11, 'gentest', '{"id":130}', '123.123', -9223372036854775808, 0);
insert into t1 (id, name, info, lat, big1, big2) values (12, 'gentest', '{"id":131}', '123.123', 9223372036854775807, 18446744073709551615);

