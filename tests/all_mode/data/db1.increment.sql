use all_mode;
insert into t1 (id, name) values (3, 'Eddard Stark');
update t1 set name = 'Arya Stark' where id = 1;
update t1 set name = 'Catelyn Stark' where name = 'catelyn';
alter table t1 add column info json;
alter table t1 add column gen_id int as (info->"$.id");
alter table t1 add index multi_col(`id`, `gen_id`);
insert into t1 (id, name, info) values (4, 'gentest', '{"id": 123}');
insert into t1 (id, name, info) values (5, 'gentest', '{"id": 124}');
