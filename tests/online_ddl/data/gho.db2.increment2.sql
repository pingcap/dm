use online_ddl;
insert into t3_gho (uid, name, info) values (30004, 'name of 30004', '{"age": 30004}');
insert into t2_gho (uid, name, info) values (50002, 'name of 50002', '{"age": 50002}');
alter table t3_gho add column address varchar(255);
alter table t2_gho add column address varchar(255);
alter table t2_gho add key address (address);
alter table t3_gho add key address (address);
insert into t2_gho (uid, name, info, address) values (50003, 'name of 50003', '{"age": 50003}', 'address of 50003');
insert into t3_gho (uid, name, info, address) values (30005, 'name of 30005', '{"age": 30005}', 'address of 30005');
alter table t2_gho drop column age;
alter table t3_gho drop column age;
insert into t3_gho (uid, name, info, address) values (30006, 'name of 30006', '{"age": 30006}', 'address of 30006');
insert into t2_gho (uid, name, info, address) values (50004, 'name of 50004', '{"age": 50004}', 'address of 50004');
