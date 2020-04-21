use `sharding_seq`;
insert into t2 (uid,name,c,d,e,f) values (16, "j", 16, 16, 16, 16);
alter table t2 drop column f;
alter table t3 drop column f;
alter table t4 drop column f;
