use `sharding_seq`;
insert into t1 (uid,name,c,d,e,f) values (15, "i", 15, 15, 15, 15);
alter table t1 drop column f;
alter table t2 drop column f;
