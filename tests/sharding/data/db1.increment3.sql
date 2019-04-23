drop table sharding2.t2;
insert into `sharding1`.t1 (uid, name, info, age) values (10009, 'lkhhgg', '{"aaa": "123"}', 10001), (10018, '..', '{"boolean": true}', 10000001);
insert into `sharding1`.t2 (uid, name, info, age) values (20010, 'minmax', '{"key": "hash string"}', 307003);

