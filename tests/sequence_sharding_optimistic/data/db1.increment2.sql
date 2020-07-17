use `sharding_seq_opt`;

-- at this point:
--      t1(id, c2, c3)
--      t2(id, c2, c3)
insert into t1 (id, c2, c3) values (100010, '120010', 130010);
insert into t2 (id, c2, c3) values (200010, '220010', 230010);

