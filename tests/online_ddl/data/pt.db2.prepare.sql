use `online_ddl`;
create table t2_pt (id bigint auto_increment, uid int, name varchar(80), info varchar(100), primary key (`id`), unique key(`uid`)) DEFAULT CHARSET=utf8mb4;
create table t3_pt (id bigint auto_increment, uid int, name varchar(80), info varchar(100), primary key (`id`), unique key(`uid`)) DEFAULT CHARSET=utf8mb4;
insert into t2_pt (uid, name, info) values (40000, 'Remedios Moscote', '{}'), (40001, 'Amaranta', '{"age": 0}');
insert into t3_pt (uid, name, info) values (30001, 'Aureliano José', '{}'), (30002, 'Santa Sofía de la Piedad', '{}'), (30003, '17 Aurelianos', NULL);
