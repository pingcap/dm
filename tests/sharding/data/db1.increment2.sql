create database `sharding2`;
use `sharding2`;
CREATE TABLE `t1` (`id` bigint(20) AUTO_INCREMENT, `uid` int(11), `name` varchar(80), `info` varchar(100), `age` int(11), `info_json` json GENERATED ALWAYS AS (`info`) VIRTUAL, `id_gen` int(11) GENERATED ALWAYS AS ((`uid` + 1)) VIRTUAL, PRIMARY KEY (`id`), UNIQUE KEY `uid` (`uid`), UNIQUE KEY `id_gen` (`id_gen`), KEY `multi_col_idx` (`uid`,`id_gen`)) DEFAULT CHARSET=utf8mb4;
CREATE TABLE `t2` (`id` bigint(20) AUTO_INCREMENT, `uid` int(11), `name` varchar(80), `info` varchar(100), `age` int(11), `info_json` json GENERATED ALWAYS AS (`info`) VIRTUAL, `id_gen` int(11) GENERATED ALWAYS AS ((`uid` + 1)) VIRTUAL, PRIMARY KEY (`id`), UNIQUE KEY `uid` (`uid`), UNIQUE KEY `id_gen` (`id_gen`), KEY `multi_col_idx` (`uid`,`id_gen`)) DEFAULT CHARSET=utf8mb4;
insert into `sharding1`.t1 (uid, name, info, age) values (10007, 'bruce wayne', '{"wealtch": "unknown"}', 1000), (10008, 'connelly', '{"boolean": true}', 1001);
insert into `sharding1`.t2 (uid, name, info, age) values (20009, 'haxwell', '{"key": "value000"}', 3003);
insert into `sharding2`.t1 (uid, name, info, age) values (60001, 'hello', '{"age": 201}', 133), (60002, 'world', '{"age": 202}', 233), (60003, 'final', '{"key": "value", "age": 203}', 333);
insert into `sharding2`.t2 (uid, name, info, age) values (80001, 'olleh', '{"age": 11}', 833), (80002, 'dlrod', NULL, 10000000), (80003, 'final***', '{"key": "value***", "age": false}', 33300);
