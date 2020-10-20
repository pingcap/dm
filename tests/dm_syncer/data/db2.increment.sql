use dm_syncer_plus;
delete from t2 where name = 'Sansa';

insert into dm_syncer_filter_rule(name) values ('howie');
update dm_syncer_filter_rule set id = 1 where name = 'howie';
drop database if exists `dm_syncer_plus`;