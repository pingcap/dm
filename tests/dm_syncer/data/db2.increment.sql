use dm_syncer;
delete from t2 where name = 'Sansa';

use dm_syncer_filter_rules;
insert into dm_syncer_filter_rule(name) values ('howie');
update dm_syncer_filter_rule set id = 1 where name = 'howie';
drop database if exists `dm_syncer_filter_rules`;