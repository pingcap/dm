drop user if exists 'dm_incremental';
flush privileges;
create user 'dm_incremental'@'%' identified by '';
grant replication slave, replication client on *.* to 'dm_incremental'@'%';
flush privileges;
