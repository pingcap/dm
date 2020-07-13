drop user if exists 'drop_column_with_index';
flush privileges;
create user 'drop_column_with_index'@'%' identified by '123456';
grant all privileges on *.* to 'drop_column_with_index'@'%';
revoke select, reload on *.* from 'drop_column_with_index'@'%';
revoke create temporary tables, lock tables, create routine, alter routine, event, create tablespace, file, shutdown, execute, process, index on *.* from 'drop_column_with_index'@'%'; # privileges not supported by TiDB
flush privileges;
