set @@session.SQL_MODE='PIPES_AS_CONCAT,IGNORE_SPACE,ONLY_FULL_GROUP_BY,NO_UNSIGNED_SUBTRACTION,NO_DIR_IN_CREATE,NO_AUTO_VALUE_ON_ZERO,NO_BACKSLASH_ESCAPES,STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ALLOW_INVALID_DATES,ERROR_FOR_DIVISION_BY_ZERO,HIGH_NOT_PRECEDENCE,NO_ENGINE_SUBSTITUTION,REAL_AS_FLOAT';
-- NO_AUTO_CREATE_USER set failed in mysql8.0
use sql_mode;

-- test sql_mode PIPES_AS_CONCAT
insert into t_2(name) values('pipes'||'as'||'concat');

-- test sql_mode ANSI_QUOTES
insert into t_2(name) values("a");

-- test sql_mode IGNORE_SPACE
select count (*) as c from t_2;

-- test sql_mode NO_AUTO_VALUE_ON_ZERO
insert into t_2(id, name) values (10, 'a');
insert into t_2(id, name) values (0, 'b');

-- test sql_mode NO_BACKSLASH_ESCAPES
insert into t_2(name) values ('\\a');