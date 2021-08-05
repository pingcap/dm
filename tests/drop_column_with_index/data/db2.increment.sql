use drop_column_with_index;

alter table t1 drop column c1;
CREATE TABLE t2 (col1 INT  PRIMARY KEY, col2 INT, INDEX func_index ((ABS(col1))));