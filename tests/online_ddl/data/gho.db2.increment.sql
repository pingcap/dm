use online_ddl;
delete from gho_t3 where name = 'Santa Sofía de la Piedad';
alter table gho_t2 add column age int;
update gho_t2 set uid = uid + 10000;
alter table gho_t3 add column age int;
update gho_t3 set age = 1;
alter table gho_t2 add key name (name);
alter table gho_t3 add key name (name);
alter table gho_t2 add column info_json json GENERATED ALWAYS AS (`info`) VIRTUAL;
update gho_t3 set age = age + 10;
alter table gho_t3 add column info_json json GENERATED ALWAYS AS (`info`) VIRTUAL;
update gho_t2 set age = age + 10;
