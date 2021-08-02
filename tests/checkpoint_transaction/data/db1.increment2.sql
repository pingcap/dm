use checkpoint_transaction;

delimiter $$
drop procedure if exists dowhile; 
create procedure dowhile(nums int)
begin
WHILE nums > 0 DO
    insert into t1(a) values(nums);
    set nums = nums - 1;
END WHILE;
end $$

delimiter ;
start transaction;
    call dowhile(100);
commit;