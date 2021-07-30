use checkpoint_transaction;

begin;
    declare @i int default 0;
    declare @nums int default 1000;
    while @i < @nums 
    begin
        insert into t1(b) values (@i);
        set @i = @i + 1;
    end while;
commit;