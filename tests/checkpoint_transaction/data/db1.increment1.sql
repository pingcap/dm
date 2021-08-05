use checkpoint_transaction;

start transaction;
    call dowhile1(100);
commit;