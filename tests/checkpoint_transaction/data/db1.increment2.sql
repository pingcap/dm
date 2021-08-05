use checkpoint_transaction;

start transaction;
    call dowhile2(100);
commit;