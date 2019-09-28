delete from test_table;

insert into test_table (x, y, z) values (0, "Lorem", 1.5);
insert into test_table (x, y, z) values (1, "Ipsum", 3.141);
insert into test_table (x, y, z) values (2, "Dolor", 42.42);

commit;