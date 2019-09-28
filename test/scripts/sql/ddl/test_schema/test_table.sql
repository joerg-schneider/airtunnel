drop table if exists test_table;

-- this is a sql line comment

create table test_table(x integer primary key, y string, z double);

-- the following tests the parameter formatting:

create index {idx_name} on test_table({idx_col});
