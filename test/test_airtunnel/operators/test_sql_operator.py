import logging
import sqlite3

import pytest

from airtunnel.operators.sql.sqloperator import SQLOperator

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def test_param_check(test_db_hook):
    with pytest.raises(AssertionError):
        x = SQLOperator(
            sql_hook=test_db_hook, script_file_relative_path=None, task_id="x"
        )
        logger.info(x)


@pytest.fixture
def sqlite_operator(test_db_hook):
    return SQLOperator(
        sql_hook=test_db_hook,
        script_file_relative_path=[
            "ddl/test_schema/test_table.sql",
            "dml/test_schema/test_table.sql",
        ],
        task_id="x",
        parameters={"idx_name": "test_index"},
        dynamic_parameters=lambda x: {"idx_col": "x"},
    )


def test_read_sql_file(sqlite_operator):
    sqlite_operator._load_sql_script()
    assert len(sqlite_operator.loaded_sql_script) > 0

    s1 = "drop table if exists test_table;"
    s2 = "create table test_table(x integer primary key, y string, z double);"

    assert s1 in sqlite_operator.loaded_sql_script
    assert s2 in sqlite_operator.loaded_sql_script


def test_split_sql_file(sqlite_operator):
    pass


"""
    assert len(statements) >= 2

    comment = "-- this is a sql line comment"

    for stmnt in statements:
        assert comment not in stmnt
"""


def test_format_and_execute_sql(sqlite_operator, test_db_path):
    # execute both scripts declared in the fixture...
    sqlite_operator.execute(context=None)
    # validate that table exists and data was inserted...
    print(test_db_path)
    conn = sqlite3.connect(test_db_path)
    res = conn.execute("select count(*) from test_table")
    assert res.fetchall()[0][0] == 3, "Expected 3 rows"

    res = conn.execute("select count(*) from test_table where y = 'Lorem'")
    assert res.fetchall()[0][0] == 1, "Expected 1 rows"
