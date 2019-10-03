import pytest

from airtunnel.operators.sql import sql_helpers

TEST_SCRIPT = "ddl/test_schema/test_table.sql"


@pytest.mark.parametrize(
    argnames=("sql_path",),
    argvalues=((TEST_SCRIPT,), ("/" + TEST_SCRIPT,), ((TEST_SCRIPT,),)),
)
def test_load_sql_script(sql_path: str):
    # load with a single relative path
    s = sql_helpers.load_sql_script(sql_path)
    assert len(s) > 50


def test_split_sql_script():
    sql_helpers.split_sql_script(sql_helpers.load_sql_script(TEST_SCRIPT))


def test_format_sql_script():
    sql_helpers.format_sql_script(
        sql_script=sql_helpers.load_sql_script(TEST_SCRIPT),
        sql_params_dict={"idx_name": "i1", "idx_col": "c1"},
    )


def test_prepare_sql_params(fake_airflow_context):
    sql_helpers.prepare_sql_params(
        compute_sql_params_function=lambda f: {"x": f["task_instance"]},
        airflow_context=fake_airflow_context,
    )
