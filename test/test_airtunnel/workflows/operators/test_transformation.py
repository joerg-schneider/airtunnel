import pytest
from airflow.hooks.dbapi_hook import DbApiHook

from airtunnel import PandasDataAsset, SQLDataAsset
from airtunnel.operators.transformation import (
    PySparkTransformationOperator,
    SQLTransformationOperator,
)


@pytest.fixture
def test_sql_asset(test_db_hook: DbApiHook) -> SQLDataAsset:
    return SQLDataAsset("test_schema.test_table", test_db_hook)


@pytest.fixture
def test_pandas_asset() -> PandasDataAsset:
    return PandasDataAsset("test_parquet_in_asset")


def test_pyspark_transformation_operator():
    with pytest.raises(NotImplementedError):
        # noinspection PyTypeChecker
        PySparkTransformationOperator(asset=None, task_id="test")


def test_sql_transformation_operator(test_sql_asset: SQLDataAsset):
    SQLTransformationOperator(asset=test_sql_asset)
