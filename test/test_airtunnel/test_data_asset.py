import os
from os import path
from typing import Dict

import pandas as pd
import pytest
from airflow.hooks.dbapi_hook import DbApiHook

from airtunnel import PandasDataAsset
from airtunnel import PandasDataAssetIO
from airtunnel.data_asset import ShellDataAsset, PySparkDataAsset, SQLDataAsset


@pytest.fixture
def test_parquet_asset() -> PandasDataAsset:
    return PandasDataAsset("test_parquet_in_asset")


@pytest.fixture
def test_parquet_asset_df(
    test_parquet_asset, iris: pd.DataFrame, fake_airflow_context
) -> pd.DataFrame:
    p = path.join(
        test_parquet_asset.staging_pickedup_path(fake_airflow_context),
        "test_parquet_in.parquet",
    )
    os.makedirs(path.dirname(p), exist_ok=True)
    iris.to_parquet(p)

    return PandasDataAssetIO.read_data_asset(test_parquet_asset, source_files=[p])


def test_data_asset_paths(
    test_parquet_asset: PandasDataAsset,
    test_parquet_asset_df: pd.DataFrame,
    fake_airflow_context: Dict,
) -> None:

    # test various path getters/properties:
    test_path = test_parquet_asset.staging_pickedup_path(fake_airflow_context)
    assert isinstance(test_path, str)
    test_path = test_parquet_asset.ingest_archive_path(fake_airflow_context)
    assert isinstance(test_path, str)
    test_path = test_parquet_asset.ready_path
    assert isinstance(test_path, str)
    test_path = test_parquet_asset.staging_ready_path
    assert isinstance(test_path, str)
    test_path = test_parquet_asset.landing_path
    assert isinstance(test_path, str)
    test_path = test_parquet_asset.ready_archive_path(fake_airflow_context)
    assert isinstance(test_path, str)


def test_shell_data_asset(fake_airflow_context: Dict) -> None:
    shell_data_asset = ShellDataAsset("test_parquet_in_asset")

    with pytest.raises(NotImplementedError):
        shell_data_asset.rebuild_for_store(fake_airflow_context)
    with pytest.raises(NotImplementedError):
        shell_data_asset.retrieve_from_store()

    pandas_data_asset = shell_data_asset.to_full_data_asset(target_type=PandasDataAsset)
    assert isinstance(pandas_data_asset, PandasDataAsset)


def test_sql_data_asset(fake_airflow_context: Dict, test_db_hook: DbApiHook) -> None:
    # test if missing hook raises
    with pytest.raises(ValueError):
        # noinspection PyTypeChecker
        sql_data_asset = SQLDataAsset(name="test_parquet_in_asset", sql_hook=None)

    sql_data_asset = SQLDataAsset(name="test_parquet_in_asset", sql_hook=test_db_hook)

    with pytest.raises(FileNotFoundError):
        sql_data_asset.get_raw_sql_script(type="dml")
        sql_data_asset.get_raw_sql_script(type="ddl")

    sql_data_asset = SQLDataAsset(name="test_schema.test_table", sql_hook=test_db_hook)

    assert len(sql_data_asset.get_raw_sql_script(type="dml")) > 20
    assert len(sql_data_asset.get_raw_sql_script(type="ddl")) > 20


def test_pyspark_data_asset(fake_airflow_context) -> None:
    with pytest.raises(NotImplementedError):
        PySparkDataAsset("test")
