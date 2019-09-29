import os
from os import path

import pandas as pd
import pytest

from airtunnel import PandasDataAsset
from airtunnel import PandasDataAssetIO


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
    test_parquet_asset, test_parquet_asset_df: pd.DataFrame, fake_airflow_context
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
