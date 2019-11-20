import os
from os import path

import pandas as pd
import pytest

from airtunnel import PandasDataAsset, PandasDataAssetIO


@pytest.fixture
def test_csv_asset() -> PandasDataAsset:
    return PandasDataAsset("test_csv_out_asset")


@pytest.fixture
def test_xlsx_in_asset() -> PandasDataAsset:
    return PandasDataAsset("test_xlsx_in_asset")


@pytest.fixture
def test_parquet_in_asset() -> PandasDataAsset:
    return PandasDataAsset("test_parquet_in_asset")


def test_read_write_csv(test_csv_asset: PandasDataAsset, iris: pd.DataFrame) -> None:
    # try without any extra kwargs:
    PandasDataAssetIO.write_data_asset(asset=test_csv_asset, data=iris)

    # try with additional kwargs:
    PandasDataAssetIO.write_data_asset(
        asset=test_csv_asset, data=iris, header=False, index=False
    )


def test_read_write_xlsx(
    test_xlsx_in_asset: PandasDataAsset, iris: pd.DataFrame, fake_airflow_context
) -> None:
    p = path.join(
        test_xlsx_in_asset.staging_pickedup_path(fake_airflow_context),
        "test_xlsx_in.xls",
    )
    os.makedirs(path.dirname(p), exist_ok=True)
    iris.to_excel(p)

    # try without any extra kwargs:
    PandasDataAssetIO.read_data_asset(asset=test_xlsx_in_asset, source_files=[p])
    # try with additional kwargs:
    PandasDataAssetIO.read_data_asset(
        asset=test_xlsx_in_asset, source_files=[p], sheet_name=0
    )


def test_read_write_parquet(
    test_parquet_in_asset: PandasDataAsset, iris: pd.DataFrame, fake_airflow_context
) -> None:
    p = path.join(
        test_parquet_in_asset.staging_pickedup_path(fake_airflow_context),
        "test_parquet_in.parquet",
    )
    os.makedirs(path.dirname(p), exist_ok=True)
    iris.to_parquet(p)

    PandasDataAssetIO.read_data_asset(test_parquet_in_asset, source_files=[p])

    # try with additional kwargs:
    PandasDataAssetIO.read_data_asset(
        asset=test_parquet_in_asset, source_files=[p], engine="auto"
    )


def test_read_empty(test_parquet_in_asset: PandasDataAsset) -> None:
    with pytest.warns(UserWarning):
        empty = PandasDataAssetIO.read_data_asset(asset=test_parquet_in_asset, source_files=[])
        assert pd.DataFrame().equals(empty)
