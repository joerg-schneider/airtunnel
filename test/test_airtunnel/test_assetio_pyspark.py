import os
from os import path
from typing import Any

import pandas as pd
import pyspark
import pytest
from pyspark.sql.utils import AnalysisException

from airtunnel import PySparkDataAssetIO, PySparkDataAsset


@pytest.fixture(scope="module")
def spark_session() -> pyspark.sql.SparkSession:
    spark = pyspark.sql.SparkSession.builder.appName("airtunnel").getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def test_csv_asset() -> PySparkDataAsset:
    return PySparkDataAsset("test_csv_out_asset")


@pytest.fixture
def test_parquet_in_asset() -> PySparkDataAsset:
    return PySparkDataAsset("test_parquet_in_asset")


@pytest.fixture
def iris_spark(
    iris: pd.DataFrame, spark_session: pyspark.sql.SparkSession
) -> pyspark.sql.DataFrame:
    return spark_session.createDataFrame(iris)


def test_read_write_csv(
    test_csv_asset: PySparkDataAsset,
    iris_spark: pyspark.sql.DataFrame,
    spark_session: pyspark.sql.SparkSession,
) -> None:
    # try without any extra kwargs:
    PySparkDataAssetIO.write_data_asset(asset=test_csv_asset, data=iris_spark)

    # try with additional kwargs:
    PySparkDataAssetIO.write_data_asset(
        asset=test_csv_asset, data=iris_spark, header=True
    )

    # test mode; default is overwrite, switch to error (if exists) should raise:
    with pytest.raises(AnalysisException):
        PySparkDataAssetIO.write_data_asset(
            asset=test_csv_asset, data=iris_spark, header=True, mode="error"
        )

    # test retrieval
    # Test check for 'spark' kwarg
    with pytest.raises(ValueError):
        PySparkDataAssetIO.retrieve_data_asset(test_csv_asset)


def test_read_write_parquet(
    test_parquet_in_asset: PySparkDataAsset,
    iris_spark: pyspark.sql.DataFrame,
    fake_airflow_context: Any,
    spark_session: pyspark.sql.SparkSession,
) -> None:
    p = path.abspath(path.join(test_parquet_in_asset.staging_pickedup_path(fake_airflow_context)))
    os.makedirs(path.dirname(p), exist_ok=True)
    iris_spark.write.mode("overwrite").parquet(p)

    count_before = iris_spark.count()
    columns_before = len(iris_spark.columns)

    with pytest.raises(expected_exception=ValueError):
        PySparkDataAssetIO.read_data_asset(test_parquet_in_asset, source_files=[p])

    x = PySparkDataAssetIO.read_data_asset(
        test_parquet_in_asset, source_files=[p], spark=spark_session
    )

    assert count_before == x.count()
    assert columns_before == len(x.columns)

    # try with additional kwargs:
    x = PySparkDataAssetIO.read_data_asset(
        asset=test_parquet_in_asset,
        source_files=[p],
        spark=spark_session,
        mergeSchema=True,
    )

    assert count_before == x.count()


def test_read_empty(test_parquet_in_asset: PySparkDataAsset) -> None:
    # todo: consider how to do this
    pass
