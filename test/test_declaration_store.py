import logging
import os
from os import path

import pytest

from airtunnel.declaration_store import DataAssetDeclaration
from airtunnel.declaration_store import (
    K_ASSET_TYPE,
    V_TYPE_INGESTED,
    DECL_FILE_SUFFIX,
    SECTION_INGEST,
    K_IN_STORAGE_FORMAT,
    K_FILE_INPUT_GLOB,
    K_ARCH_INGEST,
    SECTION_LOAD,
    K_OUT_FORMAT,
    K_ARCH_READY,
    K_OUT_COMP_CODEC,
    K_STAGING_ASSETS,
)
from airtunnel.paths import P_DECLARATIONS

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

TEST_ASSET_NAME = "dummy_1"


@pytest.fixture(scope="function")
def test_data_asset() -> str:

    asset_file = path.join(P_DECLARATIONS, TEST_ASSET_NAME + DECL_FILE_SUFFIX)
    with open(asset_file, mode="wt") as d:
        d.write(
            f"""
{K_ASSET_TYPE}: {V_TYPE_INGESTED}

{SECTION_INGEST}:
    {K_IN_STORAGE_FORMAT}: csv
    {K_FILE_INPUT_GLOB}: test_*.csv
    {K_ARCH_INGEST}: no

{SECTION_LOAD}:
    {K_OUT_FORMAT}: parquet
    {K_OUT_COMP_CODEC}: gzip
    {K_ARCH_READY}: yes

{K_STAGING_ASSETS}:
    - stage_asset_tmp1
    - stage_asset_tmp2
    - stage_asset_tmp3
"""
        )
    yield TEST_ASSET_NAME
    os.remove(asset_file)


def test_ingested_data_asset_decl(test_data_asset) -> None:
    d = DataAssetDeclaration(data_asset=test_data_asset)
    # test the various properties:

    assert d.archive_ingest == False
    assert d.archive_ready == True
    assert d.ingest_file_glob == "test_*.csv"
    assert d.in_storage_format == "csv"
    assert d.out_comp_codec == "gzip"
    assert d.out_storage_format == "parquet"

    with pytest.raises(ValueError):
        x = d.transform_renames

    with pytest.raises(ValueError):
        x = d.transform_date_formats

    assert len(d.staging_assets) == 3


def test_non_existent_asset() -> None:
    with pytest.raises(expected_exception=FileNotFoundError):
        DataAssetDeclaration(data_asset="does_not_exist")
