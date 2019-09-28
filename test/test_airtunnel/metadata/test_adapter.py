from datetime import datetime, timedelta

import pytest
from airflow.hooks.dbapi_hook import DbApiHook

from airtunnel.data_asset import ShellDataAsset
from airtunnel.metadata.adapter import BaseMetaAdapter, SQLMetaAdapter
from airtunnel.metadata.entities import Lineage, LoadStatus
from test import test_utils
from test.test_utils import table_rowcount, DUMMY_TABLE, DUMMY_TABLE2


@pytest.fixture(scope="module")
def test_meta_adapter(test_db_hook) -> BaseMetaAdapter:
    return SQLMetaAdapter(sql_hook=test_db_hook)


def test_afdb_setup(test_meta_adapter: BaseMetaAdapter, test_db_hook: DbApiHook):
    test_utils.run_on_db(
        test_db_hook=test_db_hook,
        sql=f"select * from {','.join(SQLMetaAdapter.TABLES)} ",
    )


def test_afdb_lineage(test_meta_adapter: BaseMetaAdapter, test_db_hook: DbApiHook):
    test_meta_adapter.write_lineage(
        Lineage(
            data_target=ShellDataAsset(DUMMY_TABLE2),
            data_sources=[ShellDataAsset(DUMMY_TABLE)],
        )
    )

    results = test_utils.run_on_db(
        test_db_hook=test_db_hook,
        sql=f"""
    select 
        {SQLMetaAdapter.FN_DATA_ASSET_SRC}, 
        {SQLMetaAdapter.FN_METADATA_TIME}
    from {SQLMetaAdapter.TN_LINEAGE}
    where {SQLMetaAdapter.FN_DATA_ASSET_TRG} = '{DUMMY_TABLE2}'
    """,
    )

    assert len(results) == 1
    assert results.loc[:, SQLMetaAdapter.FN_DATA_ASSET_SRC].iloc[0] == DUMMY_TABLE


def test_afdb_load_status(test_meta_adapter: BaseMetaAdapter, test_db_hook: DbApiHook):
    test_meta_adapter.write_load_status(
        LoadStatus(for_asset=ShellDataAsset(DUMMY_TABLE), load_time=datetime.now())
    )

    test_meta_adapter.write_load_status(
        LoadStatus(
            for_asset=ShellDataAsset(DUMMY_TABLE),
            load_time=datetime.now() + timedelta(days=1),
        )
    )

    test_meta_adapter.write_load_status(
        LoadStatus(
            for_asset=ShellDataAsset(DUMMY_TABLE),
            load_time=datetime.now() + timedelta(days=2),
        )
    )

    # check row-counts after two updates to the load status metadata table
    predicate = f"{SQLMetaAdapter.FN_DATA_ASSET} = '{DUMMY_TABLE}'"
    rc = table_rowcount(
        test_db_hook=test_db_hook,
        table=SQLMetaAdapter.TN_LOAD_STATUS,
        predicate=predicate,
    )

    # we expect exactly one entry in the master load status table:
    assert rc == 1

    rc_hist = table_rowcount(
        test_db_hook=test_db_hook,
        table=SQLMetaAdapter.TN_LOAD_STATUS_HIST,
        predicate=predicate,
    )
    # we expect at least 2 entries here now:
    assert rc_hist > 1
