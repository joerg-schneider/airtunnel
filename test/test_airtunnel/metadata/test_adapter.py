import os
from datetime import datetime, timedelta
from typing import Optional

import pytest
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.dbapi_hook import DbApiHook
from airflow.hooks.sqlite_hook import SqliteHook

from airtunnel.data_asset import ShellDataAsset
from airtunnel.metadata.adapter import (
    BaseMetaAdapter,
    SQLMetaAdapter,
    get_configured_meta_adapter,
    get_configured_meta_adapter_hook,
    BaseHookFactory,
    META_ADAPTER_HOOK_FACTORY_ENV_NAME,
)
from airtunnel.metadata.entities import Lineage, LoadStatus
from test_airtunnel import test_utils
from test_airtunnel.test_utils import table_rowcount, DUMMY_TABLE, DUMMY_TABLE2


def _clear_meta_tables(test_db_hook: DbApiHook) -> None:
    for t in SQLMetaAdapter.TABLES:
        test_utils.run_on_db(test_db_hook=test_db_hook, sql=f"delete from {t}")


@pytest.fixture(scope="module")
def test_meta_adapter(test_db_hook) -> BaseMetaAdapter:
    return SQLMetaAdapter(sql_hook=test_db_hook)


class TestCustomHookFactory(BaseHookFactory):
    """ A test co"""

    @staticmethod
    def make_hook() -> Optional[BaseHook]:
        """ Makes a SqliteHook to test with. """
        return SqliteHook()


def test_get_configured_meta_adapter_hook():
    assert get_configured_meta_adapter_hook() is None
    os.environ[
        META_ADAPTER_HOOK_FACTORY_ENV_NAME
    ] = "test_airtunnel.metadata.test_adapter.TestCustomHookFactory"
    assert get_configured_meta_adapter_hook() is not None and isinstance(
        get_configured_meta_adapter_hook(), SqliteHook
    )


def test_get_configured_meta_adapter():
    assert isinstance(get_configured_meta_adapter(), BaseMetaAdapter) and isinstance(
        get_configured_meta_adapter(), SQLMetaAdapter
    )


def test_afdb_setup(test_meta_adapter: BaseMetaAdapter, test_db_hook: DbApiHook):
    test_utils.run_on_db(
        test_db_hook=test_db_hook,
        sql=f"select * from {','.join(SQLMetaAdapter.TABLES)} ",
    )
    _clear_meta_tables(test_db_hook)


def test_afdb_lineage(test_meta_adapter: BaseMetaAdapter, test_db_hook: DbApiHook):
    test_meta_adapter.write_lineage(
        Lineage(
            data_target=ShellDataAsset(DUMMY_TABLE2),
            data_sources=[ShellDataAsset(DUMMY_TABLE)],
            dag_id="test_dag",
            task_id="test_task",
            dag_exec_date=datetime.now(),
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
        LoadStatus(
            for_asset=ShellDataAsset(DUMMY_TABLE),
            load_time=datetime.now(),
            dag_id="test_dag",
            task_id="test_task",
            dag_exec_date=datetime.now(),
        )
    )

    test_meta_adapter.write_load_status(
        LoadStatus(
            for_asset=ShellDataAsset(DUMMY_TABLE),
            load_time=datetime.now() + timedelta(days=1),
            dag_id="test_dag",
            task_id="test_task",
            dag_exec_date=datetime.now(),
        )
    )

    test_meta_adapter.write_load_status(
        LoadStatus(
            for_asset=ShellDataAsset(DUMMY_TABLE),
            load_time=datetime.now() + timedelta(days=2),
            dag_id="test_dag",
            task_id="test_task",
            dag_exec_date=datetime.now(),
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


def test_generic_metadata(test_meta_adapter: BaseMetaAdapter):
    payload = {"my_data": "my_value", "test_numerid": 42}
    test_meta_adapter.write_generic_metadata(
        for_asset=ShellDataAsset(DUMMY_TABLE), payload=payload
    )
