from datetime import datetime, timedelta
from typing import Tuple

import pytest

from airtunnel.data_asset import ShellDataAsset
from airtunnel.metadata.adapter import SQLMetaAdapter, BaseMetaAdapter
from airtunnel.metadata.entities import Lineage, LoadStatus, IngestedFileMetadata
from test_airtunnel.test_utils import DUMMY_TABLE, DUMMY_TABLE2


@pytest.fixture(scope="function")
def load_status() -> LoadStatus:
    yield LoadStatus(
        for_asset=ShellDataAsset(DUMMY_TABLE2),
        load_time=datetime.now(),
        dag_id="test_dag",
        task_id="test_task",
        dag_exec_date=datetime.now(),
    )


@pytest.fixture
def adapter(test_db_hook) -> BaseMetaAdapter:
    return SQLMetaAdapter(sql_hook=test_db_hook)


@pytest.fixture(scope="function")
def lineage() -> Lineage:
    yield Lineage(
        data_sources=[ShellDataAsset(DUMMY_TABLE)],
        data_target=ShellDataAsset(DUMMY_TABLE2),
        dag_id="test_dag",
        task_id="test_task",
        dag_exec_date=datetime.now(),
    )


@pytest.fixture(scope="function")
def ingested_file_metadata() -> IngestedFileMetadata:
    yield IngestedFileMetadata(
        for_asset=ShellDataAsset(DUMMY_TABLE),
        filepath="test_path.csv",
        filesize=100,
        file_mod_time=datetime.now(),
        file_create_time=datetime.now(),
        dag_id="test_dag",
        dag_exec_date=datetime.now(),
        task_id="test_task_id",
    )


def test_load_status_cls(load_status: LoadStatus, adapter: BaseMetaAdapter) -> None:
    load_status_time = datetime.now()
    adapter.write_load_status(load_status)
    status = adapter.read_load_status(for_asset=load_status.for_asset)
    read_ls_time = status.load_time

    # check that the returned datetime is within bounds of 1 second:
    assert load_status_time - timedelta(seconds=1) < read_ls_time + timedelta(seconds=1)
    assert status.is_within(frame=timedelta(seconds=20))
    # repr:
    x = "t" + str(status)


def test_ingested_file_metadata_cls(
    ingested_file_metadata: IngestedFileMetadata, adapter: BaseMetaAdapter
) -> None:
    adapter.write_inspected_files([ingested_file_metadata])
    inspected_files_read = adapter.read_inspected_files(
        for_asset=ingested_file_metadata._for_asset,
        dag_id=ingested_file_metadata._dag_id,
        dag_exec_date=ingested_file_metadata._dag_exec_date,
    )

    assert len(inspected_files_read) == 1
    assert inspected_files_read[0]._for_asset == ingested_file_metadata._for_asset

    # repr:
    x = "t" + str(inspected_files_read)


def test_lineage_cls(lineage: Lineage, adapter: BaseMetaAdapter) -> None:
    adapter.write_lineage(lineage)
    retrieved_lineage = adapter.read_lineage(lineage.data_target)
    assert len(retrieved_lineage) > 0
    assert retrieved_lineage[0][0].data_target == lineage.data_target
    assert retrieved_lineage[0][0].data_sources == lineage.data_sources

    # repr:
    x = "t" + str(retrieved_lineage)


# -----
# tests below test lineage parsing


@pytest.fixture
def test_sql1() -> Tuple[str, Lineage]:
    return (
        """
       insert into table table5
       select * from table3

    """,
        Lineage(
            data_sources=[ShellDataAsset("table3")],
            data_target=ShellDataAsset("table5"),
        ),
    )


@pytest.fixture
def test_sql2() -> Tuple[str, Lineage]:
    return (
        """
        INSERT OVERWRITE TABLE table4
        select
            from
             table1 t1 join table2 t2
            on t1.key = t2.key
        where exist (select 1 from table3 as t3 where t3.key = t2.fkey


    """,
        Lineage(
            data_sources=ShellDataAsset.from_names(["table1", "table2", "table3"]),
            data_target=ShellDataAsset("table4"),
        ),
    )


def test_lineage(test_sql1, test_sql2):
    for test_sql, expected_lineage in (test_sql1, test_sql2):
        assert (
            Lineage.lineage_from_sql_statement(
                test_sql, known_data_assets=[f"table{n}" for n in range(0, 10)]
            )
            == expected_lineage
        )
