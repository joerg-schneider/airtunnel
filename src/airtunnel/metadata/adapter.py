import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Iterable, Dict, Union, List, Tuple, Optional

import pandas as pd
import sqlalchemy
from airflow.hooks.dbapi_hook import DbApiHook
from airflow.settings import SQL_ALCHEMY_CONN
from sqlalchemy import Table, Column, Integer, String, DateTime, MetaData, and_
from sqlalchemy.sql.ddl import DDLElement

from airtunnel.data_asset import BaseDataAsset, ShellDataAsset
from airtunnel.metadata.entities import IngestedFileMetadata, LoadStatus, Lineage

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class BaseMetaAdapter(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def setup(self):
        raise NotImplementedError

    @abstractmethod
    def write_inspected_files(self, discovered_files: Iterable[IngestedFileMetadata]):
        raise NotImplementedError

    @abstractmethod
    def read_inspected_files(
        self, for_asset: BaseDataAsset, dag_id: str, dag_exec_date: datetime
    ) -> List[IngestedFileMetadata]:
        raise NotImplementedError

    @abstractmethod
    def write_generic_metadata(self, for_asset: BaseDataAsset, payload: Dict):
        raise NotImplementedError

    @abstractmethod
    def write_lineage(self, lineage: Lineage):
        raise NotImplementedError

    @abstractmethod
    def read_lineage(
        self,
        for_target: BaseDataAsset,
        dag_id: Optional[str] = None,
        dag_exec_date: Optional[datetime] = None,
    ) -> List[Tuple[Lineage, int]]:
        # should return the complete lineage for a data asset, i.e. all sources including
        # transitive ones, stating the upstream level at each node
        raise NotImplementedError

    @abstractmethod
    def write_load_status(self, load_status: LoadStatus):
        raise NotImplementedError

    @abstractmethod
    def read_load_status(self, for_asset: BaseDataAsset) -> LoadStatus:
        raise NotImplementedError


class SQLMetaAdapter(BaseMetaAdapter):

    # we define some table names as constants:
    TN_LOAD_STATUS_HIST = "airtunnel_load_status_hist"
    TN_LOAD_STATUS = "airtunnel_load_status"
    TN_GENERIC_METADATA = "airtunnel_metadata"
    TN_LINEAGE = "airtunnel_lineage"
    TN_INFILE_METADATA = "airtunnel_ingested_files"

    TABLES = (
        TN_LOAD_STATUS,
        TN_LINEAGE,
        TN_GENERIC_METADATA,
        TN_LOAD_STATUS_HIST,
        TN_INFILE_METADATA,
    )

    # and some field names:
    FN_DATA_ASSET = "data_asset"
    FN_DATA_ASSET_SRC = "data_asset_source"
    FN_DATA_ASSET_TRG = "data_asset_target"
    FN_METADATA_TIME = "metadata_time"
    FN_METADATA_JSON = "metadata_json"
    FN_LOAD_TIME = "load_time"
    FN_METADATA_TYPE = "metadata_type"

    FN_FILE_SIZE = "size"
    FN_FILE_PATH = "path"
    FN_FILE_MOD_TIME = "modtime"
    FN_FILE_CREATE_TIME = "createtime"
    FN_DAG_ID = "dag_id"
    FN_TASK_ID = "task_id"
    FN_EXEC_DATE = "dag_exec_datetime"

    def __init__(self, sql_hook: DbApiHook = None):
        super(SQLMetaAdapter, self).__init__()
        self.t_generic_meta: Table = None
        self.t_lineage: Table = None
        self.t_load_status: Table = None
        self.t_load_status_hist: Table = None
        self.t_infile_metadata: Table = None

        if sql_hook is None:
            self.engine = sqlalchemy.create_engine(SQL_ALCHEMY_CONN)
        else:
            self.engine = sql_hook.get_sqlalchemy_engine()

        self.setup()

    def setup(self):

        metadata = MetaData()

        self.t_generic_meta = Table(
            self.TN_GENERIC_METADATA,
            metadata,
            Column(self.FN_DATA_ASSET, String, nullable=False),
            Column(self.FN_METADATA_TYPE, String, primary_key=True),
            Column(self.FN_METADATA_TIME, DateTime, primary_key=True),
            Column(self.FN_METADATA_JSON, String, nullable=False),
        )

        self.t_lineage = Table(
            self.TN_LINEAGE,
            metadata,
            Column(self.FN_DATA_ASSET_SRC, String, primary_key=True),
            Column(self.FN_DATA_ASSET_TRG, String, primary_key=True),
            Column(self.FN_METADATA_TIME, DateTime, primary_key=True),
            Column(self.FN_DAG_ID, String, primary_key=True),
            Column(self.FN_EXEC_DATE, DateTime, primary_key=True),
            Column(self.FN_TASK_ID, String, nullable=False),
        )

        self.t_load_status = Table(
            self.TN_LOAD_STATUS,
            metadata,
            Column(self.FN_DATA_ASSET, String, primary_key=True),
            Column(self.FN_LOAD_TIME, DateTime, nullable=False),
            Column(self.FN_DAG_ID, String, primary_key=True),
            Column(self.FN_EXEC_DATE, DateTime, primary_key=True),
            Column(self.FN_TASK_ID, String, nullable=False),
        )

        self.t_load_status_hist = Table(
            self.TN_LOAD_STATUS_HIST,
            metadata,
            Column(self.FN_DATA_ASSET, String, primary_key=True),
            Column(self.FN_LOAD_TIME, DateTime, primary_key=True),
            Column(self.FN_DAG_ID, String, primary_key=True),
            Column(self.FN_EXEC_DATE, DateTime, primary_key=True),
            Column(self.FN_TASK_ID, String, nullable=False),
        )

        self.t_infile_metadata = Table(
            self.TN_INFILE_METADATA,
            metadata,
            Column(self.FN_DATA_ASSET, String, primary_key=True),
            Column(self.FN_FILE_PATH, String, primary_key=True),
            Column(self.FN_DAG_ID, String, primary_key=True),
            Column(self.FN_EXEC_DATE, DateTime, primary_key=True),
            Column(self.FN_TASK_ID, String, nullable=False, primary_key=True),
            Column(self.FN_FILE_SIZE, Integer, nullable=False),
            Column(self.FN_FILE_CREATE_TIME, DateTime, nullable=False),
            Column(self.FN_FILE_MOD_TIME, DateTime, primary_key=True),
        )
        metadata.create_all(bind=self.engine)

    def write_generic_metadata(self, for_asset: BaseDataAsset, payload: Dict):
        # serialize dict as a json and write it to JSON column in postgres
        payload_json = json.dumps(obj=payload, ensure_ascii=False)

        ins = self.t_generic_meta.insert().values(
            **{
                self.FN_DATA_ASSET: for_asset.name,
                self.FN_METADATA_TYPE: "g",
                self.FN_METADATA_TIME: datetime.now(),
                self.FN_METADATA_JSON: payload_json,
            }
        )

        self._execute_on_db(statements=ins)

    def write_lineage(self, lineage: Lineage):
        inserts = []

        for src in lineage.data_sources:
            inserts.append(
                self.t_lineage.insert().values(
                    **{
                        self.FN_DATA_ASSET_SRC: src.name,
                        self.FN_DATA_ASSET_TRG: lineage.data_target.name,
                        self.FN_METADATA_TIME: datetime.now(),
                        self.FN_EXEC_DATE: lineage.dag_exec_date,
                        self.FN_TASK_ID: lineage.task_id,
                        self.FN_DAG_ID: lineage.dag_id,
                    }
                )
            )

        self._execute_on_db(statements=inserts, wrap_transaction=True)

        # self._execute(statements)
        logger.info(f"Lineage for target {lineage.data_target.name} saved.")

    def read_lineage(
        self,
        for_target: BaseDataAsset,
        dag_id: Optional[str] = None,
        dag_exec_date: Optional[datetime] = None,
    ) -> List[Tuple[Lineage, int]]:
        collected_lineage = []

        # we create a dummy entry as a starting anchor for the recursive search
        lineage_to_query = [(Lineage(data_sources=[], data_target=for_target), 0)]

        def get_lineage_sources_for_target(target_name: str) -> Iterable[Lineage]:
            select = self.t_lineage.select().where(
                self.t_lineage.c[self.FN_DATA_ASSET_TRG] == target_name
            )

            lineage_for_target = pd.read_sql(sql=select, con=self._connection())

            if dag_id is not None:
                lineage_for_target = lineage_for_target.loc[
                    lineage_for_target[self.FN_DAG_ID] == dag_id
                ]
            if dag_exec_date is not None:
                lineage_for_target = lineage_for_target.loc[
                    lineage_for_target[self.FN_EXEC_DATE] == dag_exec_date
                ]

            max_execution_date_per_dag = (
                lineage_for_target.loc[:, [self.FN_DAG_ID, self.FN_EXEC_DATE]]
                .groupby(self.FN_DAG_ID)
                .max()
            )

            lineage_for_target_latest: pd.DataFrame = lineage_for_target.merge(
                right=max_execution_date_per_dag, on=[self.FN_DAG_ID, self.FN_EXEC_DATE]
            )

            # we create a lineage entity for each unique dag_id, task_id combination for this source:
            # sort by dag_id, task_id:
            lineage_for_target_latest = lineage_for_target_latest.sort_values(
                by=[self.FN_DAG_ID, self.FN_TASK_ID], ascending=True
            )

            # now we aggregate each sources per dag_id, task_id, dag_exec_date combination:
            last_dag_id, last_task_id, last_exec_date = None, None, None
            found_sources = []
            lineage_returned = []

            for idx, row in lineage_for_target_latest.iterrows():
                cur_dag_id, cur_task_id, cur_exec_date = (
                    row[self.FN_DAG_ID],
                    row[self.FN_TASK_ID],
                    row[self.FN_EXEC_DATE],
                )
                if last_dag_id is not None and last_task_id is not None:
                    if cur_dag_id != last_dag_id or cur_task_id != last_task_id:
                        lineage_returned.append(
                            Lineage(
                                data_sources=found_sources,
                                data_target=ShellDataAsset(target_name),
                                dag_id=last_dag_id,
                                task_id=last_task_id,
                                dag_exec_date=last_exec_date,
                            )
                        )
                        found_sources = []

                found_sources.append(ShellDataAsset(row[self.FN_DATA_ASSET_SRC]))
                last_dag_id, last_task_id, last_exec_date = (
                    cur_dag_id,
                    cur_task_id,
                    cur_exec_date,
                )
            if len(found_sources) > 0:
                lineage_returned.append(
                    Lineage(
                        data_sources=found_sources,
                        data_target=ShellDataAsset(target_name),
                        dag_id=last_dag_id,
                        task_id=last_task_id,
                        dag_exec_date=last_exec_date,
                    )
                )

            return lineage_returned

        while len(lineage_to_query) > 0:
            target_to_get_lineage_for, level = lineage_to_query.pop(0)
            sources = get_lineage_sources_for_target(
                target_to_get_lineage_for.data_target.name
            )

            for s in sources:
                collected_lineage.append((s, level))
                if s.data_target != for_target:
                    lineage_to_query.append((s, level + 1))

        return collected_lineage

    def write_load_status(self, load_status: LoadStatus):
        # insert potentially existing records in
        move_to_hist = self.t_load_status_hist.insert().from_select(
            [
                self.FN_DATA_ASSET,
                self.FN_LOAD_TIME,
                self.FN_DAG_ID,
                self.FN_EXEC_DATE,
                self.FN_TASK_ID,
            ],
            self.t_load_status.select().where(
                self.t_load_status.c[self.FN_DATA_ASSET] == load_status.for_asset.name
            ),
        )

        delete_existing_load_status = self.t_load_status.delete().where(
            self.t_load_status.c[self.FN_DATA_ASSET] == load_status.for_asset.name
        )

        insert_new_load_status = self.t_load_status.insert().values(
            **{
                self.FN_DATA_ASSET: load_status.for_asset.name,
                self.FN_LOAD_TIME: load_status.load_time,
                self.FN_DAG_ID: load_status.dag_id,
                self.FN_TASK_ID: load_status.task_id,
                self.FN_EXEC_DATE: load_status.dag_exec_date,
            }
        )
        self._execute_on_db(
            statements=[
                move_to_hist,
                delete_existing_load_status,
                insert_new_load_status,
            ],
            wrap_transaction=True,
        )

    def write_inspected_files(self, discovered_files: Iterable[IngestedFileMetadata]):

        # clear any pre-existing entries for this dag-id
        # we use the first file to retrieve the common key: dag_id & dag_exec_date & task_id
        discovered_files = list(discovered_files)
        first_file = discovered_files[0]
        dag_id, dag_exec_date, task_id = (
            first_file.dag_id,
            first_file.dag_exec_date,
            first_file.task_id,
        )

        clean_query = self.t_infile_metadata.delete().where(
            and_(
                self.t_infile_metadata.c[self.FN_DAG_ID] == dag_id,
                self.t_infile_metadata.c[self.FN_EXEC_DATE] == dag_exec_date,
                self.t_infile_metadata.c[self.FN_TASK_ID] == task_id,
            )
        )
        self._connection().execute(clean_query)

        ins_queries = [
            self.t_infile_metadata.insert().values(
                **{
                    self.FN_DATA_ASSET: infile.for_asset.name,
                    self.FN_FILE_PATH: infile.filepath,
                    self.FN_DAG_ID: infile.dag_id,
                    self.FN_EXEC_DATE: infile.dag_exec_date,
                    self.FN_TASK_ID: infile.task_id,
                    self.FN_FILE_SIZE: infile.filesize,
                    self.FN_FILE_CREATE_TIME: infile.file_create_time,
                    self.FN_FILE_MOD_TIME: infile.file_mod_time,
                }
            )
            for infile in discovered_files
        ]

        self._execute_on_db(statements=ins_queries, wrap_transaction=True)

    def read_load_status(self, for_asset: BaseDataAsset) -> LoadStatus:

        query = self.t_load_status.select().where(
            self.t_load_status.c[self.FN_DATA_ASSET] == for_asset.name
        )

        results: pd.DataFrame = pd.read_sql(query, con=self._connection())

        # if no load-time available, return a very old load-time:
        if len(results) == 0:
            return LoadStatus(
                for_asset=for_asset, load_time=datetime(year=1970, month=1, day=1)
            )

        return LoadStatus(
            for_asset=for_asset,
            load_time=results[self.FN_LOAD_TIME].iloc[0],
            dag_id=results[self.FN_DAG_ID].iloc[0],
            task_id=results[self.FN_TASK_ID].iloc[0],
            dag_exec_date=results[self.FN_EXEC_DATE].iloc[0],
        )

    def read_inspected_files(
        self, for_asset: BaseDataAsset, dag_id: str, dag_exec_date: datetime
    ) -> List[IngestedFileMetadata]:

        query = self.t_infile_metadata.select().where(
            and_(
                self.t_infile_metadata.c[self.FN_DAG_ID] == dag_id,
                self.t_infile_metadata.c[self.FN_EXEC_DATE] == dag_exec_date,
                self.t_infile_metadata.c[self.FN_DATA_ASSET] == for_asset.name,
            )
        )

        return [
            IngestedFileMetadata(
                for_asset=for_asset,
                filepath=row[self.FN_FILE_PATH],
                filesize=row[self.FN_FILE_SIZE],
                file_mod_time=row[self.FN_FILE_MOD_TIME],
                file_create_time=row[self.FN_FILE_CREATE_TIME],
                dag_id=dag_id,
                dag_exec_date=dag_exec_date,
                task_id=row[self.FN_TASK_ID],
            )
            for idx, row in pd.read_sql(sql=query, con=self._connection()).iterrows()
        ]

    def _connection(self) -> sqlalchemy.engine.base.Connection:
        return self.engine.connect()

    def _execute_on_db(
        self,
        statements: Union[Iterable[DDLElement], DDLElement],
        wrap_transaction: bool = True,
    ):
        def execute_statements(conn, statements):
            if isinstance(statements, Iterable):
                for statement in statements:
                    conn.execute(statement)
            else:
                conn.execute(statements)

        conn = self._connection()

        if wrap_transaction:
            trans = conn.begin()
            execute_statements(conn, statements)
            trans.commit()

        else:
            execute_statements(conn, statements)

        conn.close()
