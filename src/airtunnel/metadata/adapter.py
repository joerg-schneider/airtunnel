""" Module defining Airtunnel metadata adapters. """
import importlib
import json
import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Iterable, Dict, Union, List, Tuple, Optional

import pandas as pd
import sqlalchemy
from airflow import conf
from airflow.exceptions import AirflowConfigException
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.dbapi_hook import DbApiHook
from airflow.settings import SQL_ALCHEMY_CONN
from sqlalchemy import Table, Column, Integer, String, DateTime, MetaData, and_
from sqlalchemy.sql.ddl import DDLElement

from airtunnel.data_asset import BaseDataAsset, ShellDataAsset
from airtunnel.metadata.entities import IngestedFileMetadata, LoadStatus, Lineage

META_ADAPTER_CLASS_ENV_NAME = "AIRFLOW__AIRTUNNEL__META_ADAPTER_CLASS"
META_ADAPTER_HOOK_FACTORY_ENV_NAME = "AIRFLOW__AIRTUNNEL__META_ADAPTER_HOOK_FACTORY"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_ADAPTER_CLASS = "airtunnel.metadata.adapter.SQLMetaAdapter"
DEFAULT_HOOK_FACTORY = "airtunnel.metadata.adapter.DefaultSQLHookFactory"


class BaseMetaAdapter(ABC):
    """ Base class for all Airtunnel MetaAdapters. """

    def __init__(self):
        pass

    @abstractmethod
    def _setup(self):
        """ Method called on initialization of a MetaAdapter implementation. """
        raise NotImplementedError

    @abstractmethod
    def write_inspected_files(
        self, discovered_files: Iterable[IngestedFileMetadata]
    ) -> None:
        """
        Log metadata of discovered files of the ingestion process.

        :param discovered_files: iterable of metadata entities for all discovered files
        :return: None
        """
        raise NotImplementedError

    @abstractmethod
    def read_inspected_files(
        self, for_asset: BaseDataAsset, dag_id: str, dag_exec_date: datetime
    ) -> List[IngestedFileMetadata]:
        """
        Read existing metadata of previously inspected files.

        :param for_asset: a data asset for which to retrieve the metadata for
        :param dag_id: an Airflow DAG ID for which to retrieve the metadata for
        :param dag_exec_date: an Airflow DAG execution date for which to retrieve the metadata for
        :return: list of metadata entities
        """
        raise NotImplementedError

    @abstractmethod
    def write_generic_metadata(self, for_asset: BaseDataAsset, payload: Dict) -> None:
        """
        Log generic metadata in JSON format.

        :param for_asset: a data asset for which to retrieve the metadata for
        :param payload: the generic metadata to log – needs to be serializable as JSON
        :return: None
        """
        raise NotImplementedError

    @abstractmethod
    def write_lineage(self, lineage: Lineage) -> None:
        """
        Log lineage information.

        :param lineage: the lineage entity to log.
        :return: None
        """
        raise NotImplementedError

    @abstractmethod
    def read_lineage(
        self,
        for_target: BaseDataAsset,
        dag_id: Optional[str] = None,
        dag_exec_date: Optional[datetime] = None,
    ) -> List[Tuple[Lineage, int]]:
        """
        Read previously logged lineage metadata.

        :param for_target: the data asset for which to retrieve lineage information for, here the data target
        :param dag_id: the DAG ID for which to restrict the lineage search on (optional)
        :param dag_exec_date: the DAG execution datetime for which to restrict the lineage search on (optional)
        :return: the lineage information captured as a list of tuples, in which the first element is a lineage metadata
                 entity and the second an int of the upstream level in the lineage chain
        """
        # should return the complete lineage for a data asset, i.e. all sources including
        # transitive ones, stating the upstream level at each node
        raise NotImplementedError

    @abstractmethod
    def write_load_status(self, load_status: LoadStatus) -> None:
        """
        Log load status metadata.

        :param load_status: the load status entity to log.
        :return: None
        """
        raise NotImplementedError

    @abstractmethod
    def read_load_status(self, for_asset: BaseDataAsset) -> LoadStatus:
        """
        Read previously logged load status for a data asset.

        :param for_asset: data asset for which to retrieve the load status information
        :return: the load status metadata entity
        """
        raise NotImplementedError


class SQLMetaAdapter(BaseMetaAdapter):
    """ BaseMetaAdapter implementation using SQLAlchemy, hence compatible with relational databases that SQLAlchemy
    supports. The database connection that is used for metadata logging (=SQLAlchemy engine) can be customized through
    an Airflow DbApiHook that is either explicitly given as part of the constructor, or configured using the Airtunnel
    configuration parameter: ``meta_adapter_hook_factory``.

    If neither of these two define a DbApiHook to use, SQLMetaAdapter will use Airflow's backend database by leveraging
    the configured SQLAlchemy connection string and creating the engine based on it.
    """

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

        if sql_hook is not None:
            # a sql hook was passed into the constructor – it takes highest precedence
            self._check_hook_type(sql_hook)
            self.engine = sql_hook.get_sqlalchemy_engine()
        else:

            configured_custom_hook = get_configured_meta_adapter_hook()

            if configured_custom_hook is not None:
                # a configured custom hook was returned by the factory:
                configured_custom_hook = self._check_hook_type(configured_custom_hook)
                self.engine = configured_custom_hook.get_sqlalchemy_engine()
            else:
                # nothing was defined, create engine based on the default Airflow conn:
                self.engine = sqlalchemy.create_engine(SQL_ALCHEMY_CONN)

        self._is_set_up = False

    @staticmethod
    def _check_hook_type(hook: BaseHook) -> DbApiHook:
        if not isinstance(hook, DbApiHook):
            raise TypeError(
                f"The defined/passed hook for SQLMetaAdapter needs to be an 'DbApiHook'"
            )
        return hook

    def _setup(self):
        """ Initial setup for this adapter, creates the tables on the DB if missing, using SQLAlchemy."""

        if self._is_set_up:
            return

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

        self._is_set_up = True

    def write_generic_metadata(self, for_asset: BaseDataAsset, payload: Dict):
        """
        Log generic metadata in JSON format.

        :param for_asset: a data asset for which to retrieve the metadata for
        :param payload: the generic metadata to log – needs to be serializable as JSON
        :return: None
        """
        # serialize dict as a json and write it to JSON column in database
        payload_json = json.dumps(obj=payload, ensure_ascii=False)

        self._setup()

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
        """
        Log lineage information.

        :param lineage: the lineage entity to log.
        :return: None
        """
        self._setup()

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

        self._execute_on_db(statements=inserts)

        # self._execute(statements)
        logger.info(f"Lineage for target {lineage.data_target.name} saved.")

    def read_lineage(
        self,
        for_target: BaseDataAsset,
        dag_id: Optional[str] = None,
        dag_exec_date: Optional[datetime] = None,
    ) -> List[Tuple[Lineage, int]]:
        """
        Read previously logged lineage metadata.

        :param for_target: the data asset for which to retrieve lineage information for, here the data target.
        :param dag_id: the DAG ID for which to restrict the lineage search on (optional)
        :param dag_exec_date: the DAG execution datetime for which to restrict the lineage search on (optional)
        :return: the lineage information captured as a list of tuples, in which the first element is a lineage metadata
                 entity and the second an int of the upstream level in the lineage chain
        """

        collected_lineage = []

        self._setup()

        # we create a dummy entry as a starting anchor for the recursive search
        lineage_to_query = [(Lineage(data_sources=[], data_target=for_target), 0)]

        def _get_lineage_sources_for_target(target_name: str) -> Iterable[Lineage]:
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
                by=[self.FN_DAG_ID, self.FN_TASK_ID]
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
            sources = _get_lineage_sources_for_target(
                target_to_get_lineage_for.data_target.name
            )

            for s in sources:
                collected_lineage.append((s, level))
                if s.data_target != for_target:
                    lineage_to_query.append((s, level + 1))

        return collected_lineage

    def write_load_status(self, load_status: LoadStatus):
        """
        Log load status metadata.

        :param load_status: the load status entity to log.
        :return: None
        """
        self._setup()

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
            ]
        )

    def write_inspected_files(
        self, discovered_files: Iterable[IngestedFileMetadata]
    ) -> None:
        """
        Log metadata of discovered files of the ingestion process.

        :param discovered_files: iterable of metadata entities for all discovered files
        :return: None
        """
        self._setup()
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

        self._execute_on_db(statements=ins_queries)

    def read_load_status(self, for_asset: BaseDataAsset) -> LoadStatus:
        """
        Read previously logged load status for a data asset.

        :param for_asset: data asset for which to retrieve the load status information
        :return: the load status metadata entity
        """
        self._setup()
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
        """
        Read existing metadata of previously inspected files.

        :param for_asset: a data asset for which to retrieve the metadata for
        :param dag_id: an Airflow DAG ID for which to retrieve the metadata for
        :param dag_exec_date: an Airflow DAG execution date for which to retrieve the metadata for
        :return: list of metadata entities
        """
        self._setup()
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
        def _execute_statements(conn_, statements_):
            if isinstance(statements_, Iterable):
                for statement in statements_:
                    conn_.execute(statement)
            else:
                conn_.execute(statements_)

        conn = self._connection()

        if wrap_transaction:
            trans = conn.begin()
            _execute_statements(conn, statements)
            trans.commit()

        else:
            _execute_statements(conn, statements)

        conn.close()


class BaseHookFactory:
    """ Base class to define the interface we expect for a custom hook factory. """

    @staticmethod
    @abstractmethod
    def make_hook() -> Optional[BaseHook]:
        """ Creates and returns an Airflow hook that should be used to log metadata. """
        raise NotImplementedError


class DefaultSQLHookFactory(BaseHookFactory):
    """ Default (dummy) hook factory used with the SQLMetaAdapter if nothing custom is defined. """

    @staticmethod
    def make_hook() -> Optional[BaseHook]:
        """ This implementation does not create a hook, but returns 'None' so that the SQLMetaAdapter falls back to
        simply creating a SQLAlchemy engine based on Airflow's configured SQLAlchemy connection string.
        We do not attempt to create a hook in this scenario, as we can not know for certain the type of database backend
        one uses with their Airflow setup, and therefor we can not safely chose and instantiate the hook of the right
        type.
        """
        return None


def get_configured_meta_adapter() -> BaseMetaAdapter:
    """
    Gets the configured (or default) BaseMetaAdapter to use for metadata operations.

    It can be defined using the config key ``meta_adapter_class`` with a value that
    points to a class implementing ``airtunnel.metadata.adapter.BaseMetaAdapter``. This class
    will then be retrieved and used as the metadata adapter in Airtunnel's Airflow operators.

    :return: the configured (or default) BaseMetaAdapter
    """
    try:
        meta_adapter_class = conf.get(section="airtunnel", key="meta_adapter_class")
    except AirflowConfigException:
        logger.warning(
            f"'meta_adapter_class' for Airtunnel not configured in airflow.cfg – using default"
        )
        # set the default config as part of the environment, to hide future AirflowConfigExceptions:
        os.environ[META_ADAPTER_CLASS_ENV_NAME] = DEFAULT_ADAPTER_CLASS
        meta_adapter_class = DEFAULT_ADAPTER_CLASS

    module, cls = meta_adapter_class.rsplit(".", maxsplit=1)
    mod = importlib.import_module(name=module)
    adapter_class = getattr(mod, cls)
    return adapter_class()


def get_configured_meta_adapter_hook() -> Optional[BaseHook]:
    """
    Gets the configured hook to use for metadata operations.

    It can be defined using the config key ``meta_adapter_hook_factory`` with a value that
    points to a class implementing ``airtunnel.metadata.adapter.BaseHookFactory``. This class
    will then be retrieved and the method ``make_hook()`` will be called to get the hook.

    If nothing defined, ``airtunnel.metadata.adapter.DefaultSQLHookFactory`` is used, which returns 'None'
    for a hook – letting an implemented metadata adapter proceed to create their own default hook/connection.

    :return: a BaseHook to use for metadata operations or 'None' if not defined
    """
    try:
        meta_hook_factory = conf.get(
            section="airtunnel", key="meta_adapter_hook_factory"
        )
    except AirflowConfigException:
        logger.warning(
            f"'meta_adapter_hook_factory' for Airtunnel not configured in airflow.cfg – using default"
        )
        # set the default config as part of the environment, to hide future AirflowConfigExceptions:
        os.environ[META_ADAPTER_HOOK_FACTORY_ENV_NAME] = DEFAULT_HOOK_FACTORY
        meta_hook_factory = DEFAULT_HOOK_FACTORY

    module, cls = meta_hook_factory.rsplit(".", maxsplit=1)
    mod = importlib.import_module(name=module)
    factory: BaseHookFactory = getattr(mod, cls)
    return factory.make_hook()
