import importlib
import logging
import os
from abc import abstractmethod
from os import path
from typing import Optional, Union, Iterable, Dict, List

import pandas as pd
from airflow.hooks.dbapi_hook import DbApiHook
from airflow.models import TaskInstance

import airtunnel
from airtunnel.declaration_store import DataAssetDeclaration, V_FORMAT_PARQUET
from airtunnel.operators.sql import sqloperator
from airtunnel.paths import (
    P_DATA_READY,
    P_DATA_STAGING_PICKEDUP,
    P_DATA_STAGING_READY,
    P_DATA_INGEST_ARCHIVE,
    P_DATA_INGEST_LANDING,
    P_DATA_ARCHIVE,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class BaseDataAsset:
    __slots__ = ["name", "declarations"]

    def __init__(self, name: str) -> None:
        self.name = name
        # a ShellDataAsset is a special kind of wrapper which is purely name based and skips decl. loading
        # (useful for testing, capturing lineage, or similar tasks where declarations are not needed)
        if not isinstance(self, ShellDataAsset):
            self.declarations: DataAssetDeclaration = DataAssetDeclaration(
                data_asset=name
            )

    def __load_declarations(self) -> Optional[DataAssetDeclaration]:
        return DataAssetDeclaration(self.name)

    @property
    def ready_path(self) -> str:
        return os.path.join(P_DATA_READY, self.name)

    def make_ready_temp_path(self, airflow_context: Dict) -> str:
        return os.path.join(
            P_DATA_READY,
            "." + str(airflow_context["task_instance"].execution_date) + self.name,
        )

    @property
    def landing_path(self) -> str:
        return os.path.join(P_DATA_INGEST_LANDING, self.name)

    def ingest_archive_path(self, airflow_context) -> str:
        return os.path.join(P_DATA_INGEST_ARCHIVE, self.name)

    def staging_pickedup_path(self, airflow_context) -> str:
        return os.path.join(
            P_DATA_STAGING_PICKEDUP,
            self.name,
            str(airflow_context["task_instance"].execution_date),
        )

    @property
    def staging_ready_path(self) -> str:
        p = os.path.join(P_DATA_STAGING_READY, self.name)
        os.makedirs(p, exist_ok=True)
        return p

    def ready_archive_path(self, airflow_context) -> str:
        return os.path.join(
            P_DATA_ARCHIVE,
            self.name,
            str(airflow_context["task_instance"].execution_date),
        )

    def pickedup_files(self, airflow_context) -> List[str]:

        ti: TaskInstance = airflow_context["task_instance"]

        descovered_files = ti.xcom_pull(
            dag_id=ti.dag_id, key=self.discovered_files_xcom_key
        )

        return [
            f.replace(self.landing_path, self.staging_pickedup_path(airflow_context))
            for f in descovered_files
        ]

    @property
    def discovered_files_xcom_key(self):
        from airtunnel.sensors.ingestion import K_DISCOVERED_FILES

        return f"{K_DISCOVERED_FILES}_{self.name}"

    @property
    def output_filename(self):
        return self.name + "." + self.declarations.out_storage_format

    @abstractmethod
    def rebuild_for_store(self, airflow_context, **kwargs):
        pass

    @abstractmethod
    def retrieve_from_store(self):
        pass


class PandasDataAsset(BaseDataAsset):
    def retrieve_from_store(
        self, airflow_context=None, consuming_asset: Optional[BaseDataAsset] = None
    ) -> pd.DataFrame:

        #  attempt to log lineage
        try:
            if consuming_asset is not None:
                # have to have these imports here to avoid cross-import issues:
                from airtunnel.metadata.adapter import SQLMetaAdapter
                from airtunnel.metadata.entities import Lineage

                if airflow_context is not None:
                    task_instance: TaskInstance = airflow_context["task_instance"]
                    dag_id = task_instance.dag_id
                    task_id = task_instance.task_id
                    dag_exec_date = task_instance.execution_date
                else:
                    dag_id = None
                    task_id = None
                    dag_exec_date = None

                db = SQLMetaAdapter()
                db.write_lineage(
                    Lineage(
                        data_sources=[self],
                        data_target=consuming_asset,
                        dag_id=dag_id,
                        task_id=task_id,
                        dag_exec_date=dag_exec_date,
                    )
                )
                logger.info(
                    f"Lineage from {self.name} to {consuming_asset.name} recorded"
                )
        except Exception as e:
            logger.warning(f"Error on recording lineage: {e}")

        # todo: this should here switch formats depending on declaration
        if self.declarations.out_storage_format == V_FORMAT_PARQUET:
            return pd.read_parquet(self.ready_path)

    def rebuild_for_store(self, airflow_context, **kwargs):
        # we delegate the rebuild of this data asset to the Pandas script
        script_path = path.join(airtunnel.paths.P_SCRIPTS_PY, self.name + ".py")
        try:
            spec = importlib.util.spec_from_file_location("module.name", script_path)
            asset_script = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(asset_script)
        except Exception as e:
            logger.error(f"Could not load Pandas script expected at {script_path} ")
            raise e

        asset_script.rebuild_for_store(asset=self, airflow_context=airflow_context)

    def rename_fields_as_declared(self, data: pd.DataFrame) -> pd.DataFrame:
        rename_map = {c: c for c in data.columns}
        rename_map.update(self.declarations.transform_renames)
        logger.info(f"Renaming according to: {rename_map}")
        return data.rename(columns=rename_map)


class PySparkDataAsset(BaseDataAsset):
    # todo: implement
    pass


class SQLDataAsset(BaseDataAsset):
    def __init__(self, name: str, sql_hook: DbApiHook, db_schema: str = None):
        if sql_hook is None:
            raise ValueError("Need a DbApiHook to instantiate the SQLDataAsset")

        self._sql_hook = sql_hook
        self._db_schema = db_schema
        super(SQLDataAsset, self).__init__(name=name)

    def get_raw_sql_script(self, type: str = "dml") -> str:
        from scripts import sql

        sql_location = path.join(path.dirname(sql.__file__), type, self.name + ".sql")

        if not path.exists(sql_location):
            raise FileNotFoundError(f"SQL script expected at {sql_location} not found!")

        logger.info("Loading SQL script from: %s" % sql_location)

        with open(sql_location, "r") as sql_file:
            loaded_sql_script = sql_file.read() + "\n\n"

        return loaded_sql_script

    def formatted_sql_script(
        self, parameters, dynamic_parameters=None, airflow_context=None
    ):

        loaded_sql_script = self.get_raw_sql_script()

        if dynamic_parameters is not None:
            parameters.update(
                sqloperator.prepare_sql_params(
                    compute_sql_params_function=dynamic_parameters,
                    airflow_context=airflow_context,
                )
            )

        if parameters is not None:
            loaded_sql_script = sqloperator.format_sql_script(
                sql_script=loaded_sql_script, sql_params_dict=parameters
            )

        return loaded_sql_script

    def rebuild_for_store(self, airflow_context, **kwargs):
        formatted_sql_script = self.formatted_sql_script(
            parameters=kwargs.get("parameters", None),
            dynamic_parameters=kwargs.get("dynamic_parameters", None),
            airflow_context=airflow_context,
        )

        script_statements = sqloperator.split_sql_script(formatted_sql_script)

        sqloperator.execute_script(
            connection=self._sql_hook.get_cursor(), statements=script_statements
        )

    def retrieve_from_store(self) -> pd.DataFrame:
        schema_prefix = "" if self.db_schema is None else self.db_schema + "."
        return pd.read_sql(
            sql=f"select * from {schema_prefix}{self.name}",
            con=self._sql_hook.get_conn(),
        )

    @property
    def db_schema(self):
        return self._db_schema


class ShellDataAsset(BaseDataAsset):
    """ A shell DataAsset, acting merely as a container - i.e. for lineage. """

    def __init__(self, name: str):
        super().__init__(name=name)

    def __eq__(self, other):
        return self.name == other.name

    def __load_declarations(self) -> Optional[DataAssetDeclaration]:
        """ This method is replaced to skip loading of declarations. """
        return None

    def to_full_data_asset(
        self, target_type: Union[PySparkDataAsset.__class__, PandasDataAsset.__class__]
    ) -> Union[PySparkDataAsset, PandasDataAsset]:
        return target_type(name=self.name)

    @staticmethod
    def from_names(names: Iterable[str]) -> Iterable["ShellDataAsset"]:
        return [ShellDataAsset(name) for name in names]

    def rebuild_for_store(self, airflow_context, **kwargs):
        raise NotImplementedError(
            f"This is a {self.__class__.__name__}, use {self.to_full_data_asset.__name__} to convert."
        )

    def retrieve_from_store(self, partition_spec: str = None):
        raise NotImplementedError(
            f"This is a {self.__class__.__name__}, use {self.to_full_data_asset.__name__} to convert."
        )
