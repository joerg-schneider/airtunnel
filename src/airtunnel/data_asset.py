""" Module for Airtunnel data asset abstractions and data asset io abstractions. """
import importlib
import logging
import os
import warnings
from abc import abstractmethod, ABC
from os import path
from typing import Optional, Union, Iterable, Dict, List, Callable

import pandas as pd
from airflow.hooks.dbapi_hook import DbApiHook
from airflow.models import TaskInstance

import airtunnel
from airtunnel.declaration_store import DataAssetDeclaration, V_COMP_NONE
from airtunnel.operators.sql import sqloperator
from airtunnel.paths import (
    P_DATA_READY,
    P_DATA_STAGING_PICKEDUP,
    P_DATA_STAGING_READY,
    P_DATA_INGEST_ARCHIVE,
    P_DATA_INGEST_LANDING,
    P_DATA_ARCHIVE,
)

PYSPARK_DEFAULT_SAVEMODE = "overwrite"

try:
    import pyspark
except ImportError:
    pyspark = None

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class BaseDataAsset:
    """
    Base class for all Airtunnel data assets.
    """

    __slots__ = ["name", "declarations"]

    def __init__(self, name: str) -> None:
        self.name = name
        # a ShellDataAsset is a special kind of wrapper which is purely name based and skips decl. loading
        # (useful for testing, capturing lineage, or similar tasks where declarations are not needed)
        if not isinstance(self, ShellDataAsset):
            self.declarations: DataAssetDeclaration = DataAssetDeclaration(
                data_asset=name
            )

    @property
    def ready_path(self) -> str:
        """
        :return: the ready (folder) path for this data asset
        """
        return os.path.join(P_DATA_READY, self.name)

    def make_ready_temp_path(self, airflow_context: Dict) -> str:
        """
        Makes a temporary path under ready for staging purposes.

        It points to a hidden (prefixed by ".") folder as the last component,
        so that this path is ignored by frameworks like Spark.

        :param airflow_context: a live Airflow context, used to get the exec timestamp
        :return: the temporary staging path
        """
        return os.path.join(
            P_DATA_READY, "." + self._escaped_exec_date(airflow_context) + self.name
        )

    @staticmethod
    def _escaped_exec_date(airflow_context):
        """
        Creates a filesystem safe execution date string from a Airflow context.

        :param airflow_context: a live Airflow context
        :return: a string with the exection datetime escaped
        """
        return (
            str(airflow_context["task_instance"].execution_date)
            .replace(" ", "_")
            .replace(":", "_")
        )

    @property
    def landing_path(self) -> str:
        """
        :return: the ingest/landing path for this data asset
        """
        return os.path.join(P_DATA_INGEST_LANDING, self.name)

    @property
    def ingest_archive_path(self) -> str:
        """
        :return: the ingest/archive path for this data asset
        """
        return os.path.join(P_DATA_INGEST_ARCHIVE, self.name)

    def staging_pickedup_path(self, airflow_context) -> str:
        """
        Return the pickedup path under staging for this data asset, versioned by
        the Airflow execution date.

        :param airflow_context: a live Airflow context
        :return: the versioned staging/pickedup path for this data asset
        """
        return os.path.join(
            P_DATA_STAGING_PICKEDUP, self.name, self._escaped_exec_date(airflow_context)
        )

    @property
    def staging_ready_path(self) -> str:
        """
        :return: the staging/ready path for this data asset
        """
        p = os.path.join(P_DATA_STAGING_READY, self.name)
        # todo: this has to be replaced using the data_store_adapter!
        os.makedirs(p, exist_ok=True)
        return p

    def ready_archive_path(self, airflow_context) -> str:
        """
        Return the versioned archive path for this data asset, based on the
        Airflow execution date.

        :param airflow_context: a live Airflow context
        :return: the versioned archived path for this data asset
        """
        return os.path.join(
            P_DATA_ARCHIVE, self.name, self._escaped_exec_date(airflow_context)
        )

    def pickedup_files(self, airflow_context) -> List[str]:
        """
        Using Airflow XCOM based on the given Airflow context with its
        DAG id, fetch the list of previously discovered files for ingestion.

        Then, considering the files have been picked-up by the ingestion operator,
        adjust the path prefix for the picked-up folder and return the list of file
        paths.

        :param airflow_context: a live Airflow context
        :return: list of picked-up file paths
        """

        ti: TaskInstance = airflow_context["task_instance"]

        descovered_files = ti.xcom_pull(
            dag_id=ti.dag_id, key=self.discovered_files_xcom_key
        )

        return [
            f.replace(self.landing_path, self.staging_pickedup_path(airflow_context))
            for f in descovered_files
        ]

    @property
    def discovered_files_xcom_key(self) -> str:
        """
        :return: the XCOM key for discovered files for this data asset
        """
        # local import to avoid cross import issues
        from airtunnel.sensors.ingestion import K_DISCOVERED_FILES

        return f"{K_DISCOVERED_FILES}_{self.name}"

    @property
    def output_filename(self) -> str:
        """
        :return: creates an output filename for this data asset considering the declared storage format
        """
        return self.name + "." + self.declarations.out_storage_format

    @abstractmethod
    def rebuild_for_store(self, airflow_context, **kwargs) -> None:
        """
        Rebuild this data asset for the data store's ready folder.

        :param airflow_context: a live Airflow context
        :param kwargs: additional keyword arguments to be passed along the downstream scripts
        :return: None
        """
        raise NotImplementedError

    @abstractmethod
    def retrieve_from_store(self) -> Union[pd.DataFrame, "pyspark.sql.DataFrame"]:
        """
        Retrieve this data asset from the ready layer of the data store.

        :return: A pandas or PySpark dataframe
        """
        raise NotImplementedError


class PandasDataAsset(BaseDataAsset):
    """
    Implements a Pandas enabled Airtunnel data asset.
    """

    def retrieve_from_store(
        self, airflow_context=None, consuming_asset: Optional[BaseDataAsset] = None
    ) -> pd.DataFrame:
        """
        Retrieve this data asset from the ready layer of the data store (using Pandas).

        :param airflow_context: a live Airflow context (optional)
        :param consuming_asset: the consuming data asset for lineage collection (optional)
        :return: the data asset as a Pandas dataframe
        """

        _log_lineage(self, airflow_context, consuming_asset)

        return PandasDataAssetIO.retrieve_data_asset(asset=self)

    def rebuild_for_store(self, airflow_context, **kwargs) -> None:
        """
        Rebuild this data asset for the data store's ready folder (using Pandas).

        :param airflow_context: a live Airflow context
        :param kwargs: additional keyword arguments to be passed along the downstream scripts
        :return: None
        """
        # we delegate the rebuild of this data asset to the Pandas script
        asset_script = _load_py_script(self)
        asset_script.rebuild_for_store(asset=self, airflow_context=airflow_context)

    def rename_fields_as_declared(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Rename the columns as declared for this data asset.

        :param data: the input Pandas dataframe to perform the rename on
        :return: the dataframe with the columns renamed
        """
        rename_map = {c: c for c in data.columns}
        rename_map.update(self.declarations.transform_renames)
        logger.info(f"Renaming according to: {rename_map}")
        return data.rename(columns=rename_map)


class PySparkDataAsset(BaseDataAsset):
    """ A PySpark enabled Airtunnel data asset. """

    def rebuild_for_store(self, airflow_context, **kwargs):
        """
        Rebuild this data asset for the data store's ready folder (using PySpark).

        :param airflow_context: a live Airflow context
        :param kwargs: additional keyword arguments to be passed along the downstream scripts
        :return: None
        """
        # we delegate the rebuild of this data asset to the PySpark script
        asset_script = _load_py_script(self)
        asset_script.rebuild_for_store(asset=self, airflow_context=airflow_context)

    def retrieve_from_store(
        self,
        airflow_context=None,
        consuming_asset: Optional[BaseDataAsset] = None,
        spark_session: "pyspark.sql.SparkSession" = None,
    ) -> "pyspark.sql.DataFrame":
        """
        Retrieve this data asset from the ready layer of the data store (using Pandas).

        :param airflow_context: a live Airflow context (optional)
        :param consuming_asset: the consuming data asset for lineage collection (optional)
        :param spark_session: a live Spark session to use (required)
        :return: the data asset as a Pandas dataframe
        """

        _log_lineage(self, airflow_context, consuming_asset)

        return PySparkDataAssetIO.retrieve_data_asset(
            asset=self, spark_session=spark_session
        )

    def rename_fields_as_declared(
        self, data: "pyspark.sql.DataFrame"
    ) -> "pyspark.sql.DataFrame":
        """
        Rename the columns as declared for this data asset.

        :param data: the input PySpark dataframe to perform the rename on
        :return: the PySpark dataframe with the columns renamed
        """
        rename_map = {c: c for c in data.columns}
        rename_map.update(self.declarations.transform_renames)
        logger.info(f"Renaming according to: {rename_map}")

        for c_from, c_to in rename_map.items():
            data = data.withColumnRenamed(c_from, c_to)

        return data


class SQLDataAsset(BaseDataAsset):
    """ A SQL enabled Airtunnel data asset. """

    def __init__(self, name: str, sql_hook: DbApiHook):
        if sql_hook is None:
            raise ValueError("Need a DbApiHook to instantiate the SQLDataAsset")

        self._sql_hook = sql_hook
        super(SQLDataAsset, self).__init__(name=name)

    def get_raw_sql_script(self, script_type: str = "dml") -> str:
        """
        Returns the raw SQL script for this data asset.

        :param script_type: i.e. dml, ddl – translates to a subfolder in the script-store/sql/ folder
        :return: the raw SQL script as a string
        """
        # todo: change this using the paths module
        from scripts import sql

        sql_location = path.join(
            path.dirname(sql.__file__),
            script_type,
            self.name.replace(".", "/") + ".sql",
        )

        if not path.exists(sql_location):
            raise FileNotFoundError(f"SQL script expected at {sql_location} not found!")

        logger.info("Loading SQL script from: %s" % sql_location)

        with open(sql_location) as sql_file:
            loaded_sql_script = sql_file.read() + "\n\n"

        return loaded_sql_script

    def formatted_sql_script(
        self,
        parameters: Dict,
        script_type: str = "dml",
        dynamic_parameters: Callable = None,
        airflow_context=None,
    ) -> str:
        """
        Get the formatted SQL script for this data asset.

        :param parameters: dictionary of parameters to inject into the script (i.e. to format the raw string with)
        :param script_type: i.e. dml, ddl – translates to a subfolder in the script-store/sql/ folder
        :param dynamic_parameters: a callable that is dynamically executed to compute SQL script parameters at runtime
        :param airflow_context: a live Airflow context – passed into the function to compute dynamic parameters
        :return: the formatted SQL script for this data asset
        """

        loaded_sql_script = self.get_raw_sql_script(script_type=script_type)

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

    # todo: we might check if a DDL script even exists, it could be optional!
    def rebuild_for_store(
        self,
        airflow_context,
        parameters: Dict = None,
        dynamic_parameters: Callable = None,
        **kwargs,
    ):
        """
        Rebuild this data asset for the data store's ready folder (using SQL).

        :param airflow_context: a live Airflow context
        :param parameters: dictionary of parameters to inject into the script (i.e. to format the raw string with)
        :param dynamic_parameters: a callable that is dynamically executed to compute SQL script parameters at runtime
        :param kwargs: additional keyword arguments to be passed along the downstream scripts
        :return: None
        """
        # run first ddl, then dml -- or just solely dml:
        scripts_to_run = ("ddl", "dml") if self.declarations.run_ddl else ("dml",)

        for script_type in scripts_to_run:
            formatted_sql_script = self.formatted_sql_script(
                parameters=parameters,
                script_type=script_type,
                dynamic_parameters=dynamic_parameters,
                airflow_context=airflow_context,
            )

            script_statements = sqloperator.split_sql_script(formatted_sql_script)

            connection = self._sql_hook.get_sqlalchemy_engine().connect()
            connection = connection.execution_options(autocommit=False)

            sqloperator.execute_script(
                connection=connection, statements=script_statements
            )

    def retrieve_from_store(self) -> pd.DataFrame:
        """
        Retrieve this data asset from the ready layer of the data store (using SQL).

        :return: the data asset as a Pandas dataframe
        """
        return pd.read_sql(
            sql=f"select * from {self.name}", con=self._sql_hook.get_conn()
        )


class ShellDataAsset(BaseDataAsset):
    """ A shell DataAsset, acting merely as a container - i.e. for lineage. """

    def __init__(self, name: str):
        super().__init__(name=name)

    def __eq__(self, other):
        return self.name == other.name

    def to_full_data_asset(
        self, target_type: Union[PySparkDataAsset.__class__, PandasDataAsset.__class__]
    ) -> Union[PySparkDataAsset, PandasDataAsset]:
        """
        Convert this ShellDataAsset to a full Airtunnel data asset of a given type.

        :param target_type: the target class to convert this ShellDataAsset to
        :return: the full data asset
        """
        return target_type(name=self.name)

    @staticmethod
    def from_names(names: Iterable[str]) -> List["ShellDataAsset"]:
        """
        Create several ShellDataAssets from a list of names.

        :param names: the names for which to create ShellDataAssets for
        :return: list of created ShellDataAssets
        """
        return [ShellDataAsset(name) for name in names]

    def rebuild_for_store(self, airflow_context, **kwargs) -> None:
        """
        Unimplemented method of the BaseDataAsset interface – do not use!
        """
        raise NotImplementedError(
            f"This is a {self.__class__.__name__}, use {self.to_full_data_asset.__name__} to convert."
        )

    def retrieve_from_store(self):
        """
        Unimplemented method of the BaseDataAsset interface – do not use!
        """
        raise NotImplementedError(
            f"This is a {self.__class__.__name__}, use {self.to_full_data_asset.__name__} to convert."
        )


class BaseDataAssetIO(ABC):
    """ Base class for all Airtunnel DataAssetIO. """

    @staticmethod
    @abstractmethod
    def write_data_asset(
        asset: BaseDataAsset,
        data: Union[pd.DataFrame, "pyspark.sql.DataFrame"],
        **writer_kwargs,
    ) -> None:
        """
        Writes a data asset.

        :param asset: the data asset to write
        :param data: date to write for the data asset
        :param writer_kwargs: additional keyword arguments to pass into the writer function
        :return: None
        """
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def read_data_asset(
        asset: BaseDataAsset, source_files: Iterable[str], **reader_kwargs
    ) -> Union[pd.DataFrame, "pyspark.sql.DataFrame"]:
        """
        Reads a data asset.

        :param asset: the data asset to read
        :param source_files: input source file(s) to read
        :param reader_kwargs: additional keyword arguments to pass into the reader function
        :return: the read in data as a dataframe
        """
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def retrieve_data_asset(
        asset: BaseDataAsset, **reader_kwargs
    ) -> Union[pd.DataFrame, "pyspark.sql.DataFrame"]:
        """
        Retrieves a data asset from the Airtunnel data store.

        :param asset: the data asset to retrieve
        :param reader_kwargs: additional keyword arguments to pass into the reader function
        :return: the retrieved data as a dataframe
        """
        raise NotImplementedError


class PandasDataAssetIO(BaseDataAssetIO):
    """ IO interface for the Airtunnel PandasDataAsset. """

    @staticmethod
    def retrieve_data_asset(asset: PandasDataAsset, **reader_kwargs) -> pd.DataFrame:
        """
        Retrieves a PandasDataAsset from the Airtunnel data store (using Pandas).

        :param asset: the data asset to retrieve
        :param reader_kwargs: additional keyword arguments to pass into the Pandas reader function
        :return: the retrieved data as a Pandas dataframe
        """

        data = []
        source_files = [
            path.join(root, f)
            for root, dirs, files in os.walk(asset.ready_path)
            for f in files
        ]

        if asset.declarations.is_parquet_output:
            data = [pd.read_parquet(f, **reader_kwargs) for f in source_files]
        elif asset.declarations.is_csv_output:
            data = [pd.read_csv(f, **reader_kwargs) for f in source_files]

        if not data:
            return pd.DataFrame()

        return pd.concat(data) if len(data) > 1 else data[0]

    @staticmethod
    def write_data_asset(
        asset: PandasDataAsset, data: pd.DataFrame, **writer_kwargs
    ) -> None:
        """
        Writes a PandasDataAsset using Pandas.

        :param asset: the data asset to write
        :param data: date to write for the data asset
        :param writer_kwargs: additional keyword arguments to pass into the Pandas writer function
        :return: None
        """
        if asset.declarations.is_parquet_output:
            data.to_parquet(
                path.join(asset.staging_ready_path, asset.output_filename),
                compression=asset.declarations.out_comp_codec,
                **writer_kwargs,
            )
        elif asset.declarations.is_csv_output:
            data.to_csv(
                path_or_buf=path.join(asset.staging_ready_path, asset.output_filename),
                compression=asset.declarations.out_comp_codec,
                **writer_kwargs,
            )
        else:
            raise ValueError(f"Only output formats of csv/parquet are supported!")

    @staticmethod
    def read_data_asset(
        asset: PandasDataAsset, source_files: Iterable[str], **reader_kwargs
    ) -> pd.DataFrame:
        """
        Reads a PandasDataAsset using Pandas.

        :param asset: the PandasDataAsset to read
        :param source_files: input source file(s) to read
        :param reader_kwargs: additional keyword arguments to pass into the reader function
        :return: the read in data as a dataframe
        """
        data = []

        if asset.declarations.is_csv_input:
            data = [pd.read_csv(f, **reader_kwargs) for f in source_files]
        elif asset.declarations.is_xls_input:
            data = [pd.read_excel(f, **reader_kwargs) for f in source_files]
        elif asset.declarations.is_parquet_input:
            data = [pd.read_parquet(f, **reader_kwargs) for f in source_files]

        if not data:
            warnings.warn(f"No data for {asset.name} was read - please double check!!")
            return pd.DataFrame()

        return pd.concat(data) if len(data) > 1 else data[0]


class PySparkDataAssetIO(BaseDataAssetIO):
    """ IO interface for the Airtunnel PySparkDataAsset. """

    @staticmethod
    def retrieve_data_asset(
        asset: PySparkDataAsset,
        spark_session: "pyspark.sql.SparkSession" = None,
        **reader_kwargs,
    ) -> "pyspark.sql.DataFrame":
        """
        Retrieves a PySparkDataAsset from the Airtunnel data store (using PySpark).

        :param asset: the data asset to retrieve
        :param spark_session: a live PySpark session to use (required)
        :param reader_kwargs: additional keyword arguments to pass into the PySpark reader function
        :return: the retrieved data as a PySpark dataframe
        """

        PySparkDataAssetIO._check_spark_session(spark_session)

        if asset.declarations.is_parquet_output:
            data = spark_session.read.parquet(asset.ready_path)
        elif asset.declarations.is_csv_output:
            data = spark_session.read.csv(path=asset.ready_path, **reader_kwargs)
        else:
            raise ValueError(f"Unsupported asset output format for PySpark data asset.")

        return data

    @staticmethod
    def write_data_asset(
        asset: PySparkDataAsset, data: "pyspark.sql.DataFrame", **writer_kwargs
    ) -> None:
        """
        Writes a PySparkDataAsset using PySpark.

        :param asset: the data asset to write
        :param data: date to write for the data asset
        :param writer_kwargs: additional keyword arguments to pass into the PySpark writer function
        :return: None
        """

        if "mode" not in writer_kwargs:
            writer_kwargs["mode"] = PYSPARK_DEFAULT_SAVEMODE

        if (
            "compression" not in writer_kwargs
            and asset.declarations.out_comp_codec != V_COMP_NONE
        ):
            writer_kwargs["compression"] = asset.declarations.out_comp_codec

        if asset.declarations.is_parquet_output:
            data.write.parquet(asset.staging_ready_path, **writer_kwargs)

        elif asset.declarations.is_csv_output:
            data.write.csv(path=asset.staging_ready_path, **writer_kwargs)
        else:
            raise ValueError(f"Only output formats of csv/parquet are supported!")

    @staticmethod
    def read_data_asset(
        asset: PySparkDataAsset,
        source_files: Iterable[str],
        spark_session: "pyspark.sql.SparkSession" = None,
        **reader_kwargs,
    ) -> "pyspark.sql.DataFrame":
        """
        Reads a PandasDataAsset using PySpark.

        :param asset: the PySparkDataAsset to read
        :param source_files: input source file(s) to read
        :param spark_session: a live PySpark session to use (required)
        :param reader_kwargs: additional keyword arguments to pass into the PySpark reader function
        :return: the read in data as a dataframe
        """

        PySparkDataAssetIO._check_spark_session(spark_session)

        data = None

        if asset.declarations.is_csv_input:
            data = spark_session.read.csv(path=list(source_files), **reader_kwargs)
        elif asset.declarations.is_xls_input:
            raise ValueError(
                "Reading Excel into a PySpark data asset is not supported – consider PandasDataAsset."
            )
        elif asset.declarations.is_parquet_input:
            data = spark_session.read.parquet(*source_files)

        return data

    @staticmethod
    def _check_spark_session(spark_session):
        """ Checks a user-provided spark session. """
        if spark_session is None:
            raise ValueError(
                "Please provide a Spark session using the kwarg 'spark_session'."
            )
        if not isinstance(spark_session, pyspark.sql.SparkSession):
            raise TypeError(
                f"kwarg 'spark' is no instance of pyspark.sql.SparkSession: {type(spark_session)}"
            )


def _log_lineage(
    for_asset: BaseDataAsset, airflow_context, consuming_asset: Optional[BaseDataAsset]
):
    """ Attempt to log lineage between two Airtunnel data assets, for a dag & task instance (if given)."""
    #  attempt to log lineage
    try:
        if consuming_asset is not None:
            # have to have these imports here to avoid cross-import issues:
            import airtunnel.metadata.adapter
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

            meta_adapter = airtunnel.metadata.adapter.get_configured_adapter()
            meta_adapter.write_lineage(
                Lineage(
                    data_sources=[for_asset],
                    data_target=consuming_asset,
                    dag_id=dag_id,
                    task_id=task_id,
                    dag_exec_date=dag_exec_date,
                )
            )
            logger.info(
                f"Lineage from {for_asset.name} to {consuming_asset.name} recorded"
            )
    except Exception as e:
        logger.warning(f"Error on recording lineage: {e}")


def _load_py_script(asset: BaseDataAsset):
    """ Load a Python script for a given DataAsset from the defined Airtunnel script store. """
    script_path = path.join(airtunnel.paths.P_SCRIPTS_PY, asset.name + ".py")
    try:
        spec = importlib.util.spec_from_file_location("module.name", script_path)
        asset_script = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(asset_script)
    except Exception as e:
        logger.error(f"Could not load Python script expected at {script_path} ")
        raise e
    return asset_script
