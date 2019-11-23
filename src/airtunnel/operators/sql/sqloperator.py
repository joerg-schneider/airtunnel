""" Module for Airtunnel's SQLOperator. """
import logging
from typing import Union, List

from airflow.hooks.dbapi_hook import DbApiHook
from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults

from .sql_helpers import (
    load_sql_script,
    format_sql_script,
    split_sql_script,
    execute_script,
    prepare_sql_params,
)

log = logging.getLogger(__name__)


class SQLOperator(BaseOperator):
    """ Airtunnel's SQLOperator. """

    @apply_defaults
    def __init__(
        self,
        sql_hook: DbApiHook,
        script_file_relative_path: Union[str, List[str]] = None,
        parameters: dict = None,
        dynamic_parameters: callable([[None], dict]) = None,
        *args,
        **kwargs
    ):

        super(SQLOperator, self).__init__(*args, **kwargs)

        self._connection = None
        self._sql_hook = sql_hook
        self._sql_relative_path = script_file_relative_path
        self._sql_params = {} if parameters is None else parameters
        self._dynamic_params_func = dynamic_parameters
        self._loaded_sql_script = None
        self._loaded_sql_statements = None
        self._db_connection = None

        self._check_arguments()

    def _check_arguments(self):
        assert self._sql_relative_path is not None, "SQL path needs to be supplied!"

    def _load_sql_script(self) -> None:
        self._loaded_sql_script = load_sql_script(
            script_file_relative_path=self._sql_relative_path
        )

    def execute(self, context):
        """ Execute the operator instance using Airflow. """

        self._load_sql_script()

        if self._dynamic_params_func is not None:
            self._sql_params.update(
                prepare_sql_params(self._dynamic_params_func, context)
            )

        if self._sql_params != {}:
            self._loaded_sql_script = format_sql_script(
                sql_script=self._loaded_sql_script, sql_params_dict=self._sql_params
            )

        self._loaded_sql_statements = split_sql_script(self._loaded_sql_script)

        connection = self._sql_hook.get_sqlalchemy_engine().connect()
        connection = connection.execution_options(autocommit=False)
        execute_script(connection, self._loaded_sql_statements)
        connection.close()
