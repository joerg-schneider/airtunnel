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

        self.__connection = None
        self.__sql_hook = sql_hook
        self.__sql_relative_path = script_file_relative_path
        self.__sql_params = {} if parameters is None else parameters
        self.__dynamic_params_func = dynamic_parameters
        self.__loaded_sql_script = None
        self.__loaded_sql_statements = None
        self.__db_connection = None

        self.check_arguments()

    def check_arguments(self):
        assert self.__sql_relative_path is not None, "SQL path needs to be supplied!"

    def _load_sql_script(self) -> None:
        self.__loaded_sql_script = load_sql_script(
            script_file_relative_path=self.__sql_relative_path
        )

    def execute(self, context):
        """

        :param context: the Airflow context
        :return: None
        """

        self._load_sql_script()

        if self.__dynamic_params_func is not None:
            self.__sql_params.update(
                prepare_sql_params(self.__dynamic_params_func, context)
            )

        if self.__sql_params != {}:
            self.__loaded_sql_script = format_sql_script(
                sql_script=self.__loaded_sql_script, sql_params_dict=self.__sql_params
            )

        self.__loaded_sql_statements = split_sql_script(self.__loaded_sql_script)

        connection = self.__sql_hook.get_sqlalchemy_engine().connect()
        connection = connection.execution_options(autocommit=False)
        execute_script(connection, self.__loaded_sql_statements)
        connection.close()

    @property
    def loaded_sql_script(self):
        script = self.__loaded_sql_script
        return script
