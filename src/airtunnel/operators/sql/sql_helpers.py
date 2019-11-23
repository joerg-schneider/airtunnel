""" A module with several SQL helpers used for Airtunnel. """
import logging
import re
from os import path
from typing import Union, List, Iterable, Dict

import airtunnel.paths

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

MINIMUM_SQL_STATEMENT_LEN = 5


def load_sql_script(script_file_relative_path: Union[str, Iterable[str]]) -> str:
    """
    Load a SQL script from Airtunnel's script store.

    :param script_file_relative_path: single or multiple relative script path(s) from the Airtunnel
     script store SQL folder
    :return: the full loaded SQL script as a raw string
    """

    # if given just a path to one file (string), put it in a list
    if isinstance(script_file_relative_path, str):
        script_file_relative_path = [script_file_relative_path]

    # init with empty str.
    loaded_sql_script = ""

    # iterate over all given paths of sql scripts to execute
    for sql_path in script_file_relative_path:
        # path is relative, so if it begins with "/", then strip it
        if sql_path.startswith("/"):
            sql_path = sql_path[1:]

        script_abs_path = path.join(airtunnel.paths.P_SCRIPTS_SQL, sql_path)

        logger.info("Loading SQL script from: %s" % script_abs_path)
        with open(script_abs_path) as sql_file:
            loaded_sql_script = loaded_sql_script + sql_file.read() + "\n\n"

    return loaded_sql_script


def split_sql_script(full_script: str) -> List[str]:
    """
    Splits a full SQL script into its individual statements (using ;) and
    removes commented lines.

    :param full_script: the full raw SQL script
    :return: list of individual SQL statements
    """
    logger.info("Splitting SQL script into statements")

    # remove commented SQL lines, starting with "--"
    cleaned_lines = []
    for line in full_script.split("\n"):
        line = re.sub(r"^\s*--.*$", "", line, re.DOTALL)
        cleaned_lines.append(line)

    # construct full SQL script using remaining (non-commented) lines
    full_sql = "\n".join(cleaned_lines)

    # split the input file at each semicolon
    # removing too short tokens
    return [
        stmnt.strip()
        for stmnt in full_sql.split(";")
        if len(stmnt) > MINIMUM_SQL_STATEMENT_LEN
    ]


def execute_statement(db_conn, sql_statement) -> None:
    """ Executes a single SQL statement on a DB connection. """
    logger.info("Executing SQL statement:\n\n %s \n" % sql_statement)
    db_conn.execute(sql_statement)


def execute_script(connection, statements: Iterable[str]) -> None:
    """
    Executes a SQL script.

    :param connection: a live DB connection, implementing ``execute()`` as in Python DB API (PEP:249)
    :param statements: list of individual SQL statements to execute
    :return: None
    """
    logger.info("Executing SQL script")

    # assert the connection that is returned is valid going forward
    assert hasattr(
        connection, "execute"
    ), "Database connection lacks execute()-function! Only Python DB API (PEP:	249) supported!"

    for sql_statement in statements:
        execute_statement(db_conn=connection, sql_statement=sql_statement)

    logger.info("Done executing SQL script")


def format_sql_script(sql_script: str, sql_params_dict: Dict[str, str]) -> str:
    """
    Formats a SQL script using the standard Python string format method.

    :param sql_script: raw SQL script string with formatting parameters expressed as ``{parameter_key}``
    :param sql_params_dict: the dictionary (of type string to string) that will be passed
    to the format function
    :return:
    """
    logger.info("Formatting SQL script using params: %s" % str(sql_params_dict))
    return sql_script.format(**sql_params_dict)


def prepare_sql_params(compute_sql_params_function, airflow_context) -> Dict[str, str]:
    """
    Prepares further dynamic SQL parameters using an anonymous passed function that computes them.

    The dynamic function will be passed the live Airflow context, so that it can leverage it's information.

    :return: a dict (of type string to string) with additional formatting key-value pairs that are added
    to the default static formatting parameters.
    """
    logger.info(
        "Computing SQL params using user passed function called %s "
        % compute_sql_params_function.__name__
    )
    return compute_sql_params_function(airflow_context)
