import logging
import os
import shutil
from typing import Optional

import pandas as pd
from airflow import DAG
from airflow.hooks.dbapi_hook import DbApiHook
from airflow.models import DagBag

import airtunnel.paths

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DUMMY_TABLE = "dummy_table_1"
DUMMY_TABLE2 = "dummy_table_2"


def run_sequential_airflow_dag(dag_id: str):
    dag: DAG = DagBag().get_dag(dag_id=dag_id)
    # before running the dag, clean up archival folders, as dag execution dates are only day specific
    # (i.e. 2019-09-01T00:00:00+00:00  where h,m,s,ss is zero...) using dag.run()
    #  this avoids two consecutive test runs for the same dag_id clashing on the folder-name side,
    for f in os.listdir(airtunnel.paths.P_DATA_INGEST_ARCHIVE):
        shutil.rmtree(
            path=os.path.join(airtunnel.paths.P_DATA_INGEST_ARCHIVE, f),
            ignore_errors=True,
        )
    for f in os.listdir(airtunnel.paths.P_DATA_ARCHIVE):
        shutil.rmtree(
            path=os.path.join(airtunnel.paths.P_DATA_ARCHIVE, f), ignore_errors=True
        )

    dag.run()


def run_on_db(test_db_hook: DbApiHook, sql: str) -> Optional[pd.DataFrame]:
    """ Utility method to run a single SQL statement and optionally return the result. """

    c = test_db_hook.get_sqlalchemy_engine().connect()

    if "select " in sql.lower():
        return pd.read_sql_query(sql=sql, con=c)
    else:
        c.execute(sql)
        c.close()


def table_rowcount(
    test_db_hook: DbApiHook, table: str, predicate: Optional[str] = None
):
    query = f"""
    select count(*) from {table}
    {"" if predicate is None else " where "+predicate}
    """
    return run_on_db(test_db_hook, query).iloc[0][0]
