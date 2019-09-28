import logging
import os
import tempfile
import urllib.parse

import pytest

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

AF_HOME = "AIRFLOW_HOME"

assert AF_HOME in os.environ and os.path.exists(
    os.environ[AF_HOME]
), f"env-variable {AF_HOME} needs to be set properly"

NECESSARY_DATA_STORE_PATHS = (
    "archive",
    "ingest",
    "ready",
    "staging",
    "staging/intermediate",
    "staging/ready",
    "staging/pickedup",
    "ingest/archive",
    "ingest/landing",
)


@pytest.fixture(scope="session")
def test_db_path() -> str:
    db_path = os.path.join(tempfile.gettempdir(), "test.db")
    logger.info(f"Using test-database: {db_path}")
    yield db_path
    logger.info(f"Removing file of test-database: {db_path}")
    os.remove(db_path)


@pytest.fixture(scope="session", autouse=True)
def provide_airflow_cfg(test_db_path: str) -> None:
    test_folder_path = os.path.abspath(os.path.dirname(__file__))
    cfg_path = os.path.join(test_folder_path, "airflow_home/airflow.cfg")

    cfg_path_template = os.path.join(
        test_folder_path, "airflow_home/airflow.template.cfg"
    )

    dags_folder_path = os.path.join(
        test_folder_path, "test_airtunnel/workflows/testdags"
    )

    decls_folder_path = os.path.join(test_folder_path, "test_airtunnel/declarations")

    data_store_folder_path = os.path.join(test_folder_path, "data_store")

    # pre-caution: ensure data store folders all exist (i.e. for docker container)
    os.makedirs(data_store_folder_path, exist_ok=True)

    for data_store_subfolder_path in NECESSARY_DATA_STORE_PATHS:
        os.makedirs(
            os.path.join(data_store_folder_path, data_store_subfolder_path),
            exist_ok=True,
        )

    scripts_folder_path = os.path.join(test_folder_path, "scripts")

    with open(cfg_path_template, "r") as cfgfile_template:
        cfg_template = cfgfile_template.read()

    airflow_test_cfg = cfg_template.format(
        dags_folder=dags_folder_path,
        sqlite_path=test_db_path,
        decls_folder=decls_folder_path,
        data_store_folder=data_store_folder_path,
        scripts_folder=scripts_folder_path,
    )

    with open(cfg_path, "w") as cfgfile:
        cfgfile.write(airflow_test_cfg)

    logger.info(f"Updated test airflow.cfg at: {cfg_path}")

    # initialize the airflow db
    import airflow.utils.db

    airflow.utils.db.initdb()


@pytest.fixture(scope="module")
def test_db_hook(test_db_path, provide_airflow_cfg):
    from airflow.hooks.sqlite_hook import SqliteHook

    os.environ[
        "AIRFLOW_CONN_SQLITE"
    ] = f"sqlite:///{urllib.parse.quote_plus(test_db_path)}"
    return SqliteHook("sqlite")


def test_setup_airflow_cfg(provide_airflow_cfg):
    # pseudo-test we can call before collecting all other tests,
    # this will trigger an initial setup of airflow.cfg
    # (if airflow.cfg is not correctly in-place, no other test-cases can safely import anything from "airflow"
    #  which unfortunately happens on pytest collect...)
    pass
