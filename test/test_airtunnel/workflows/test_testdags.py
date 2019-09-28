import logging
import os
import shutil
import subprocess

import pytest

import airtunnel.paths
from test_utils import run_sequential_airflow_dag

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def has_airflow_cli() -> bool:
    p = subprocess.Popen("which airflow", shell=True)
    p.wait()
    if p.returncode == 0:
        logger.info("airflow binary is available in session - running test DAGs!")
        return True
    else:
        logger.info("airflow binary is not available in session - skipping test DAGs!")
        return False


@pytest.mark.skipif(
    condition=not has_airflow_cli(), reason="Airflow binary not available"
)
def test_testdag1():
    # prepare the landing data by copying it into the landing directory:
    current_dir = os.path.dirname(__file__)
    landing_test_data = os.path.join(
        current_dir, os.pardir, os.pardir, "test_raw_landing_data"
    )
    for test_data_asset in os.listdir(landing_test_data):
        landing_dir = os.path.join(airtunnel.paths.P_DATA_INGEST_LANDING, test_data_asset)
        if os.path.exists(landing_dir):
            shutil.rmtree(landing_dir)
        shutil.copytree(
            os.path.join(landing_test_data, test_data_asset),
            landing_dir,
        )

    run_sequential_airflow_dag(dag_id="university")

    # the landing file should be gone
    # assert not os.path.exists(landing_file_path)

    # check the data asset
    # test_table = PandasDataAsset(name="test_table")

    # check that the data on the data store in ready equals the data before loading
    # assert test_table.retrieve_from_store().equals(landed_data)
