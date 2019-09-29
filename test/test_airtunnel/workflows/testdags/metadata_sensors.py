from datetime import datetime, timedelta

import pytest
from airflow.models import DAG

from airtunnel import PandasDataAsset
from airtunnel.sensors.metadata import (
    AwaitLoadStatusSensor,
    AwaitAssetAncestorsUpdatedSensor,
)

enrollment_summary = PandasDataAsset("enrollment_summary")

with DAG(
    dag_id="metadata_sensors",
    schedule_interval=None,
    start_date=datetime(year=2019, month=9, day=1),
) as dag:
    await_load_status = AwaitLoadStatusSensor(
        asset=enrollment_summary,
        refreshed_within=timedelta(days=1),
        poke_interval=5,
        timeout=120,
    )

    await_load_status_refreshed_after = AwaitLoadStatusSensor(
        asset=enrollment_summary,
        task_id="enrollment_summary_load_status_2",
        refreshed_after=datetime.now() - timedelta(days=1),
        poke_interval=5,
        timeout=120,
    )

    with pytest.raises(ValueError):
        # missing parameters:
        AwaitLoadStatusSensor(asset=enrollment_summary)

    await_ancestors_updated = AwaitAssetAncestorsUpdatedSensor(
        asset=enrollment_summary, poke_interval=5, timeout=120
    )

    dag >> await_load_status >> await_load_status_refreshed_after >> await_ancestors_updated
