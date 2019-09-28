from datetime import datetime, timedelta

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
        asset=enrollment_summary, refreshed_within=timedelta(days=1), poke_interval=5
    )
    await_ancestors_updated = AwaitAssetAncestorsUpdatedSensor(
        asset=enrollment_summary, poke_interval=5
    )

    dag >> await_load_status >> await_ancestors_updated
