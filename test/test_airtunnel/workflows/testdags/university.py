from airflow.models import DAG

from airtunnel.operators.archival import *
from airtunnel.operators.ingestion import *
from airtunnel.operators.loading import *
from airtunnel.operators.transformation import *
from airtunnel.sensors.ingestion import SourceFileIsReadySensor

student = PandasDataAsset("student")
programme = PandasDataAsset("programme")
enrollment = PandasDataAsset("enrollment")
enrollment_summary = PandasDataAsset("enrollment_summary")

with DAG(
    dag_id="university",
    schedule_interval=None,
    start_date=datetime(year=2019, month=9, day=1),
) as dag:
    ingested_ready_tasks = set()

    # a common stream of tasks for all ingested assets:
    for ingested_asset in (student, programme, enrollment):
        source_is_ready = SourceFileIsReadySensor(asset=ingested_asset)
        ingest = IngestOperator(asset=ingested_asset)
        transform = PandasTransformationOperator(asset=ingested_asset)
        archive = DataAssetArchiveOperator(asset=ingested_asset)
        staging_to_ready = StagingToReadyOperator(asset=ingested_asset)
        ingest_archival = IngestArchiveOperator(asset=ingested_asset)

        dag >> source_is_ready >> ingest >> transform >> archive >> staging_to_ready >> ingest_archival

        ingested_ready_tasks.add(staging_to_ready)

    # upon having loaded the three ingested assets, connect the aggregation downstream to them:
    build_enrollment_summary = PandasTransformationOperator(asset=enrollment_summary)
    build_enrollment_summary.set_upstream(ingested_ready_tasks)

    staging_to_ready = StagingToReadyOperator(asset=enrollment_summary)

    dag >> build_enrollment_summary >> staging_to_ready
