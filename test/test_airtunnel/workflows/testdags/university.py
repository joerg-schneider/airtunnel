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

    for ingested_asset in (student, programme, enrollment):
        source_is_ready = SourceFileIsReadySensor(
            asset=ingested_asset, no_of_required_static_pokes=2, poke_interval=3
        )
        ingest = IngestOperator(asset=ingested_asset)
        transf = PandasTransformationOperator(asset=ingested_asset)
        archive = DataAssetArchiveOperator(asset=ingested_asset)
        stg_to_ready = StagingToReadyOperator(asset=ingested_asset)
        ing_archive = IngestArchiveOperator(asset=ingested_asset)

        dag >> source_is_ready >> ingest >> transf >> archive >> stg_to_ready >> ing_archive

        ingested_ready_tasks.add(stg_to_ready)

    build_enrollment_summary = PandasTransformationOperator(asset=enrollment_summary)
    build_enrollment_summary.set_upstream(ingested_ready_tasks)

    stg_to_ready = StagingToReadyOperator(asset=enrollment_summary)

    dag >> build_enrollment_summary >> stg_to_ready
