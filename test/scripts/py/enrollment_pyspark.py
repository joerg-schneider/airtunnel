import pyspark

from airtunnel import PySparkDataAsset, PySparkDataAssetIO


def rebuild_for_store(asset: PySparkDataAsset, airflow_context):
    spark_session = pyspark.sql.SparkSession.builder.getOrCreate()
    enrollment_data = PySparkDataAssetIO.read_data_asset(
        asset=asset,
        source_files=asset.pickedup_files(airflow_context),
        spark_session=spark_session,
        header=True,
        inferSchema=True,
    )

    PySparkDataAssetIO.write_data_asset(asset=asset, data=enrollment_data)
    spark_session.stop()
