import pyspark

from airtunnel import PySparkDataAsset, PySparkDataAssetIO


def rebuild_for_store(asset: PySparkDataAsset, airflow_context):
    spark_session = pyspark.sql.SparkSession.builder.getOrCreate()
    programme_data = PySparkDataAssetIO.read_data_asset(
        asset=asset,
        source_files=asset.pickedup_files(airflow_context),
        spark_session=spark_session,
    )
    programme_data = programme_data.drop_duplicates(
        subset=asset.declarations.key_columns
    )
    PySparkDataAssetIO.write_data_asset(asset=asset, data=programme_data)
    spark_session.stop()
